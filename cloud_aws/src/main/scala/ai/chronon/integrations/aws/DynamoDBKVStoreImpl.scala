package ai.chronon.integrations.aws

import ai.chronon.api.Constants
import ai.chronon.api.Constants.{ContinuationKey, ListLimit}
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.TilingUtils
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.spark.{IonPathConfig, IonWriter}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.ListResponse
import ai.chronon.online.KVStore.ListValue
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.metrics.Metrics.Context
import ai.chronon.online.metrics.Metrics
import ai.chronon.online.metrics.TTLCache
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  CreateTableRequest,
  DescribeImportRequest,
  DescribeTableRequest,
  GetItemRequest,
  ImportStatus,
  ImportTableDescription,
  ImportTableRequest,
  InputCompressionType,
  InputFormat,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughputExceededException,
  PutItemRequest,
  QueryRequest,
  QueryResponse,
  ResourceInUseException,
  ResourceNotFoundException,
  S3BucketSource,
  ScalarAttributeType,
  ScanRequest,
  ScanResponse,
  TableCreationParameters,
  TimeToLiveSpecification,
  UpdateTimeToLiveRequest
}

import java.nio.charset.Charset
import java.time.{Duration, Instant}
import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import scala.compat.java8.FutureConverters
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class DynamoDBKVStoreImpl(dynamoDbClient: DynamoDbAsyncClient, conf: Map[String, String] = Map.empty) extends KVStore {
  import DynamoDBKVStoreConstants._

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("dynamodb")

  // TTLCache: resolves logical batch dataset names to physical date-suffixed table names
  private val batchTableCache: TTLCache[String, String] = new TTLCache[String, String](
    f = { dataset =>
      val keyMap = Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(dataset.getBytes)).build)
      val request = GetItemRequest.builder
        .tableName(batchTableRegistry)
        .key(keyMap.toJava)
        .build
      val item = dynamoDbClient.getItem(request).join().item().toScala
      item.get("valueBytes").map(v => new String(v.b().asByteArray())).getOrElse(dataset)
    },
    contextBuilder = { _ => metricsContext.withSuffix("batch_table_cache") }
  )

  private[aws] def resolveTableName(dataset: String): String = {
    // Use refresh() instead of apply() so the cache re-checks DynamoDB every ~8s rather than every 2hrs
    if (dataset.endsWith(batchSuffix)) batchTableCache.refresh(dataset)
    else dataset
  }

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    val dbWaiter = dynamoDbClient.waiter
    val maybeSortKeys = props.get(isTimedSorted) match {
      case Some(value: String) if value.toLowerCase == "true" => Some(sortKeyColumn)
      case Some(value: Boolean) if value                      => Some(sortKeyColumn)
      case _ if isStreamingTable(dataset)                     => Some(sortKeyColumn)
      case _                                                  => None
    }

    val keyAttributes =
      Seq(AttributeDefinition.builder.attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build) ++
        maybeSortKeys.map(k => AttributeDefinition.builder.attributeName(k).attributeType(ScalarAttributeType.N).build)

    val keySchema =
      Seq(KeySchemaElement.builder.attributeName(partitionKeyColumn).keyType(KeyType.HASH).build) ++
        maybeSortKeys.map(p => KeySchemaElement.builder.attributeName(p).keyType(KeyType.RANGE).build)

    val request =
      CreateTableRequest.builder
        .attributeDefinitions(keyAttributes.toList.toJava)
        .keySchema(keySchema.toList.toJava)
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .tableName(dataset)
        .build

    logger.info(s"Triggering creation of DynamoDb table: $dataset")
    try {
      dynamoDbClient.createTable(request).join()
      val tableRequest = DescribeTableRequest.builder.tableName(dataset).build
      // Wait until the Amazon DynamoDB table is created.
      val waiterResponse = dbWaiter.waitUntilTableExists(tableRequest).join()
      if (waiterResponse.matched.exception().isPresent)
        throw waiterResponse.matched.exception().get()

      val tableDescription = waiterResponse.matched().response().get().table()
      logger.info(s"Table created successfully! Details: \n${tableDescription.toString}")

      // Enable TTL on the table
      val ttlSpec = TimeToLiveSpecification.builder
        .enabled(true)
        .attributeName("ttl")
        .build
      val ttlRequest = UpdateTimeToLiveRequest.builder
        .tableName(dataset)
        .timeToLiveSpecification(ttlSpec)
        .build
      dynamoDbClient.updateTimeToLive(ttlRequest).join()
      logger.info(s"TTL enabled on table: $dataset with attribute 'ttl'")

      metricsContext.increment("create.successes")
    } catch {
      case _: ResourceInUseException =>
        logger.info(s"Table: $dataset already exists")
      case e: CompletionException if e.getCause.isInstanceOf[ResourceInUseException] =>
        logger.info(s"Table: $dataset already exists")
      case e: Exception =>
        logger.error(s"Error creating Dynamodb table: $dataset", e)
        metricsContext.increment("create.failures")
        throw e
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    // partition our requests into pure get style requests (where we only have key lookup)
    // and query requests (we want to query a range based on afterTsMillis -> endTsMillis or now() )
    val (getLookups, queryLookups) = requests.partition(r => r.startTsMillis.isEmpty)
    val getItemResults = doGetLookups(getLookups)
    val aggregatedQueryResults = doQueryLookups(queryLookups)

    Future.sequence(getItemResults ++ aggregatedQueryResults)
  }

  protected def doGetLookups(getLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    val getItemCompletables = getLookups.map { req =>
      val keyAttributeMap = primaryKeyMap(req.keyBytes)
      val tableName = resolveTableName(req.dataset)
      val getItemReq = GetItemRequest.builder.key(keyAttributeMap.toJava).tableName(tableName).build
      val startTs = System.currentTimeMillis()
      (req, dynamoDbClient.getItem(getItemReq), startTs)
    }

    // timestamp to use for all get responses when the underlying tables don't have a ts field
    val defaultTimestamp = Instant.now().toEpochMilli

    val getItemResults = getItemCompletables.map { case (req, completableFuture, startTs) =>
      handleDynamoDbOperation(metricsContext.withSuffix("multiget"), req.dataset, startTs)(completableFuture)
        .transform {
          case Success(response) =>
            val resultValue = extractTimedValues(List(response.item()).toJava, defaultTimestamp)
            Success(GetResponse(req, resultValue))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
    }
    getItemResults
  }

  protected def queryPartition(dataset: String,
                               partitionKeyBytes: Array[Byte],
                               startTs: Long,
                               endTs: Option[Long]): Future[QueryResponse] = {
    val queryRequest = buildTimeRangeQuery(dataset, partitionKeyBytes, startTs, endTs)
    val callStartTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("query"), dataset, callStartTs)(
      dynamoDbClient.query(queryRequest)
    )
  }

  protected def queryPartitionOnly(dataset: String, partitionKeyBytes: Array[Byte]): Future[QueryResponse] = {
    val queryRequest = buildPartitionOnlyQuery(dataset, partitionKeyBytes)
    val callStartTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("query"), dataset, callStartTs)(
      dynamoDbClient.query(queryRequest)
    )
  }

  protected def doQueryLookups(queryLookups: Seq[KVStore.GetRequest]): Seq[Future[GetResponse]] = {
    val defaultTimestamp = Instant.now().toEpochMilli

    queryLookups.map { req =>
      val resolvedDataset = resolveTableName(req.dataset)
      val tileComponents = extractTileKeyComponents(req.keyBytes)
      val endTs = req.endTsMillis.getOrElse(System.currentTimeMillis())
      val partitionKeys = generateTimeSeriesKeys(
        tileComponents.baseKeyBytes,
        req.startTsMillis.get,
        endTs,
        tileComponents.tileSizeMillis
      )

      // Optimize for the common case of a single partition key (queries within one day)
      if (partitionKeys.length == 1) {
        queryPartition(resolvedDataset, partitionKeys.head, req.startTsMillis.get, req.endTsMillis)
          .transform {
            case Success(response) =>
              val timedValues = extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty)
              Success(GetResponse(req, Success(timedValues)))
            case Failure(e) =>
              Success(GetResponse(req, Failure(e)))
          }
      } else {
        // Multi-day query: fan out to multiple partition keys
        val queryFutures = partitionKeys.map { partitionKeyBytes =>
          queryPartition(resolvedDataset, partitionKeyBytes, req.startTsMillis.get, req.endTsMillis)
        }

        Future.sequence(queryFutures).transform {
          case Success(responses) =>
            val allTimedValues = responses.flatMap { response =>
              extractTimedValues(response.items(), defaultTimestamp).getOrElse(Seq.empty)
            }
            Success(GetResponse(req, Success(allTimedValues)))
          case Failure(e) =>
            Success(GetResponse(req, Failure(e)))
        }
      }
    }
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => 100
    }

    val maybeExclusiveStartKey = request.props.get(ContinuationKey)
    val maybeExclusiveStartKeyAttribute = maybeExclusiveStartKey.map { k =>
      AttributeValue.builder.b(SdkBytes.fromByteArray(k.asInstanceOf[Array[Byte]])).build
    }

    val scanBuilder = ScanRequest.builder.tableName(request.dataset).limit(listLimit)
    val scanRequest = maybeExclusiveStartKeyAttribute match {
      case Some(value) => scanBuilder.exclusiveStartKey(Map(partitionKeyColumn -> value).toJava).build
      case _           => scanBuilder.build
    }

    val startTs = System.currentTimeMillis()
    handleDynamoDbOperation(metricsContext.withSuffix("list"), request.dataset, startTs)(
      dynamoDbClient.scan(scanRequest)
    ).map { scanResponse =>
      val resultElements = extractListValues(scanResponse)
      val noPagesLeftResponse = ListResponse(request, resultElements, Map.empty)
      if (scanResponse.hasLastEvaluatedKey) {

        val lastEvalKey = scanResponse.lastEvaluatedKey().toScala.get(partitionKeyColumn)
        lastEvalKey match {
          case Some(av) => ListResponse(request, resultElements, Map(ContinuationKey -> av.b().asByteArray()))
          case _        => noPagesLeftResponse
        }
      } else {
        noPagesLeftResponse
      }
    }.recover { case e: Exception =>
      ListResponse(request, Failure(e), Map.empty)
    }
  }

  // Dynamo has restrictions on the number of requests per batch (and the payload size) as well as some partial
  // success behavior on batch writes which necessitates a bit more logic on our end to tie things together.
  // To keep things simple for now, we implement the multiput as a sequence of put calls.
  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.info(s"Triggering multiput for ${keyValueDatasets.size}: rows")
    val futureResponses = keyValueDatasets.map { req =>
      val (actualKeyBytes, actualTimestamp) = if (isStreamingTable(req.dataset)) {
        // For streaming tables, unwrap TileKey to use entity key + tileSizeMs as partition key
        // and tileStartTs as sort key. Including tileSizeMs in the key supports tile layering.
        val tileComponents = extractTileKeyComponents(req.keyBytes)
        val timestamp = tileComponents.tileStartTimestampMillis
        val tiledKey = buildKeyWithTileSize(tileComponents.baseKeyBytes, timestamp, tileComponents.tileSizeMillis)
        (tiledKey, timestamp)
      } else {
        val timestampInPutRequest = req.tsMillis.getOrElse(System.currentTimeMillis())
        (req.keyBytes, timestampInPutRequest)
      }

      val attributeMap: Map[String, AttributeValue] = buildAttributeMap(actualKeyBytes, req.valueBytes)
      val tsMap = Map(sortKeyColumn -> AttributeValue.builder.n(actualTimestamp.toString).build)
      val ttlSeconds = (System.currentTimeMillis() / 1000).toInt + DataTTLSeconds
      val ttlMap = Map("ttl" -> AttributeValue.builder.n(ttlSeconds.toString).build)

      val putItemReq =
        PutItemRequest.builder.tableName(req.dataset).item((attributeMap ++ tsMap ++ ttlMap).toJava).build()
      val startTs = System.currentTimeMillis()
      handleDynamoDbOperation(metricsContext.withSuffix("multiput"), req.dataset, startTs)(
        dynamoDbClient.putItem(putItemReq)
      ).transform {
        case Success(_) => Success(true)
        case Failure(_) => Success(false)
      }
    }
    Future.sequence(futureResponses)
  }

  /** Bulk loads data from S3 Ion files into DynamoDB using the ImportTable API.
    *
    * The Ion files are expected to have been written by IonWriter during GroupByUpload.
    * The S3 location is determined by IonWriter.resolveS3Location using:
    *   - Root path from config: spark.chronon.table_write.upload.root_path
    *   - Dataset name: sourceOfflineTable (e.g., namespace.groupby_v1__upload)
    *   - Partition column and value: ds={partition}
    *
    * Full path: s3://{spark.chronon.table_write.upload.root_path}/{sourceOfflineTable}/ds={partition}/
    *
    * Creates a date-suffixed physical table (e.g. MY_GROUPBY_BATCH_2026_02_17) and registers the
    * mapping from logical dataset name to physical table in CHRONON_BATCH_TABLE_REGISTRY.
    */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val rootPath = conf.get(IonPathConfig.UploadLocationKey)
    val partitionColumn = conf.getOrElse(IonPathConfig.PartitionColumnKey, IonPathConfig.DefaultPartitionColumn)

    // Use shared IonWriter path resolution to ensure consistency between producer and consumer
    val path = IonWriter.resolvePartitionPath(sourceOfflineTable, partitionColumn, partition, rootPath)
    val s3Source = toS3BucketSource(path)
    val logicalTableName = destinationOnlineDataSet
    val timestamp = Instant.now().toEpochMilli
    val physicalTableName =
      logicalTableName.sanitize.toUpperCase + "_" + partition.replace("-", "_") + "_" + timestamp
    logger.info(
      s"Starting DynamoDB import for table: $physicalTableName (logical: $logicalTableName) from S3: $s3Source")

    val tableParams = TableCreationParameters
      .builder()
      .tableName(physicalTableName)
      .keySchema(
        KeySchemaElement.builder().attributeName(partitionKeyColumn).keyType(KeyType.HASH).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build()
      )
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .build()

    val importRequest = ImportTableRequest
      .builder()
      .s3BucketSource(s3Source)
      .inputFormat(InputFormat.ION)
      .inputCompressionType(InputCompressionType.NONE)
      .tableCreationParameters(tableParams)
      .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
      .build()

    try {
      val startTs = System.currentTimeMillis()
      val importResponse = dynamoDbClient.importTable(importRequest).join()
      val importArn = importResponse.importTableDescription().importArn()

      logger.info(s"DynamoDB import initiated with ARN: $importArn for table: $physicalTableName")

      waitForImportCompletion(importArn, physicalTableName)

      // Register the physical table name in the batch table registry
      create(batchTableRegistry)
      val registryKey = logicalTableName.sanitize.toUpperCase + batchSuffix
      Await.result(
        multiPut(Seq(KVStore.PutRequest(registryKey.getBytes, physicalTableName.getBytes, batchTableRegistry))),
        30.seconds
      )
      logger.info(s"Registry updated: $registryKey -> $physicalTableName")

      val duration = System.currentTimeMillis() - startTs
      logger.info(s"DynamoDB import completed for table: $physicalTableName in ${duration}ms")
      metricsContext.increment("bulkPut.successes")
      metricsContext.distribution("bulkPut.latency", duration)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to import data to DynamoDB table: $physicalTableName", e)
        metricsContext.increment("bulkPut.failures")
        throw e
    }
  }

  /** Converts a Hadoop Path to an S3BucketSource for DynamoDB ImportTable. */
  private def toS3BucketSource(path: org.apache.hadoop.fs.Path): S3BucketSource = {
    val uri = path.toUri
    S3BucketSource
      .builder()
      .s3Bucket(uri.getHost)
      .s3KeyPrefix(uri.getPath.stripPrefix("/") + "/")
      .build()
  }

  private def waitForImportCompletion(importArn: String, tableName: String): Unit = {
    val maxWaitTimeMs = 30 * 60 * 1000L // 30 minutes
    val pollIntervalMs = 10 * 1000L // 10 seconds
    val startTime = System.currentTimeMillis()

    var status: ImportStatus = ImportStatus.IN_PROGRESS
    var lastDescription: ImportTableDescription = null
    while (status == ImportStatus.IN_PROGRESS && (System.currentTimeMillis() - startTime) < maxWaitTimeMs) {
      Thread.sleep(pollIntervalMs)

      try {
        val describeRequest = DescribeImportRequest
          .builder()
          .importArn(importArn)
          .overrideConfiguration(DynamoDBKVStoreConstants.ControlPlaneApiOverride)
          .build()
        val describeResponse = dynamoDbClient.describeImport(describeRequest).join()
        lastDescription = describeResponse.importTableDescription()
        status = lastDescription.importStatus()

        val elapsed = (System.currentTimeMillis() - startTime) / 1000
        logger.info(
          s"DynamoDB import status for $tableName: $status " +
            s"(${elapsed}s elapsed, processed: ${lastDescription.processedItemCount()} items, " +
            s"imported: ${lastDescription.importedItemCount()} items, " +
            s"errors: ${lastDescription.errorCount()})")
      } catch {
        case e: Exception =>
          logger.error(s"Error polling import status for $tableName", e)
          throw e
      }
    }

    status match {
      case ImportStatus.COMPLETED =>
        logger.info(
          s"DynamoDB import completed successfully for table: $tableName " +
            s"(imported: ${lastDescription.importedItemCount()} items, errors: ${lastDescription.errorCount()})")
      case ImportStatus.FAILED | ImportStatus.CANCELLED =>
        val diagnostics =
          s"""DynamoDB import failed for table: $tableName
             |  Status: $status
             |  Failure Code: ${lastDescription.failureCode()}
             |  Failure Message: ${lastDescription.failureMessage()}
             |  Error Count: ${lastDescription.errorCount()}
             |  Processed Items: ${lastDescription.processedItemCount()}
             |  Imported Items: ${lastDescription.importedItemCount()}
             |  Import ARN: $importArn""".stripMargin
        logger.error(diagnostics)
        throw new RuntimeException(diagnostics)
      case ImportStatus.IN_PROGRESS =>
        throw new RuntimeException(s"DynamoDB import timed out after ${maxWaitTimeMs}ms for table: $tableName")
      case _ =>
        logger.warn(s"Unknown import status: $status for table: $tableName")
    }
  }

  protected def handleDynamoDbOperation[T](context: Context, dataset: String, startTs: Long)(
      completableFuture: CompletableFuture[T]): Future[T] = {
    FutureConverters.toScala(completableFuture).transform {
      case Success(result) =>
        context.distribution("latency", System.currentTimeMillis() - startTs)
        Success(result)
      case Failure(exception) =>
        exception match {
          case e: ProvisionedThroughputExceededException =>
            logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", e)
            context.increment("iops_error")
            Failure(e)
          case e: ResourceNotFoundException =>
            logger.error(s"Unable to trigger operation on $dataset as its not found", e)
            context.increment("missing_table")
            Failure(e)
          case e: CompletionException =>
            e.getCause match {
              case ce: ProvisionedThroughputExceededException =>
                logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", ce)
                context.increment("iops_error")
                Failure(ce)
              case ce: ResourceNotFoundException =>
                logger.error(s"Unable to trigger operation on $dataset as its not found", ce)
                context.increment("missing_table")
                Failure(ce)
              case _ =>
                logger.error("Error interacting with DynamoDB", e.getCause)
                context.increment("dynamodb_error")
                Failure(e.getCause)
            }
          case e: Exception =>
            logger.error("Error interacting with DynamoDB", e)
            context.increment("dynamodb_error")
            Failure(e)
        }
    }
  }

  protected def extractTimedValues(ddbResponseList: util.List[util.Map[String, AttributeValue]],
                                   defaultTimestamp: Long): Try[Seq[TimedValue]] = {
    Try {
      ddbResponseList.toScala.filterNot(_.isEmpty).map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala

        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())
        if (valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing valueBytes")

        val timestamp = responseMap.get(sortKeyColumn).map(v => v.n().toLong).getOrElse(defaultTimestamp)
        TimedValue(valueBytes.get, timestamp)
      }
    }
  }

  private def extractListValues(scanResponse: ScanResponse): Try[Seq[ListValue]] = {
    Try {
      scanResponse.items().toScala.filterNot(_.isEmpty).map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala

        val keyBytes = responseMap.get("keyBytes").map(v => v.b().asByteArray())
        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())

        if (keyBytes.isEmpty || valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing key / valueBytes")
        ListValue(keyBytes.get, valueBytes.get)
      }
    }
  }
}

object DynamoDBKVStoreConstants {
  val batchTableRegistry = "CHRONON_BATCH_TABLE_REGISTRY"
  val batchSuffix = "_BATCH"

  // Optional field that indicates if this table is meant to be time sorted in Dynamo or not
  val isTimedSorted = "is-time-sorted"

  // Name of the partition key column to use
  val partitionKeyColumn = "keyBytes"

  // Name of the time sort key column to use
  val sortKeyColumn = Constants.TimeColumn

  // Streaming tables use TileKey wrapping for tiled data.
  def isStreamingTable(dataset: String): Boolean = dataset.endsWith("_STREAMING")

  // Control plane operations (ImportTable, DeleteTable, DescribeImport) are slower than
  // data plane operations (GetItem, PutItem). Use higher timeouts to avoid intermittent failures.
  val ControlPlaneApiOverride: AwsRequestOverrideConfiguration = AwsRequestOverrideConfiguration
    .builder()
    .apiCallTimeout(Duration.ofSeconds(30))
    .apiCallAttemptTimeout(Duration.ofSeconds(10))
    .build()

  case class TileKeyComponents(baseKeyBytes: Array[Byte], tileSizeMillis: Long, tileStartTimestampMillis: Long)

  /** Unwraps a TileKey to extract the entity key for use as DynamoDB partition key.
    *
    * Streaming tables have two serialization layers:
    *   - Outer: Thrift (TileKey struct with dataset, keyBytes, tileSizeMs, tileStartTs)
    *   - Inner: Avro (entity key, e.g. customer_id, stored in TileKey.keyBytes)
    *
    * This method deserializes only the Thrift layer. The returned baseKeyBytes
    * remain Avro-encoded and are used directly as the DynamoDB partition key.
    */
  def extractTileKeyComponents(keyBytes: Array[Byte]): TileKeyComponents = {
    val tileKey = TilingUtils.deserializeTileKey(keyBytes)
    val baseKeyBytes = tileKey.keyBytes.toScala.map(_.toByte).toArray
    val tileSizeMs = tileKey.tileSizeMillis
    val tileStartTs = tileKey.tileStartTimestampMillis
    TileKeyComponents(baseKeyBytes, tileSizeMs, tileStartTs)
  }

  val DataTTLSeconds = 5.days.toSeconds.toInt
  val MillisPerDay = 1.day.toMillis

  def roundToDay(timestampMillis: Long): Long = {
    timestampMillis - (timestampMillis % MillisPerDay)
  }

  // Partition key format: {entity-key}#{dayTs}#{tileSizeMs}
  def buildKeyWithTileSize(baseKeyBytes: Array[Byte], timestampMillis: Long, tileSizeMs: Long): Array[Byte] = {
    val dayTs = roundToDay(timestampMillis)
    baseKeyBytes ++ s"#$dayTs#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }

  def generateTimeSeriesKeys(baseKeyBytes: Array[Byte],
                             startTs: Long,
                             endTs: Long,
                             tileSizeMs: Long): Seq[Array[Byte]] = {
    val startDay = roundToDay(startTs)
    val endDay = roundToDay(endTs)
    (startDay to endDay by MillisPerDay).map { dayTs =>
      buildKeyWithTileSize(baseKeyBytes, dayTs, tileSizeMs)
    }
  }

  def primaryKeyMap(keyBytes: Array[Byte]): Map[String, AttributeValue] = {
    Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(keyBytes)).build)
  }

  def buildAttributeMap(keyBytes: Array[Byte], valueBytes: Array[Byte]): Map[String, AttributeValue] = {
    primaryKeyMap(keyBytes) ++
      Map(
        "valueBytes" -> AttributeValue.builder.b(SdkBytes.fromByteArray(valueBytes)).build
      )
  }

  // Builds a DynamoDB query for a partition key with a time range on the sort key.
  def buildTimeRangeQuery(dataset: String,
                          partitionKeyBytes: Array[Byte],
                          startTs: Long,
                          endTs: Option[Long]): QueryRequest = {
    val partitionAlias = "#pk"
    val timeAlias = "#ts"
    val attrNameAliasMap = Map(partitionAlias -> partitionKeyColumn, timeAlias -> sortKeyColumn)
    val endTsResolved = endTs.getOrElse(System.currentTimeMillis())
    val attrValuesMap = Map(
      ":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build,
      ":start" -> AttributeValue.builder.n(startTs.toString).build,
      ":end" -> AttributeValue.builder.n(endTsResolved.toString).build
    )

    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue AND $timeAlias BETWEEN :start AND :end")
      .expressionAttributeNames(attrNameAliasMap.toJava)
      .expressionAttributeValues(attrValuesMap.toJava)
      .build
  }

  // Queries by partition key only — used for time-sorted tables when the sort key is unknown,
  // e.g. schema/metadata rows whose write timestamp isn't tracked at read time.
  def buildPartitionOnlyQuery(dataset: String, partitionKeyBytes: Array[Byte]): QueryRequest = {
    val partitionAlias = "#pk"
    QueryRequest.builder
      .tableName(dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue")
      .expressionAttributeNames(Map(partitionAlias -> partitionKeyColumn).toJava)
      .expressionAttributeValues(
        Map(":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(partitionKeyBytes)).build).toJava)
      .build
  }
}
