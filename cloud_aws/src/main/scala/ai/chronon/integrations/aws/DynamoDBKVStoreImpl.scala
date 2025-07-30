package ai.chronon.integrations.aws

import ai.chronon.api.Constants
import ai.chronon.api.Constants.{ContinuationKey, ListLimit}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.ListResponse
import ai.chronon.online.KVStore.ListValue
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.metrics.Metrics.Context
import ai.chronon.online.metrics.Metrics
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

import java.time.Instant
import java.util
import scala.concurrent.Future
import scala.util.Success
import scala.util.Try
import scala.collection.Seq

object DynamoDBKVStoreConstants {
  // Read capacity units to configure DynamoDB table with
  val readCapacityUnits = "read-capacity"

  // Write capacity units to configure DynamoDB table with
  val writeCapacityUnits = "write-capacity"

  // Optional field that indicates if this table is meant to be time sorted in Dynamo or not
  val isTimedSorted = "is-time-sorted"

  // Name of the partition key column to use
  val partitionKeyColumn = "keyBytes"

  // Name of the time sort key column to use
  val sortKeyColumn = Constants.TimeColumn

  // TODO: tune these
  val defaultReadCapacityUnits = 10L
  val defaultWriteCapacityUnits = 10L
}

class DynamoDBKVStoreImpl(dynamoDbClient: DynamoDbClient) extends KVStore {
  import DynamoDBKVStoreConstants._

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("dynamodb")

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    val dbWaiter = dynamoDbClient.waiter
    val maybeSortKeys = props.get(isTimedSorted) match {
      case Some(value: String) if value.toLowerCase == "true" => Some(sortKeyColumn)
      case Some(value: Boolean) if value                      => Some(sortKeyColumn)
      case _                                                  => None
    }

    val keyAttributes =
      Seq(AttributeDefinition.builder.attributeName(partitionKeyColumn).attributeType(ScalarAttributeType.B).build) ++
        maybeSortKeys.map(k => AttributeDefinition.builder.attributeName(k).attributeType(ScalarAttributeType.N).build)

    val keySchema =
      Seq(KeySchemaElement.builder.attributeName(partitionKeyColumn).keyType(KeyType.HASH).build) ++
        maybeSortKeys.map(p => KeySchemaElement.builder.attributeName(p).keyType(KeyType.RANGE).build)

    val rcu = getCapacityUnits(props, readCapacityUnits, defaultReadCapacityUnits)
    val wcu = getCapacityUnits(props, writeCapacityUnits, defaultWriteCapacityUnits)

    val request =
      CreateTableRequest.builder
        .attributeDefinitions(keyAttributes.toList.toJava)
        .keySchema(keySchema.toList.toJava)
        .provisionedThroughput(ProvisionedThroughput.builder.readCapacityUnits(rcu).writeCapacityUnits(wcu).build)
        .tableName(dataset)
        .build

    logger.info(s"Triggering creation of DynamoDb table: $dataset")
    try {
      val _ = dynamoDbClient.createTable(request)
      val tableRequest = DescribeTableRequest.builder.tableName(dataset).build
      // Wait until the Amazon DynamoDB table is created.
      val waiterResponse = dbWaiter.waitUntilTableExists(tableRequest)
      if (waiterResponse.matched.exception().isPresent)
        throw waiterResponse.matched.exception().get()

      val tableDescription = waiterResponse.matched().response().get().table()
      logger.info(s"Table created successfully! Details: \n${tableDescription.toString}")
      metricsContext.increment("create.successes")
    } catch {
      case _: ResourceInUseException => logger.info(s"Table: $dataset already exists")
      case e: Exception =>
        logger.error(s"Error creating Dynamodb table: $dataset", e)
        metricsContext.increment("create.failures")
        throw e
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    // partition our requests into pure get style requests (where we're missing timestamps and only have key lookup)
    // and query requests (we want to query a range based on afterTsMillis -> endTsMillis or now() )
    val (getLookups, queryLookups) = requests.partition(r => r.startTsMillis.isEmpty)
    val getItemRequestPairs = getLookups.map { req =>
      val keyAttributeMap = primaryKeyMap(req.keyBytes)
      (req, GetItemRequest.builder.key(keyAttributeMap.toJava).tableName(req.dataset).build)
    }

    val queryRequestPairs = queryLookups.map { req =>
      val queryRequest: QueryRequest = buildQueryRequest(req)
      (req, queryRequest)
    }

    // timestamp to use for all get responses when the underlying tables don't have a ts field
    val defaultTimestamp = Instant.now().toEpochMilli

    val getItemResults = getItemRequestPairs.map { case (req, getItemReq) =>
      Future {
        val item: Try[util.Map[String, AttributeValue]] =
          handleDynamoDbOperation(metricsContext.withSuffix("multiget"), req.dataset) {
            dynamoDbClient.getItem(getItemReq).item()
          }

        val response = item.map(i => List(i).toJava)
        val resultValue: Try[Seq[TimedValue]] = extractTimedValues(response, defaultTimestamp)
        GetResponse(req, resultValue)
      }
    }

    val queryResults = queryRequestPairs.map { case (req, queryRequest) =>
      Future {
        val responses = handleDynamoDbOperation(metricsContext.withSuffix("query"), req.dataset) {
          dynamoDbClient.query(queryRequest).items()
        }
        val resultValue: Try[Seq[TimedValue]] = extractTimedValues(responses, defaultTimestamp)
        GetResponse(req, resultValue)
      }
    }

    Future.sequence(getItemResults ++ queryResults)
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

    Future {
      val tryScanResponse = handleDynamoDbOperation(metricsContext.withSuffix("list"), request.dataset) {
        dynamoDbClient.scan(scanRequest)
      }
      val resultElements = extractListValues(tryScanResponse)
      val noPagesLeftResponse = ListResponse(request, resultElements, Map.empty)
      val listResponse = tryScanResponse match {
        case Success(scanResponse) if scanResponse.hasLastEvaluatedKey =>
          val lastEvalKey = scanResponse.lastEvaluatedKey().toScala.get(partitionKeyColumn)
          lastEvalKey match {
            case Some(av) => ListResponse(request, resultElements, Map(ContinuationKey -> av.b().asByteArray()))
            case _        => noPagesLeftResponse
          }
        case _ => noPagesLeftResponse
      }

      listResponse
    }
  }

  // Dynamo has restrictions on the number of requests per batch (and the payload size) as well as some partial
  // success behavior on batch writes which necessitates a bit more logic on our end to tie things together.
  // To keep things simple for now, we implement the multiput as a sequence of put calls.
  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.info(s"Triggering multiput for ${keyValueDatasets.size}: rows")
    val datasetToWriteRequests = keyValueDatasets.map { req =>
      val attributeMap: Map[String, AttributeValue] = buildAttributeMap(req.keyBytes, req.valueBytes)
      val tsMap =
        req.tsMillis.map(ts => Map(sortKeyColumn -> AttributeValue.builder.n(ts.toString).build)).getOrElse(Map.empty)

      val putItemReq =
        PutItemRequest.builder.tableName(req.dataset).item((attributeMap ++ tsMap).toJava).build()
      (req.dataset, putItemReq)
    }

    val futureResponses = datasetToWriteRequests.map { case (dataset, putItemRequest) =>
      Future {
        handleDynamoDbOperation(metricsContext.withSuffix("multiput"), dataset) {
          dynamoDbClient.putItem(putItemRequest)
        }.isSuccess
      }
    }
    Future.sequence(futureResponses)
  }

  /** Implementation of bulkPut is currently a TODO for the DynamoDB store. This involves transforming the underlying
    * Parquet data to Amazon's Ion format + swapping out old table for new (as bulkLoad only writes to new tables)
    */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = ???

  private def getCapacityUnits(props: Map[String, Any], key: String, defaultValue: Long): Long = {
    props.get(key) match {
      case Some(value: Long)   => value
      case Some(value: String) => value.toLong
      case _                   => defaultValue
    }
  }

  private def handleDynamoDbOperation[T](context: Context, dataset: String)(operation: => T): Try[T] = {
    Try {
      val startTs = System.currentTimeMillis()
      val result = operation
      context.distribution("latency", System.currentTimeMillis() - startTs)
      result
    }.recover {
      // log and emit metrics
      case e: ProvisionedThroughputExceededException =>
        logger.error(s"Provisioned throughput exceeded as we are low on IOPS on $dataset", e)
        context.increment("iops_error")
        throw e
      case e: ResourceNotFoundException =>
        logger.error(s"Unable to trigger operation on $dataset as its not found", e)
        context.increment("missing_table")
        throw e
      case e: Exception =>
        logger.error("Error interacting with DynamoDB", e)
        context.increment("dynamodb_error")
        throw e
    }
  }

  private def extractTimedValues(response: Try[util.List[util.Map[String, AttributeValue]]],
                                 defaultTimestamp: Long): Try[Seq[TimedValue]] = {
    response.map { ddbResponseList =>
      ddbResponseList.toScala.map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala
        if (responseMap.isEmpty)
          throw new Exception("Empty response returned from DynamoDB")

        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())
        if (valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing valueBytes")

        val timestamp = responseMap.get(sortKeyColumn).map(v => v.n().toLong).getOrElse(defaultTimestamp)
        TimedValue(valueBytes.get, timestamp)
      }
    }
  }

  private def extractListValues(tryScanResponse: Try[ScanResponse]): Try[Seq[ListValue]] = {
    tryScanResponse.map { response =>
      val ddbResponseList = response.items()
      ddbResponseList.toScala.map { ddbResponseMap =>
        val responseMap = ddbResponseMap.toScala
        if (responseMap.isEmpty)
          throw new Exception("Empty response returned from DynamoDB")

        val keyBytes = responseMap.get("keyBytes").map(v => v.b().asByteArray())
        val valueBytes = responseMap.get("valueBytes").map(v => v.b().asByteArray())

        if (keyBytes.isEmpty || valueBytes.isEmpty)
          throw new Exception("DynamoDB response missing key / valueBytes")
        ListValue(keyBytes.get, valueBytes.get)
      }
    }
  }

  private def primaryKeyMap(keyBytes: Array[Byte]): Map[String, AttributeValue] = {
    Map(partitionKeyColumn -> AttributeValue.builder.b(SdkBytes.fromByteArray(keyBytes)).build)
  }

  private def buildAttributeMap(keyBytes: Array[Byte], valueBytes: Array[Byte]): Map[String, AttributeValue] = {
    primaryKeyMap(keyBytes) ++
      Map(
        "valueBytes" -> AttributeValue.builder.b(SdkBytes.fromByteArray(valueBytes)).build
      )
  }

  private def buildQueryRequest(request: KVStore.GetRequest): QueryRequest = {
    // Set up an alias for the partition key name in case it's a reserved word.
    val partitionAlias = "#pk"
    val timeAlias = "#ts"
    val attrNameAliasMap = Map(partitionAlias -> partitionKeyColumn, timeAlias -> sortKeyColumn)
    val startTs = request.startTsMillis.get
    val endTs = request.endTsMillis.getOrElse(System.currentTimeMillis())
    val attrValuesMap =
      Map(
        ":partitionKeyValue" -> AttributeValue.builder.b(SdkBytes.fromByteArray(request.keyBytes)).build,
        ":start" -> AttributeValue.builder.n(startTs.toString).build,
        ":end" -> AttributeValue.builder.n(endTs.toString).build
      )

    QueryRequest.builder
      .tableName(request.dataset)
      .keyConditionExpression(s"$partitionAlias = :partitionKeyValue AND $timeAlias BETWEEN :start AND :end")
      .expressionAttributeNames(attrNameAliasMap.toJava)
      .expressionAttributeValues(attrValuesMap.toJava)
      .build
  }
}
