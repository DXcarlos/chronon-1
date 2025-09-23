package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Constants.{ContinuationKey, ListEntityType, ListLimit}
import ai.chronon.api.Extensions.{GroupByOps, StringOps, WindowOps, WindowUtils}
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec, TilingUtils}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{ListRequest, ListResponse, ListValue}
import ai.chronon.online.metrics.Metrics
import com.google.api.core.ApiFuture
import com.google.cloud.RetryOption
import com.google.cloud.bigquery._
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.{CreateTableRequest, GCRules}
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.Range.{ByteStringRange, TimestampRange}
import com.google.cloud.bigtable.data.v2.models.{Filters, Query, RowMutation, TableId => BTTableId}
import com.google.protobuf.ByteString
import org.slf4j.{Logger, LoggerFactory}
import org.threeten.bp.Duration

import java.nio.charset.Charset
import scala.collection.concurrent.TrieMap
import scala.collection.{Seq, mutable}
import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/** BigTable based KV store implementation. We store a few kinds of data in our KV store:
  * 1) Entity data - An example is thrift serialized Groupby / Join configs. If entities are updated / rewritten, we
  * serve the latest version.
  * 2) Timeseries data - This is either our batch IRs or streaming tiles for feature fetching. It also
  * includes drift / skew time summaries.
  *
  * We have multi use-case tables for the _BATCH and _STREAMING time series tile data.
  * To ensure that data from different groupBys are isolated from each other, we prefix the key with the dataset name:
  * Row key: dataset#key
  *
  * In case of time series data that is likely to see many data points per day (e.g. tile_summaries, streaming tiles), we
  * bucket the data by day to ensure that we don't need to filter a Row with thousands of cells (and also worry about the per Row size / cell count limits).
  * This also helps as GC in BigTable can take ~1 week. Without this day based bucketing we might have cells spanning a week.
  *
  * This row key structure looks like (tile size included in case of streaming tiles to support tile layering):
  * Row key: dataset#key#timestamp_rounded_to_day[#tileSize]
  *
  * Values are written to individual cells with timestamp of the time series point being the cell timestamp.
  *
  * Tables created via this client have a default TTL of 5 days and a max cell count of 10k. This is to ensure we don't
  * store data indefinitely and also to cap the amount of data we store.
  */
class BigTableKVStoreImpl(dataClient: BigtableDataClient,
                          maybeAdminClient: scala.Option[BigtableTableAdminClient] = None,
                          maybeBigQueryClient: scala.Option[BigQuery] = None,
                          conf: Map[String, String] = Map.empty)
    extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import BigTableKVStore._

  // We keep data around for a 5 day TTL. This gives us a little buffer in case of incidents while still capping our storage
  private val DataTTL = Duration.ofDays(5)

  // Cap the maximum number of cells we store.
  private val MaxCellCount = 10000

  // BT docs (https://cloud.google.com/bigtable/docs/garbage-collection) cover this more - union ensures we GC data if either rule is met
  private val DefaultGcRules =
    GCRules.GCRULES.union().rule(GCRules.GCRULES.maxAge(DataTTL)).rule(GCRules.GCRULES.maxVersions(MaxCellCount))

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("bigtable")

  protected val tableToContext = new TrieMap[String, Metrics.Context]()

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    maybeAdminClient
      .map { adminClient =>
        try {

          if (!adminClient.exists(dataset)) {

            // we can explore split points if we need custom tablet partitioning. For now though, we leave this to BT
            val createTableRequest = CreateTableRequest.of(dataset).addFamily(ColumnFamilyString, DefaultGcRules)
            val table = adminClient.createTable(createTableRequest)
            // TODO: this actually submits an async task. thus, the submission can succeed but the task can fail.
            //  doesn't return a future but maybe we can poll
            logger.info(s"Created table: $table")
            metricsContext.increment("create.successes")

          } else {

            logger.info(s"Table $dataset already exists")

          }
        } catch {

          case e: Exception =>
            logger.error("Error creating table", e)
            metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        }
      }
      .orElse(throw new IllegalStateException("Missing BigTable admin client. Is the ENABLE_UPLOAD_CLIENTS flag set?"))
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    logger.debug(s"Performing multi-get for ${requests.size} requests")

    // Group requests by dataset to minimize the number of BigTable calls
    val requestsByDataset = requests.groupBy(_.dataset)

    // For each dataset, make a single query with all relevant row keys
    val datasetFutures = readRowsMultiGet(requestsByDataset)
    // Combine results from all datasets
    Future.sequence(datasetFutures).map(_.flatten)
  }

  private def readRowsMultiGet(
      requestsByDataset: Map[String, Seq[KVStore.GetRequest]]): Seq[Future[Seq[KVStore.GetResponse]]] = {
    requestsByDataset.map { case (dataset, datasetRequests) =>
      val targetId = mapDatasetToTable(dataset)
      val datasetMetricsContext = tableToContext.getOrElseUpdate(
        targetId.toString,
        metricsContext.copy(dataset = targetId.toString)
      )

      // Create a single query for all requests in this dataset
      val query = Query
        .create(targetId)
        .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
        .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))

      // Track which request corresponds to which row key(s)
      val requestsWithRowKeys = datasetRequests.map { request =>
        val tableType = getTableType(dataset)
        val rowKeys = new mutable.ArrayBuffer[ByteString]()
        // Apply the appropriate filters based on request type
        (request.startTsMillis, tableType) match {
          case (Some(startTs), TileSummaries) =>
            val endTime = request.endTsMillis.getOrElse(System.currentTimeMillis())
            // Use existing method to add row keys
            val (_, addedRowKeys) = setQueryTimeSeriesFilters(query, startTs, endTime, request.keyBytes, dataset)
            rowKeys ++= addedRowKeys

          case (Some(startTs), StreamingTable) =>
            val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
            val tileSizeMs = tileKey.tileSizeMillis
            val baseKeyBytes = tileKey.keyBytes.asScala.map(_.asInstanceOf[Byte])
            val endTime = request.endTsMillis.getOrElse(System.currentTimeMillis())

            // Use existing method to add row keys
            val (_, addedRowKeys) =
              setQueryTimeSeriesFilters(query, startTs, endTime, baseKeyBytes, dataset, Some(tileSizeMs))
            rowKeys ++= addedRowKeys

          case _ =>
            // For non-timeseries data, just add the single row key
            val baseRowKey = buildRowKey(request.keyBytes, dataset)
            query.rowKey(ByteString.copyFrom(baseRowKey))
            query.filter(Filters.FILTERS.limit().cellsPerRow(1))
            rowKeys.append(ByteString.copyFrom(baseRowKey))
        }

        (request, rowKeys)
      }
      val startTs = System.currentTimeMillis()

      // Make a single BigTable call for all rows in this dataset
      val apiFuture = dataClient.readRowsCallable().all().futureCall(query)
      val scalaResultFuture = googleFutureToScalaFuture(apiFuture)

      // Process all results at once
      scalaResultFuture
        .map { rows =>
          datasetMetricsContext.distribution("multiGet.latency", System.currentTimeMillis() - startTs)
          datasetMetricsContext.increment("multiGet.successes")

          // Create a map for quick lookup by row key
          val rowKeyToRowMap = rows.asScala.map(row => row.getKey() -> row).toMap

          // Map back to original requests
          requestsWithRowKeys.map { case (request, rowKeys) =>
            // Get all cells from all row keys for this request
            val timedValues = rowKeys.flatMap { rowKey =>
              rowKeyToRowMap.get(rowKey).toSeq.flatMap { row =>
                row.getCells(ColumnFamilyString, ColumnFamilyQualifier).asScala.map { cell =>
                  KVStore.TimedValue(cell.getValue.toByteArray, cell.getTimestamp / 1000)
                }
              }
            }

            KVStore.GetResponse(request, Success(timedValues))
          }
        }
        .recover { case e: Exception =>
          logger.error("Error getting values", e)
          datasetMetricsContext.increment("multiGet.bigtable_errors", Map("exception" -> e.getClass.getName))
          // If the batch fails, return failures for all requests in the batch
          datasetRequests.map { request =>
            KVStore.GetResponse(request, Failure(e))
          }
        }
    }.toSeq
  }

  private def setQueryTimeSeriesFilters(query: Query,
                                        startTs: Long,
                                        endTs: Long,
                                        keyBytes: Seq[Byte],
                                        dataset: String,
                                        maybeTileSize: scala.Option[Long] = None): (Query, Iterable[ByteString]) = {
    // we need to generate a rowkey corresponding to each day from the startTs to now
    val millisPerDay = 1.day.toMillis

    val startDay = startTs - (startTs % millisPerDay)
    val endDay = endTs - (endTs % millisPerDay)
    // get the rowKeys
    val rowKeyByteStrings =
      (startDay to endDay by millisPerDay).map(dayTs => {
        val rowKey =
          maybeTileSize
            .map(tileSize => buildTiledRowKey(keyBytes, dataset, dayTs, tileSize))
            .getOrElse(buildRowKey(keyBytes, dataset, Some(dayTs)))
        val rowKeyByteString = ByteString.copyFrom(rowKey)
        query.rowKey(rowKeyByteString)
        rowKeyByteString
      })

    // Bigtable uses microseconds, and we need to scan from startTs (millis) to endTs (millis)
    (query.filter(Filters.FILTERS.timestamp().range().startClosed(startTs * 1000).endClosed(endTs * 1000)),
     rowKeyByteStrings)
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    logger.info(s"Performing list for ${request.dataset}")

    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => defaultListLimit
    }

    val maybeListEntityType = request.props.get(ListEntityType)
    val maybeStartKey = request.props.get(ContinuationKey)

    val targetId = mapDatasetToTable(request.dataset)
    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      targetId.toString,
      metricsContext.copy(dataset = targetId.toString)
    )
    val query = Query
      .create(targetId)
      .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
      .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))
      // we also limit to the latest cell per row as we don't want clients to iterate over all prior edits
      .filter(Filters.FILTERS.limit().cellsPerRow(1))
      .limit(listLimit)

    (maybeStartKey, maybeListEntityType) match {
      case (Some(startKey), _) =>
        // we have a start key, we use that to pick up from where we left off
        query.range(ByteStringRange.unbounded().startOpen(ByteString.copyFrom(startKey.asInstanceOf[Array[Byte]])))
      case (None, Some(listEntityType)) =>
        val startRowKey = buildRowKey(s"$listEntityType/".getBytes(Charset.forName("UTF-8")), request.dataset)
        query.range(ByteStringRange.unbounded().startOpen(ByteString.copyFrom(startRowKey)))
      case _ =>
        logger.info("No start key or list entity type provided. Starting from the beginning")
    }

    val startTs = System.currentTimeMillis()
    val rowsApiFuture = dataClient.readRowsCallable().all.futureCall(query)
    val rowsScalaFuture = googleFutureToScalaFuture(rowsApiFuture)

    rowsScalaFuture
      .map { rows =>
        datasetMetricsContext.distribution("list.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("list.successes")

        val listValues = rows.asScala.flatMap { row =>
          row.getCells(ColumnFamilyString, ColumnFamilyQualifier).asScala.map { cell =>
            ListValue(row.getKey.toByteArray, cell.getValue.toByteArray)
          }
        }

        val propsMap: Map[String, Any] =
          if (listValues.size < listLimit) {
            Map.empty // last page, we're done
          } else
            Map(ContinuationKey -> listValues.last.keyBytes)

        ListResponse(request, Success(listValues), propsMap)

      }
      .recover { case e: Exception =>
        logger.error("Error listing values", e)
        datasetMetricsContext.increment("list.bigtable_errors", Map("exception" -> e.getClass.getName))

        ListResponse(request, Failure(e), Map.empty)

      }
  }

  // We stick to individual put calls here as our invocations are fairly small sized sequences (couple of elements).
  // Using the individual mutate calls allows us to easily return fine-grained success/failure information in the form
  // our callers expect.
  override def multiPut(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} requests")
    val resultFutures = {
      requests.map { request =>
        val tableId = mapDatasetToTable(request.dataset)
        val datasetMetricsContext = tableToContext.getOrElseUpdate(
          tableId.toString,
          metricsContext.copy(dataset = tableId.toString)
        )

        val tableType = getTableType(request.dataset)
        val timestampInPutRequest = request.tsMillis.getOrElse(System.currentTimeMillis())

        val (rowKey, timestamp) = (request.tsMillis, tableType) match {
          case (Some(ts), TileSummaries) =>
            (buildRowKey(request.keyBytes, request.dataset, Some(ts)), timestampInPutRequest)
          case (Some(ts), StreamingTable) =>
            val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
            val baseKeyBytes = tileKey.keyBytes.asScala.map(_.asInstanceOf[Byte])
            (buildTiledRowKey(baseKeyBytes, request.dataset, ts, tileKey.tileSizeMillis),
             tileKey.tileStartTimestampMillis)
          case _ =>
            (buildRowKey(request.keyBytes, request.dataset), timestampInPutRequest)
        }

        val timestampMicros = timestamp * 1000
        val mutation = RowMutation.create(tableId, ByteString.copyFrom(rowKey))
        val cellValue = ByteString.copyFrom(request.valueBytes)
        // if we have prior cells with the same timestamp, we queue up a delete operation before the put
        mutation.deleteCells(ColumnFamilyString,
                             ColumnFamilyQualifier,
                             TimestampRange.create(timestampMicros, timestampMicros + 1000))
        mutation.setCell(ColumnFamilyString, ColumnFamilyQualifier, timestampMicros, cellValue)

        val startTs = System.currentTimeMillis()
        val mutateApiFuture = dataClient.mutateRowAsync(mutation)
        val scalaFuture = googleFutureToScalaFuture(mutateApiFuture)
        scalaFuture
          .map { _ =>
            datasetMetricsContext.distribution("multiPut.latency", System.currentTimeMillis() - startTs)
            datasetMetricsContext.increment("multiPut.successes")
            true
          }
          .recover { case e: Exception =>
            logger.error("Error putting data", e)
            datasetMetricsContext.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
            false
          }
      }
    }
    Future.sequence(resultFutures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val uploader = conf.getOrElse("UPLOADER", "bigquery")

    uploader match {
      case "spark"    => bulkPutFromSpark(sourceOfflineTable, destinationOnlineDataSet, partition)
      case "bigquery" => bulkPutFromBigQuery(sourceOfflineTable, destinationOnlineDataSet, partition)
      case other =>
        logger.error(s"Unsupported uploader: $other")
        metricsContext.increment("bulkPut.failures", Map("exception" -> "unsupported_uploader"))
        throw new IllegalArgumentException(s"Unsupported uploader: $other.")
    }
  }

  private def bulkPutFromSpark(sourceOfflineTable: String,
                               destinationOnlineDataSet: String,
                               partition: String): Unit = {
    if (maybeAdminClient.isEmpty) {
      logger.error("Need the BigTable admin client available to export data to BigTable")
      metricsContext.increment("bulkPut.failures", Map("exception" -> "missinguploadclients"))
      throw new RuntimeException("BigTable admin client is needed to export data to BigTable")
    }

    val adminClient = maybeAdminClient.get
    val startTs = System.currentTimeMillis()

    logger.info(
      s"Triggering Spark-based bulk load for dataset: $destinationOnlineDataSet, " +
        s"table: $sourceOfflineTable, partition: $partition")

    try {
      // Use Spark2BigTableLoader to load data from Hive/Delta tables
      val loaderArgs = Array(
        "--table-name",
        sourceOfflineTable,
        "--dataset",
        destinationOnlineDataSet,
        "--end-ds",
        partition,
        "--project-id",
        adminClient.getProjectId,
        "--instance-id",
        adminClient.getInstanceId
      )

      // Run the Spark job
      Spark2BigTableLoader.main(loaderArgs)

      logger.info("Spark-based bulk load completed successfully")
      metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
      metricsContext.increment("bulkPut.successes")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to run Spark-based bulk load for $sourceOfflineTable", e)
        metricsContext.increment("bulkPut.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  private def bulkPutFromBigQuery(sourceOfflineTable: String,
                                  destinationOnlineDataSet: String,
                                  partition: String): Unit = {
    if (maybeBigQueryClient.isEmpty || maybeAdminClient.isEmpty) {
      logger.error("Need the BigTable admin and BigQuery available to export data to BigTable")
      metricsContext.increment("bulkPut.failures", Map("exception" -> "missinguploadclients"))
      throw new RuntimeException("BigTable admin and BigQuery clients are needed to export data to BigTable")
    }

    val adminClient = maybeAdminClient.get

    // we write groupby data to 1 large multi use-case table
    val batchTable = "GROUPBY_BATCH"

    // we use the endDs + span to indicate the timestamp of all the cell data we upload for endDs
    // this is used in the KV store multiget calls
    val partitionSpec = PartitionSpec("ds", "yyyy-MM-dd", WindowUtils.Day.millis)
    val endDsPlusOne = partitionSpec.epochMillis(partition) + partitionSpec.spanMillis

    // we need to sanitize and append the batch suffix to the groupBy name as that's
    // what we use to look things up while fetching
    val groupBy = new GroupBy().setMetaData(new MetaData().setName(destinationOnlineDataSet))
    val datasetName = groupBy.batchDataset

    val exportQuery =
      s"""
         |EXPORT DATA OPTIONS (
         |  format='CLOUD_BIGTABLE',
         |  overwrite=true,
         |  uri="https://bigtable.googleapis.com/projects/${adminClient.getProjectId}/instances/${adminClient.getInstanceId}/appProfiles/GROUPBY_INGEST/tables/$batchTable",
         |  bigtable_options='''{
         |   "columnFamilies" : [
         |      {
         |        "familyId": "cf",
         |        "encoding": "BINARY",
         |        "columns": [
         |           {"qualifierString": "value", "fieldName": ""}
         |        ]
         |      }
         |   ]
         |}'''
         |) AS
         |SELECT
         |  CONCAT(CAST(CONCAT('$datasetName', '#') AS BYTES), key_bytes) as rowkey,
         |  value_bytes as cf,
         |  TIMESTAMP_MILLIS($endDsPlusOne) as _CHANGE_TIMESTAMP
         |FROM $sourceOfflineTable
         |WHERE ds = '$partition'
         |""".stripMargin
    logger.info(s"Kicking off bulkLoad with query:\n$exportQuery")

    maybeBigQueryClient.foreach { bigQueryClient =>
      val queryConfig = QueryJobConfiguration
        .newBuilder(exportQuery)
        .build()

      val startTs = System.currentTimeMillis()
      // we append the timestamp to the jobID as BigQuery doesn't allow us to re-run the same job
      val jobId =
        JobId.of(adminClient.getProjectId, s"export_${sourceOfflineTable.sanitize}_to_bigtable_${partition}_$startTs")
      val job: Job = bigQueryClient.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
      logger.info(s"Export job started with Id: $jobId and link: ${job.getSelfLink}")
      val retryConfig =
        BigQueryRetryConfig.newBuilder
          .retryOnMessage(BigQueryErrorMessages.RATE_LIMIT_EXCEEDED_MSG)
          .retryOnMessage(BigQueryErrorMessages.JOB_RATE_LIMIT_EXCEEDED_MSG)
          .build

      val initialRetryDelay = Duration.ofMinutes(1)
      val totalRetryTimeout = Duration.ofHours(6)
      logger.info(s"We will wait for $totalRetryTimeout for the job to complete")
      val completedJob = job.waitFor(retryConfig,
                                     RetryOption.initialRetryDelay(initialRetryDelay),
                                     RetryOption.totalTimeout(totalRetryTimeout))
      if (completedJob == null) {
        // job no longer exists
        logger.error(s"Job corresponding to $jobId no longer exists")
        metricsContext.increment("bulkPut.failures", Map("exception" -> "missingjob"))
        throw new RuntimeException(s"Export job corresponding to $jobId no longer exists")
      } else if (completedJob.getStatus.getError != null) {
        logger.error(s"Job failed with error: ${completedJob.getStatus.getError}")
        metricsContext.increment("bulkPut.failures",
                                 Map("exception" -> s"${completedJob.getStatus.getError.getReason}"))
        throw new RuntimeException(s"Export job failed with error: ${completedJob.getStatus.getError}")
      } else {
        logger.info("Export job completed successfully")
        metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
        metricsContext.increment("bulkPut.successes")
      }
    }
  }
}

object BigTableKVStore {

  // Default list limit
  val defaultListLimit: Int = 100

  sealed trait TableType
  case object BatchTable extends TableType
  case object StreamingTable extends TableType
  case object TileSummaries extends TableType

  /** row key (with tiling) convention:
    * <dataset>#<entity-key>#<start_date>#<tile_size>
    *
    *  row key (without tiling) convention:
    *  <dataset>#<entity_key>#<start_date>
    */
  def buildTiledRowKey(baseKeyBytes: Seq[Byte], dataset: String, ts: Long, tileSizeMs: Long): Array[Byte] = {
    val baseRowKey = s"$dataset#".getBytes(Charset.forName("UTF-8")) ++ baseKeyBytes
    val dayTs = ts - (ts % 1.day.toMillis)
    baseRowKey ++ s"#$dayTs".getBytes(Charset.forName("UTF-8")) ++ s"#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }

  // We prefix the dataset name to the key to ensure we can have multiple datasets in the same table
  def buildRowKey(baseKeyBytes: Seq[Byte], dataset: String, maybeTs: scala.Option[Long] = None): Array[Byte] = {
    val baseRowKey = s"$dataset#".getBytes(Charset.forName("UTF-8")) ++ baseKeyBytes
    maybeTs match {
      case Some(ts) =>
        // For time series data, we append the day timestamp to the row key to ensure that time series points across different
        // days are split across rows
        val dayTs = ts - (ts % 1.day.toMillis)
        baseRowKey ++ s"#$dayTs".getBytes(Charset.forName("UTF-8"))
      case _ => baseRowKey
    }
  }

  def mapDatasetToTable(dataset: String): BTTableId = {
    if (dataset.endsWith("_BATCH")) {
      BTTableId.of("GROUPBY_BATCH")
    } else if (dataset.endsWith("_STREAMING")) {
      BTTableId.of("GROUPBY_STREAMING")
    } else {
      BTTableId.of(dataset)
    }
  }

  def getTableType(dataset: String): TableType = {
    dataset match {
      case d if d.endsWith("_BATCH")     => BatchTable
      case d if d.endsWith("_STREAMING") => StreamingTable
      case d if d.endsWith("SUMMARIES")  => TileSummaries
      case _                             => BatchTable // default to batch table for tables like chronon_metadata
    }
  }

  def googleFutureToScalaFuture[T](apiFuture: ApiFuture[T]): Future[T] = {
    val completableFuture = ApiFutureUtils.toCompletableFuture(apiFuture)
    FutureConverters.toScala(completableFuture)
  }

  val ColumnFamilyString: String = "cf"
  val ColumnFamilyQualifierString: String = "value"
  val ColumnFamilyQualifier: ByteString = ByteString.copyFromUtf8(ColumnFamilyQualifierString)
}
