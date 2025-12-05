package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.KVStore
import ai.chronon.online.metrics.Metrics
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.{CreateTableRequest, GCRules}
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.{Filters, Query, RowMutation, TableId => BTTableId}
import com.google.protobuf.ByteString
import org.slf4j.{Logger, LoggerFactory}
import org.threeten.bp.Duration

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneOffset}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/** BigTable-based KV store implementation specifically for enhanced statistics.
  *
  * This implementation follows BigTable time-series best practices:
  * - Key schema: conf_name#date_stamp (YYYY-MM-DD format)
  * - Column families:
  *   - "data": stores the statistical data (IRs, aggregated values)
  *   - "cardinalityMap": stores cardinality metadata
  * - Efficient date range queries by generating row keys for each date in the range
  *
  * Each row contains data for a specific configuration on a specific date.
  * Multiple cells within a row can represent different time buckets (hourly data, etc.)
  * using cell timestamps.
  *
  * Reference: https://cloud.google.com/bigtable/docs/schema-design-time-series
  */
class EnhancedDatasetKVStoreImpl(dataClient: BigtableDataClient,
                                 tableBaseName: String,
                                 maybeAdminClient: Option[BigtableTableAdminClient] = None,
                                 conf: Map[String, String] = Map.empty)
    extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import EnhancedDatasetKVStoreImpl._

  // Keep enhanced stats data for 90 days
  private val DataTTL = Duration.ofDays(90)

  // Cap the maximum number of cells per row (allows many hourly data points per day)
  private val MaxCellCount = 2000

  // GC rules for enhanced stats table
  private val DefaultGcRules =
    GCRules.GCRULES.union().rule(GCRules.GCRULES.maxAge(DataTTL)).rule(GCRules.GCRULES.maxVersions(MaxCellCount))

  protected val metricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.KVStore).withSuffix("enhanced_stats")

  protected val tableToContext = new TrieMap[String, Metrics.Context]()

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {
    maybeAdminClient
      .map { adminClient =>
        try {
          // For admin operations, use just the table name
          if (!adminClient.exists(tableBaseName)) {
            val createTableRequest = CreateTableRequest
              .of(tableBaseName)
              .addFamily(DataColumnFamily, DefaultGcRules)
              .addFamily(CardinalityMapColumnFamily, DefaultGcRules)

            val table = adminClient.createTable(createTableRequest)
            logger.info(
              s"Created enhanced stats table: $table with column families: $DataColumnFamily, $CardinalityMapColumnFamily")
            metricsContext.increment("create.successes")
          } else {
            logger.info(s"Enhanced stats table $tableBaseName already exists")
          }
        } catch {
          case e: Exception =>
            logger.error("Error creating enhanced stats table", e)
            metricsContext.increment("create.failures", Map("exception" -> e.getClass.getName))
        }
      }
      .orElse(throw new IllegalStateException("Missing BigTable admin client. Is the ENABLE_UPLOAD_CLIENTS flag set?"))
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    logger.debug(s"Performing multi-get for ${requests.size} enhanced stats requests")

    val requestsByDataset = requests.groupBy(_.dataset)
    val datasetFutures = readRowsMultiGet(requestsByDataset)
    Future.sequence(datasetFutures).map(_.flatten)
  }

  private def readRowsMultiGet(
      requestsByDataset: Map[String, Seq[KVStore.GetRequest]]): Seq[Future[Seq[KVStore.GetResponse]]] = {
    requestsByDataset.map { case (dataset, datasetRequests) =>
      val targetId = BTTableId.of(tableBaseName)

      // Build query for this dataset
      val query = Query
        .create(targetId)
        .filter(Filters.FILTERS.family().exactMatch(DataColumnFamily))

      val (finalQuery, requestsWithRowKeys) =
        datasetRequests.foldLeft((query, List.empty[(KVStore.GetRequest, mutable.ArrayBuffer[ByteString])])) {
          case ((currentQuery, acc), request) =>
            val rowKeys = new mutable.ArrayBuffer[ByteString]()

            // Enhanced stats are always time series
            val startTs = request.startTsMillis.getOrElse(System.currentTimeMillis() - 1.day.toMillis)
            val endTime = request.endTsMillis.getOrElse(System.currentTimeMillis())
            val (updatedQuery, addedRowKeys) =
              setQueryTimeSeriesFilters(currentQuery, startTs, endTime, request.keyBytes)
            rowKeys ++= addedRowKeys

            (updatedQuery, acc :+ (request, rowKeys))
        }

      val startTs = System.currentTimeMillis()
      val apiFuture = dataClient.readRowsCallable().all().futureCall(finalQuery)
      val scalaResultFuture = BigTableKVStore.googleFutureToScalaFuture(apiFuture)

      scalaResultFuture
        .map { rows =>
          val rowKeyToRowMap = rows.asScala.map(row => row.getKey() -> row).toMap

          requestsWithRowKeys.map { case (request, rowKeys) =>
            val timedValues = rowKeys.flatMap { rowKey =>
              rowKeyToRowMap.get(rowKey).toSeq.flatMap { row =>
                // Get all cells from both column families
                row.getCells(DataColumnFamily).asScala.map { cell =>
                  KVStore.TimedValue(cell.getValue.toByteArray, cell.getTimestamp / 1000)
                }
              }
            }.toSeq
            KVStore.GetResponse(request, Success(timedValues))
          }
        }
        .recover { case e: Exception =>
          logger.error("Error getting enhanced stats values", e)
          datasetRequests.map { request =>
            KVStore.GetResponse(request, Failure(e))
          }
        }
    }.toSeq
  }

  private def setQueryTimeSeriesFilters(query: Query,
                                        startTs: Long,
                                        endTs: Long,
                                        keyBytes: Seq[Byte]): (Query, Iterable[ByteString]) = {

    val keyString = new String(keyBytes.toArray, StandardCharsets.UTF_8)

    // Special case for schema and non-time-dependent metadata rows - they don't have date suffix
    if (
      keyString.endsWith("__keySchema") ||
      keyString.endsWith("__valueSchema") ||
      keyString.contains("/selectedSchema") ||
      keyString.contains("/cardinalityMap") ||
      keyString.contains("/noKeysSchema")
    ) {
      val schemaRowKey = buildEnhancedStatsRowKey(keyBytes, None)
      val schemaRowKeyByteString = ByteString.copyFrom(schemaRowKey)
      query.rowKey(schemaRowKeyByteString)

      // For schema/metadata rows, get the latest timestamped cell without time range filters
      (query, List(schemaRowKeyByteString))
    } else {
      // Generate row keys for each date in the range for regular data rows
      val dates = generateDateRange(startTs, endTs)

      val rowKeyByteStrings = dates.map { dateTimestamp =>
        val rowKey = buildEnhancedStatsRowKey(keyBytes, Some(dateTimestamp))
        val rowKeyByteString = ByteString.copyFrom(rowKey)
        query.rowKey(rowKeyByteString)
        rowKeyByteString
      }

      // Apply timestamp filter to only get cells within the requested time range
      val modifiedQuery =
        query.filter(Filters.FILTERS.timestamp().range().startClosed(startTs * 1000).endClosed(endTs * 1000))
      (modifiedQuery, rowKeyByteStrings)
    }
  }

  /** Generate date range from start to end timestamp (inclusive).
    * Returns timestamps for the start of each date in the range.
    */
  private def generateDateRange(startTs: Long, endTs: Long): List[Long] = {
    val startZoned = Instant.ofEpochMilli(startTs).atZone(ZoneOffset.UTC)
    val endZoned = Instant.ofEpochMilli(endTs).atZone(ZoneOffset.UTC)

    val startDate = startZoned.toLocalDate
    val endDate = endZoned.toLocalDate

    Iterator
      .iterate(startDate)(_.plusDays(1))
      .takeWhile(!_.isAfter(endDate))
      .map(_.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli)
      .toList
  }

  override def list(request: KVStore.ListRequest): Future[KVStore.ListResponse] = ???

  override def multiPut(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} enhanced stats requests")

    val resultFutures = requests.map { request =>
      val tableId = BTTableId.of(tableBaseName)
      val timestampInPutRequest = request.tsMillis

      // Determine which column family to use and the base key
      val keyString = new String(request.keyBytes, StandardCharsets.UTF_8)

      // Schema and static metadata keys should NOT have date suffix
      val isSchemaOrMetadata = keyString.endsWith("__keySchema") ||
        keyString.endsWith("__valueSchema") ||
        keyString.contains("/selectedSchema") ||
        keyString.contains("/cardinalityMap") ||
        keyString.contains("/noKeysSchema")

      // Build row key - add date suffix for data and cardinalityMap (daily data)
      val rowKey = if (isSchemaOrMetadata) {
        buildEnhancedStatsRowKey(request.keyBytes, None)
      } else {
        buildEnhancedStatsRowKey(request.keyBytes, timestampInPutRequest)
      }

      // For data points, use the provided timestamp; for metadata, use current time
      val timestampMillis = timestampInPutRequest.getOrElse(System.currentTimeMillis())
      val timestampMicros = timestampMillis * 1000

      val mutation = RowMutation.create(tableId, ByteString.copyFrom(rowKey))

      // Store the value bytes in the appropriate column family
      val cellValue = ByteString.copyFrom(request.valueBytes)
      val qualifier = ByteString.copyFromUtf8("value")
      mutation.setCell(DataColumnFamily, qualifier, timestampMicros, cellValue)

      val startTs = System.currentTimeMillis()
      val mutateApiFuture = dataClient.mutateRowAsync(mutation)
      val scalaFuture = BigTableKVStore.googleFutureToScalaFuture(mutateApiFuture)

      scalaFuture
        .map { _ =>
          true
        }
        .recover { case e: Exception =>
          logger.error("Error putting enhanced stats data", e)
          false
        }
    }
    Future.sequence(resultFutures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    throw new UnsupportedOperationException("bulkPut is not supported for EnhancedDatasetKVStoreImpl")
  }

  /** Map dataset to table ID for enhanced stats.
    * Returns a BTTableId object suitable for data operations.
    */
  def mapDatasetToTable(dataset: String): BTTableId = {
    BTTableId.of(tableBaseName)
  }
}

private object EnhancedDatasetKVStoreImpl {

  val DataColumnFamily: String = "data"
  val CardinalityMapColumnFamily: String = "cardinalityMap"

  /** Build row key for enhanced stats.
    *
    * Format:
    * - For data: conf_name#YYYY-MM-DD (one row per configuration per day)
    * - For metadata/schema: conf_name (no date suffix)
    */
  private def buildEnhancedStatsRowKey(baseKeyBytes: Seq[Byte], maybeTs: Option[Long] = None): Array[Byte] = {
    (maybeTs match {
      case Some(ts) =>
        val dateString = formatDate(ts)
        baseKeyBytes ++ s"#$dateString".getBytes(StandardCharsets.UTF_8)
      case _ => baseKeyBytes
    }).toArray
  }

  /** Format timestamp to date string (YYYY-MM-DD) */
  private def formatDate(timestampMillis: Long): String = {
    val instant = Instant.ofEpochMilli(timestampMillis)
    val localDate = instant.atZone(ZoneOffset.UTC).toLocalDate
    localDate.format(DateTimeFormatter.ISO_LOCAL_DATE)
  }
}
