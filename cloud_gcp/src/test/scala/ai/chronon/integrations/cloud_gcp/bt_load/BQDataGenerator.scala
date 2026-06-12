package ai.chronon.integrations.cloud_gcp.bt_load

import com.google.cloud.bigquery.{BigQuery, DatasetId, DatasetInfo, JobId, JobInfo, QueryJobConfiguration, TableId}
import org.slf4j.{Logger, LoggerFactory}

/** Synthesizes a BigQuery source table with the schema expected by bulkPutFromBigQuery:
  *   key_bytes  BYTES  (~32-64 bytes, two concatenated MD5 hashes of the row index)
  *   value_bytes BYTES (~2KB, padded to exactly 2048 bytes)
  *   ds         STRING (the partition label passed to bulkPut)
  *
  * Generation runs entirely inside BigQuery — no local data movement.
  */
object BQDataGenerator {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(
      bigQueryClient: BigQuery,
      project: String,
      bqDataset: String,
      bqTable: String,
      numRows: Long,
      partition: String
  ): Unit = {
    val fqTable = s"`$project.$bqDataset.$bqTable`"
    logger.info(s"Generating $numRows rows into $fqTable (ds='$partition')")

    // Create the dataset if it doesn't already exist; swallow ALREADY_EXISTS races.
    try {
      bigQueryClient.create(DatasetInfo.newBuilder(DatasetId.of(project, bqDataset)).build())
      logger.info(s"BQ dataset $project:$bqDataset created")
    } catch {
      case e: com.google.cloud.bigquery.BigQueryException if e.getCode == 409 =>
        logger.info(s"BQ dataset $project:$bqDataset already exists")
    }

    // Two MD5 hashes concatenated → 32 bytes per key (well within 30-50B target).
    // Value is padded to exactly 2048 bytes using b'\x00'.
    // GENERATE_ARRAY is limited to ~10M elements per call; for larger counts we use
    // a cross-join of two arrays (rows = a_size * b_size) where each factor ≤ ceiling(sqrt(numRows)).
    val (aSize, bSize) = splitRowCount(numRows)
    // The bulkPut export SQL only reads key_bytes, value_bytes, and filters WHERE ds = '$partition'.
    // No partitioning is needed on this staging table.
    val sql =
      s"""CREATE OR REPLACE TABLE $fqTable
         |AS
         |SELECT
         |  CONCAT(
         |    MD5(CAST(a AS STRING)),
         |    MD5(CAST(a * 7 + b AS STRING))
         |  ) AS key_bytes,
         |  RPAD(MD5(CAST(a + b AS STRING)), 2048, b'\\x00') AS value_bytes,
         |  '$partition' AS ds
         |FROM
         |  UNNEST(GENERATE_ARRAY(0, ${aSize - 1})) AS a,
         |  UNNEST(GENERATE_ARRAY(0, ${bSize - 1})) AS b
         |WHERE a * $bSize + b < $numRows
         |""".stripMargin

    logger.info(s"Running BQ CREATE TABLE AS:\n$sql")
    val jobConfig = QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build()
    val jobId = JobId.of(project, s"bt_load_gen_${partition.replace("-", "")}_${System.currentTimeMillis()}")
    val job = bigQueryClient.create(JobInfo.newBuilder(jobConfig).setJobId(jobId).build())
    logger.info(s"BQ data-gen job started: ${job.getJobId} — link: ${job.getSelfLink}")

    val completed = job.waitFor()
    if (completed == null) {
      throw new RuntimeException(s"BQ data-gen job ${job.getJobId} no longer exists")
    } else if (completed.getStatus.getError != null) {
      throw new RuntimeException(s"BQ data-gen job failed: ${completed.getStatus.getError}")
    }

    val tableInfo = bigQueryClient.getTable(TableId.of(project, bqDataset, bqTable))
    val actualRows = tableInfo.getNumRows
    val numBytes = tableInfo.getNumBytes
    logger.info(s"BQ data-gen complete: $actualRows rows, $numBytes bytes in $fqTable")
  }

  // GENERATE_ARRAY supports up to ~Int.MaxValue elements; for numRows > 10M we use a cross-join
  // of two arrays whose product equals numRows.
  private[bt_load] def splitRowCount(numRows: Long): (Long, Long) = {
    val aSize = math.ceil(math.sqrt(numRows.toDouble)).toLong
    val bSize = math.ceil(numRows.toDouble / aSize).toLong
    (aSize, bSize)
  }
}
