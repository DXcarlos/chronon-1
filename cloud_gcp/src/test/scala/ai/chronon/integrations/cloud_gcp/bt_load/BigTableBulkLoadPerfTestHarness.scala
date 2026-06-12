package ai.chronon.integrations.cloud_gcp.bt_load

import ai.chronon.integrations.cloud_gcp.GcpApiImpl
import com.google.cloud.bigquery.BigQueryOptions
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

/** ScalaTest entry point for the BigTable bulkPut (BQ → BT) load test.
  *
  * Tests are gated behind CHRONON_PERF_TEST_ENABLED=true; when absent they are
  * *canceled* (not failed) so a normal `./mill cloud_gcp.test` run is unaffected.
  *
  * Workflow:
  *   Phase 1 — BQDataGenerator creates a synthetic BQ table with the schema
  *              expected by bulkPutFromBigQuery (key_bytes, value_bytes, ds).
  *   Phase 2 — bulkPut is called, which issues a BQ EXPORT DATA job that streams
  *              the rows directly into BigTable.  Wall-clock time and throughput
  *              are logged at the end.
  *
  * Environment variables:
  *   CHRONON_PERF_TEST_ENABLED    must be "true" to run (required)
  *   GCP_PROJECT_ID               GCP project (required)
  *   GCP_BIGTABLE_INSTANCE_ID     BigTable instance (required)
  *   BT_LOAD_NUM_ROWS             row count target; default 50000000 (50M)
  *   BT_LOAD_BQ_DATASET           BQ dataset for the synthetic table; default chronon_perf_test
  *   BT_LOAD_BQ_TABLE             BQ table name; default bt_bulk_load_perf_src
  *   BT_LOAD_PARTITION            ds value written into every row and passed to bulkPut;
  *                                default 2026-06-11
  *   BT_LOAD_DESTINATION_DATASET  destinationOnlineDataSet arg to bulkPut; default bt_load_perf_test
  *   BT_LOAD_SKIP_DATA_GEN        set to "true" to skip Phase 1 (reuse existing BQ table)
  *   BT_LOAD_UPLOADER             "bigquery" (default) or "spark"; spark path submits to Dataproc
  *   BT_LOAD_DATAPROC_CLUSTER     Dataproc cluster name (required when BT_LOAD_UPLOADER=spark)
  *   BT_LOAD_JAR_URI              GCS URI of cloud_gcp assembly jar (required when BT_LOAD_UPLOADER=spark)
  *   BT_LOAD_EXTRA_JAR_URIS       comma-separated GCS URIs of extra jars (e.g. spark-bigtable connector)
  */
class BigTableBulkLoadPerfTestHarness extends AnyFlatSpec {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigTableBulkLoadPerfTestHarness])

  private val perfTestEnabled: Boolean =
    sys.env.getOrElse("CHRONON_PERF_TEST_ENABLED", "false").toLowerCase == "true"

  // --- Parameters ------------------------------------------------------------
  private val projectId: String =
    sys.env.getOrElse("GCP_PROJECT_ID", "")

  private val instanceId: String =
    sys.env.getOrElse("GCP_BIGTABLE_INSTANCE_ID", "")

  private val numRows: Long =
    sys.env.getOrElse("BT_LOAD_NUM_ROWS", "50000000").toLong

  private val bqDataset: String =
    sys.env.getOrElse("BT_LOAD_BQ_DATASET", "chronon_perf_test")

  // Suffix derived from numRows so each scale run lands in its own table (e.g. bt_bulk_load_perf_src_50M).
  private val rowSuffix: String = numRows match {
    case n if n % 1000000 == 0 => s"${n / 1000000}M"
    case n if n % 1000 == 0    => s"${n / 1000}K"
    case n                     => s"$n"
  }

  private val bqTable: String =
    sys.env.getOrElse("BT_LOAD_BQ_TABLE", s"bt_bulk_load_perf_src_$rowSuffix")

  private val partition: String =
    sys.env.getOrElse("BT_LOAD_PARTITION", "2026-06-11")

  private val destinationDataset: String =
    sys.env.getOrElse("BT_LOAD_DESTINATION_DATASET", "bt_load_perf_test")

  private val skipDataGen: Boolean =
    sys.env.getOrElse("BT_LOAD_SKIP_DATA_GEN", "false").toLowerCase == "true"

  private val uploader: String =
    sys.env.getOrElse("BT_LOAD_UPLOADER", "bigquery").toLowerCase

  private val dataprocCluster: String =
    sys.env.getOrElse("BT_LOAD_DATAPROC_CLUSTER", "")

  private val jarUri: String =
    sys.env.getOrElse("BT_LOAD_JAR_URI", "")

  private val extraJarUris: Seq[String] =
    sys.env.getOrElse("BT_LOAD_EXTRA_JAR_URIS", "").split(",").map(_.trim).filter(_.nonEmpty).toSeq

  // Lazy so clients are not created when tests are canceled.
  private val gcpLocation: String =
    sys.env.getOrElse("GCP_LOCATION", "us-central1")

  // Optional override for the target BT table name; defaults to GROUPBY_BATCH in production.
  // Set to a throwaway table name during perf testing to avoid polluting the live table.
  private val btBatchTableOverride: Option[String] =
    sys.env.get("BT_BATCH_TABLE_OVERRIDE")

  private lazy val gcpApi: GcpApiImpl =
    new GcpApiImpl(
      Map(
        "GCP_PROJECT_ID"           -> projectId,
        "GCP_BIGTABLE_INSTANCE_ID" -> instanceId,
        "GCP_LOCATION"             -> gcpLocation,
        "ENABLE_UPLOAD_CLIENTS"    -> "true"
      ) ++ btBatchTableOverride.map("BT_BATCH_TABLE_OVERRIDE" -> _)
    )

  private lazy val kvStore = gcpApi.genKvStore

  private lazy val bigQueryClient = BigQueryOptions.getDefaultInstance.getService

  // --- Phase 1: synthesize BQ source table -----------------------------------
  "BigTable bulkPut load test" should "generate BQ source data" in {
    assume(perfTestEnabled, "Set CHRONON_PERF_TEST_ENABLED=true to run perf tests")
    assume(projectId.nonEmpty, "Set GCP_PROJECT_ID")
    assume(instanceId.nonEmpty, "Set GCP_BIGTABLE_INSTANCE_ID")

    if (skipDataGen) {
      logger.info("BT_LOAD_SKIP_DATA_GEN=true — skipping BQ data generation phase")
    } else {
      logger.info(
        s"Phase 1: generating $numRows rows into $projectId.$bqDataset.$bqTable (ds='$partition')")
      BQDataGenerator.run(bigQueryClient, projectId, bqDataset, bqTable, numRows, partition)
    }
  }

  // --- Phase 2: load BQ → BigTable -------------------------------------------
  it should "run bulkPut from BQ to BigTable and report throughput" in {
    assume(perfTestEnabled, "Set CHRONON_PERF_TEST_ENABLED=true to run perf tests")
    assume(projectId.nonEmpty, "Set GCP_PROJECT_ID")
    assume(instanceId.nonEmpty, "Set GCP_BIGTABLE_INSTANCE_ID")

    val srcTable = s"$projectId.$bqDataset.$bqTable"
    logger.info(
      s"Phase 2 [$uploader]: $srcTable → BigTable instance $instanceId " +
        s"(destinationDataset=$destinationDataset, partition=$partition, numRows=$numRows)")

    val startMs = System.currentTimeMillis()

    uploader match {
      case "spark" =>
        assume(dataprocCluster.nonEmpty, "Set BT_LOAD_DATAPROC_CLUSTER for spark uploader")
        assume(jarUri.nonEmpty, "Set BT_LOAD_JAR_URI for spark uploader")
        SparkDataprocLoader.run(
          projectId    = projectId,
          region       = gcpLocation,
          clusterName  = dataprocCluster,
          jarUri       = jarUri,
          tableName    = srcTable,
          dataset      = destinationDataset,
          endDs        = partition,
          instanceId   = instanceId,
          extraJarUris = extraJarUris
        )
      case _ =>
        kvStore.create(destinationDataset)
        kvStore.bulkPut(srcTable, destinationDataset, partition)
    }

    val elapsedMs = System.currentTimeMillis() - startMs
    val elapsedSec = elapsedMs / 1000.0
    val rowsPerSec = if (elapsedSec > 0) (numRows / elapsedSec).toLong else 0L
    logger.info(
      s"bulkPut complete: uploader=$uploader, numRows=$numRows, elapsed=${elapsedSec}s, throughput=${rowsPerSec} rows/s")
  }
}
