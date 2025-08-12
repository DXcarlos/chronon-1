package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.local.EvalConf
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import py4j.GatewayServer

/** Gateway server for EvalConf with BigQuery support
  */
object EvalConfServer extends App {

  // Default to current directory if CHRONON_ROOT not set
  val rootDir = sys.env.getOrElse("CHRONON_ROOT", ".")

  // Create a local spark session for evaluation with BigQuery support
  val gcpProject = sys.env.getOrElse("GCP_PROJECT_ID", "canary-443022")
  val bigQueryConfig = Map(
    "spark.sql.catalogImplementation" -> "in-memory",
    "spark.chronon.partition.column" -> "ds",
    // BigQuery connector configuration
    "viewsEnabled" -> "true",
    "materializationProject" -> gcpProject,
    "materializationDataset" -> "tmp",
    "spark.bigquery.parallelism" -> "4"
  )
  val spark: SparkSession = SparkSessionBuilder.build(
    "EvalConfServer",
    local = true,
    additionalConfig = Some(bigQueryConfig)
  )
  implicit val tableUtils: TableUtils = TableUtils(spark)

  // Use the BigQuery implementation of SchemaUtils
  val schemaUtils = new BigQuerySchemaUtils()(spark)

  val evalConf = new EvalConf(rootDir, schemaUtils)
  val gateway = new GatewayServer(evalConf)

  gateway.start()
  println(s"EvalConf gateway server with BigQuery support started for directory: $rootDir")
  println(evalConf.tablesToGroupBy)
  println(evalConf.tablesToJoin)
  println(evalConf.tablesToStagingQuery)
}
