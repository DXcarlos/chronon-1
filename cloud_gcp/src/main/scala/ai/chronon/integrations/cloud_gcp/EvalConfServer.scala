package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.local.EvalConf
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.orchestration.{StagingQueryEvalResult, GroupByEvalResult, JoinEvalResult}
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.{Join, GroupBy, StagingQuery}
import org.apache.spark.sql.SparkSession
import py4j.GatewayServer
import scala.util.{Try, Success, Failure}

/** Wrapper for EvalConf that creates Spark session dynamically based on config */
class DynamicEvalConf(rootDir: String) {

  private def extractGcpProject(configPath: String): Option[String] = {
    Try {
      if (configPath.contains("/joins/")) {
        val join = ThriftJsonCodec.fromJsonFile[Join](configPath, check = false)
        Option(join.metaData.executionInfo.conf)
          .flatMap(conf => Option(conf.common))
          .flatMap(common => Option(common.get("spark.sql.catalog.spark_catalog.gcp_project")))
      } else if (configPath.contains("/group_bys/")) {
        val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](configPath, check = false)
        Option(groupBy.metaData.executionInfo.conf)
          .flatMap(conf => Option(conf.common))
          .flatMap(common => Option(common.get("spark.sql.catalog.spark_catalog.gcp_project")))
      } else if (configPath.contains("/staging_queries/")) {
        val stagingQuery = ThriftJsonCodec.fromJsonFile[StagingQuery](configPath, check = false)
        Option(stagingQuery.metaData.executionInfo.conf)
          .flatMap(conf => Option(conf.common))
          .flatMap(common => Option(common.get("spark.sql.catalog.spark_catalog.gcp_project")))
      } else {
        None
      }
    }.toOption.flatten
  }

  private def createSparkSessionForProject(gcpProject: String): (SparkSession, TableUtils, BigQuerySchemaUtils) = {
    val bigQueryConfig = Map(
      "spark.sql.catalogImplementation" -> "in-memory",
      "spark.chronon.partition.column" -> "ds",
      // BigQuery connector configuration
      "viewsEnabled" -> "true",
      "materializationProject" -> gcpProject,
      "materializationDataset" -> "tmp",
      "spark.bigquery.parallelism" -> "4"
    )
    val spark = SparkSessionBuilder.build(
      s"EvalConf-$gcpProject",
      local = true,
      additionalConfig = Some(bigQueryConfig)
    )
    val tableUtils = TableUtils(spark)
    val schemaUtils = new BigQuerySchemaUtils()(spark)
    (spark, tableUtils, schemaUtils)
  }

  def evalJoinConf(rootConfPath: String): JoinEvalResult = {
    val gcpProject = extractGcpProject(rootConfPath).getOrElse {
      throw new RuntimeException(s"Could not extract GCP project from config: $rootConfPath")
    }

    val (spark, tableUtils, schemaUtils) = createSparkSessionForProject(gcpProject)
    try {
      val evalConf = new EvalConf(rootDir, schemaUtils)(tableUtils)
      evalConf.evalJoinConf(rootConfPath)
    } finally {
      spark.stop()
    }
  }

  def evalGroupByConf(rootConfPath: String): GroupByEvalResult = {
    val gcpProject = extractGcpProject(rootConfPath).getOrElse {
      throw new RuntimeException(s"Could not extract GCP project from config: $rootConfPath")
    }

    val (spark, tableUtils, schemaUtils) = createSparkSessionForProject(gcpProject)
    try {
      val evalConf = new EvalConf(rootDir, schemaUtils)(tableUtils)
      evalConf.evalGroupByConf(rootConfPath)
    } finally {
      spark.stop()
    }
  }

  def evalStagingQueryConf(rootConfPath: String): StagingQueryEvalResult = {
    val gcpProject = extractGcpProject(rootConfPath).getOrElse {
      throw new RuntimeException(s"Could not extract GCP project from config: $rootConfPath")
    }

    val (spark, tableUtils, schemaUtils) = createSparkSessionForProject(gcpProject)
    try {
      val evalConf = new EvalConf(rootDir, schemaUtils)(tableUtils)
      evalConf.evalStagingQueryConf(rootConfPath)
    } finally {
      spark.stop()
    }
  }

  def reset(): Unit = {
    // Nothing to reset since we create fresh sessions each time
  }
}

/** Gateway server for EvalConf with BigQuery support
  */
object EvalConfServer extends App {

  // Default to current directory if CHRONON_ROOT not set
  val rootDir = sys.env.getOrElse("CHRONON_ROOT", ".")

  val evalConf = new DynamicEvalConf(rootDir)
  val gateway = new GatewayServer(evalConf)

  gateway.start()
}
