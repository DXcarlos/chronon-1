package ai.chronon.spark.utils

import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.nio.file.Files

/**
 * Abstract base class for tests that require a Spark session with Iceberg catalog configuration.
 * Sets up a Spark session with:
 * - Iceberg Spark extensions
 * - Hadoop tables catalog
 * - Temporary warehouse directory
 * - Local master with all available cores
 */
abstract class SparkTestBase extends AnyFlatSpec with BeforeAndAfterAll {

  protected lazy val icebergWarehouse = Files.createTempDirectory("iceberg-test").toString

  /**
   * Override this method to provide additional Spark configurations.
   * These configs will be merged with the default configs, with the custom configs taking precedence.
   */
  protected def sparkConfs: Map[String, String] = Map.empty

  private[spark] lazy val DefaultSparkConfs = Map(
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.warehouse.dir" -> s"${System.getProperty("java.io.tmpdir")}/warehouse",
    "spark.sql.catalog.spark_catalog.type" -> "hadoop",
    "spark.sql.catalog.spark_catalog.warehouse" -> icebergWarehouse,
    "spark.driver.bindAddress" -> "127.0.0.1",
    "spark.ui.enabled" -> "false"
  )

  protected implicit lazy val spark: SparkSession = {
    val mergedConfig = DefaultSparkConfs ++ sparkConfs
    SparkSessionBuilder.build(
      getClass.getSimpleName,
      local = true,
      additionalConfig = Option(mergedConfig)
    )
  }

  /**
   * Creates a database in the test environment.
   * This is a test utility method moved from TableUtils.scala.
   * Delegates to the companion object method.
   *
   * @param database the database name to create
   * @return true if database was created, false if it already existed
   */
  protected def createDatabase(database: String): Boolean = {
    SparkTestBase.createDatabase(spark, database)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
}

object SparkTestBase {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Creates a database in the test environment.
   * This is a test utility method moved from TableUtils.scala.
   * Can be used by any test class, even those that don't extend SparkTestBase.
   *
   * @param spark the SparkSession to use
   * @param database the database name to create
   * @return true if database was created, false if it already existed
   */
  def createDatabase(spark: SparkSession, database: String): Boolean = {
    try {
      val command = s"CREATE DATABASE IF NOT EXISTS $database"
      logger.info(s"Creating database with command: $command")
      spark.sql(command)
      true
    } catch {
      case _: AlreadyExistsException =>
        false // 'already exists' is a swallowable exception
      case e: Exception =>
        logger.error(s"Failed to create database $database", e)
        throw e
    }
  }
}