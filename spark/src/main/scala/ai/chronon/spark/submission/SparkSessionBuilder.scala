/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.submission

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import java.io.File
import java.util.logging.Logger
import scala.util.Properties

object SparkSessionBuilder {

  def configureLogging(): Unit = {

    // Force reconfiguration
    LoggerContext.getContext(false).close()

    val builder = ConfigurationBuilderFactory.newConfigurationBuilder()

    // Add status logger to debug logging setup
    // builder.setStatusLevel(Level.DEBUG)

    // Create console appender
    val console = builder
      .newAppender("console", "Console")
      .addAttribute("target", "SYSTEM_OUT")

    // Create pattern layout with colors
    val patternLayout = builder
      .newLayout("PatternLayout")
      .addAttribute("pattern",
                    "%cyan{%d{yyyy/MM/dd HH:mm:ss}} %highlight{%-5level} %style{%file:%line}{GREEN} - %message%n")
      .addAttribute("disableAnsi", "false")

    console.add(patternLayout)
    builder.add(console)

    // Configure root logger
    val rootLogger = builder.newRootLogger(Level.ERROR)
    rootLogger.add(builder.newAppenderRef("console"))
    builder.add(rootLogger)

    // Configure specific logger for ai.chronon
    val chrononLogger = builder.newLogger("ai.chronon", Level.INFO)
    builder.add(chrononLogger)

    // Build and apply configuration
    val config = builder.build()
    val context = LoggerContext.getContext(false)
    context.start(config)

    // Add a test log message
    val logger = LogManager.getLogger(getClass)
    logger.info("Chronon logging system initialized. Overrides spark's configuration")

  }

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val warehouseId = java.util.UUID.randomUUID().toString.takeRight(6)
  private val DefaultWarehouseDir = new File("/tmp/chronon/spark-warehouse_" + warehouseId)
  val FormatTestEnvVar: String = "format_test"

  def expandUser(path: String): String = path.replaceFirst("~", System.getProperty("user.home"))
  // we would want to share locally generated warehouse during CI testing
  def build(name: String,
            local: Boolean = false,
            hiveSupport: Boolean = true,
            localWarehouseLocation: Option[String] = None,
            additionalConfig: Option[Map[String, String]] = None,
            enforceKryoSerializer: Boolean = true): SparkSession = {

    // allow us to override the format by specifying env vars. This allows us to not have to worry about interference
    // between Spark sessions created in existing chronon tests that need the hive format and some specific tests
    // that require a format override like delta lake.
    val (formatConfigs, kryoRegistrator) = sys.env.get(FormatTestEnvVar) match {
      case Some("deltalake") =>
        logger.info("Using the delta lake table format + kryo registrators")
        val configMap = Map(
          "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
          "spark.chronon.table_write.format" -> "delta"
        )
        (configMap, "ai.chronon.spark.submission.ChrononDeltaLakeKryoRegistrator")
      case _ => (Map.empty, "ai.chronon.spark.submission.ChrononKryoRegistrator")
    }

    // tack on format configs with additional configs
    val mergedConfigs = additionalConfig.getOrElse(Map.empty) ++ formatConfigs

    val userName = Properties.userName
    val warehouseDir = localWarehouseLocation.map(expandUser).getOrElse(DefaultWarehouseDir.getAbsolutePath)
    println(s"Using warehouse dir: $warehouseDir")

    var baseBuilder = SparkSession
      .builder()
      .appName(name)

    if (hiveSupport) baseBuilder = baseBuilder.enableHiveSupport()

    baseBuilder = baseBuilder
      .config("spark.sql.session.timeZone", "UTC")
      // otherwise overwrite will delete ALL partitions, not just the ones it touches
      .config("spark.sql.sources.partitionOverwriteMode",
              "DYNAMIC"
      ) // needs to be uppercase until https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/1313 is available
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.hadoop.hive.exec.max.dynamic.partitions", 30000)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
      .config(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, false)

    // Staging queries don't benefit from the KryoSerializer and in fact may fail with buffer underflow in some cases.
    if (enforceKryoSerializer) {
      val sparkConf = new SparkConf()
      val kryoSerializerConfMap = Map(
        "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator" -> kryoRegistrator,
        "spark.kryoserializer.buffer.max" -> "2000m",
        "spark.kryo.referenceTracking" -> "false"
      ).filter { case (k, _) => !sparkConf.contains(k) }

      baseBuilder.config(kryoSerializerConfMap)
    }

    if (SPARK_VERSION.startsWith("2")) {
      // Otherwise files left from deleting the table with the same name result in test failures
      baseBuilder.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    }

    val builder = if (local) {
      logger.info(s"Building local spark session with warehouse at $warehouseDir")
      val metastoreDb = s"jdbc:derby:;databaseName=$warehouseDir/metastore_db;create=true"
      baseBuilder
        // use all threads - or the tests will be slow
        .master("local[*]")
        .config("spark.kryo.registrationRequired", s"${localWarehouseLocation.isEmpty}")
        .config("spark.local.dir", s"/tmp/$userName/${name}_$warehouseId")
        .config("spark.sql.warehouse.dir", s"$warehouseDir/data")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastoreDb)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.catalogImplementation", "hive")
    } else {
      // hive jars need to be available on classpath - no needed for local testing
      baseBuilder
    }
    mergedConfigs.foreach { config => baseBuilder = baseBuilder.config(config._1, config._2) }
    val spark = builder.getOrCreate()
    // disable log spam
    spark.sparkContext.setLogLevel("ERROR")

    Logger.getLogger("parquet.hadoop").setLevel(java.util.logging.Level.SEVERE)
    configureLogging()
    spark
  }

  def buildStreaming(local: Boolean): SparkSession = {
    val userName = Properties.userName
    val baseBuilder = SparkSession
      .builder()
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "ai.chronon.spark.submission.ChrononKryoRegistrator")
      .config("spark.kryoserializer.buffer.max", "2000m")
      .config("spark.kryo.referenceTracking", "false")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val builder = if (local) {
      baseBuilder
        // use all threads - or the tests will be slow
        .master("local[*]")
        .config("spark.local.dir", s"/tmp/$userName/chronon-spark-streaming")
        .config("spark.kryo.registrationRequired", "true")
    } else {
      baseBuilder
    }
    val spark = builder.getOrCreate()
    // disable log spam
    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("parquet.hadoop").setLevel(java.util.logging.Level.SEVERE)
    spark
  }
}
