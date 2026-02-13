package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.cloud_azure.CosmosKVStore.buildKeyHash
import ai.chronon.spark.catalog
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

/** Spark loader for bulk uploading data to Cosmos DB
  * Uses the Azure Cosmos DB Spark Connector for efficient bulk inserts
  */
object Spark2CosmosLoader {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    val tableName = opt[String](
      name = "table-name",
      descr = "Input table name to load into Cosmos",
      required = true
    )
    val dataset = opt[String](
      name = "dataset",
      descr = "Dataset name (e.g., GroupBy) being uploaded",
      required = true
    )
    val endDs = opt[String](
      name = "end-ds",
      descr = "End date in YYYY-MM-DD format",
      required = true
    )
    val cosmosEndpoint = opt[String](
      name = "cosmos-endpoint",
      descr = "Cosmos DB endpoint URL",
      required = true
    )
    val cosmosKey = opt[String](
      name = "cosmos-key",
      descr = "Cosmos DB master key",
      required = true
    )
    val cosmosDatabase = opt[String](
      name = "cosmos-database",
      descr = "Cosmos DB database name",
      required = true
    )
    val cosmosContainer = opt[String](
      name = "cosmos-container",
      descr = "Cosmos DB container name",
      required = true
    )
    val ttl = opt[Int](
      name = "ttl",
      descr = "TTL in seconds",
      default = Some(432000) // 5 days
    )
    verify()
  }

  def main(args: Array[String]): Unit = {
    val config = new Conf(args)

    logger.info(
      s"Starting Cosmos bulk load for table: ${config.tableName()}, dataset: ${config.dataset()}, partition: ${config.endDs()}")

    val spark = SparkSessionBuilder.build(s"Spark2CosmosLoader-${config.tableName()}")

    try {
      val tableUtils = catalog.TableUtils(spark)

      // Calculate timestamp (endDs + 1 day)
      val endDsPlusOne = tableUtils.partitionSpec.epochMillis(config.endDs()) + tableUtils.partitionSpec.spanMillis

      // Read data from offline table
      val dataDf = tableUtils.sql(s"""
        |SELECT key_bytes, value_bytes, '${config.dataset()}' as dataset
        |FROM ${config.tableName()}
        |WHERE ds = '${config.endDs()}'
        |""".stripMargin)

      logger.info(s"Read ${dataDf.count()} records from ${config.tableName()}")

      val transformedDf = buildTransformedDataFrame(
        dataDf,
        endDsPlusOne,
        config.ttl(),
        spark
      )

      writeToCosmosDB(
        transformedDf,
        config.cosmosEndpoint(),
        config.cosmosKey(),
        config.cosmosDatabase(),
        config.cosmosContainer()
      )

      logger.info(s"Successfully bulk loaded data to Cosmos container: ${config.cosmosContainer()}")

    } finally {
      spark.stop()
    }
  }

  /** Transform DataFrame with required Cosmos DB fields
    * Adds: id, keyHash, tsMillis, ttl
    */
  def buildTransformedDataFrame(
      dataDf: DataFrame,
      endDsPlusOne: Long,
      ttl: Int,
      spark: SparkSession
  ): DataFrame = {
    import spark.implicits._

    // UDF to build keyHash from key_bytes
    val buildKeyHashUDF = udf((keyBytes: Array[Byte]) => buildKeyHash(keyBytes))

    // UDF to build document ID
    val buildDocIdUDF = udf((dataset: String, keyHash: String) => s"${dataset}_${keyHash}")

    dataDf
      .withColumn("keyHash", buildKeyHashUDF(col("key_bytes")))
      .withColumn("id", buildDocIdUDF(col("dataset"), col("keyHash")))
      .withColumn("tsMillis", lit(endDsPlusOne))
      .withColumn("ttl", lit(ttl))
      // Rename columns to match Cosmos schema
      .withColumnRenamed("key_bytes", "keyBytes")
      .withColumnRenamed("value_bytes", "valueBytes")
  }

  def writeToCosmosDB(
      df: DataFrame,
      endpoint: String,
      key: String,
      database: String,
      container: String
  ): Unit = {
    logger.info(s"Triggering writes to Cosmos container: $container")

    // Use Gateway mode only for emulator (required for SSL/connection compatibility)
    // Production uses Direct mode for better performance
    val isEmulator = CosmosKVStoreConstants.isEmulator(endpoint)
    val writeBuilder = df.write
      .format("cosmos.oltp")
      .option("spark.cosmos.accountEndpoint", endpoint)
      .option("spark.cosmos.accountKey", key)
      .option("spark.cosmos.database", database)
      .option("spark.cosmos.container", container)
      .option("spark.cosmos.write.strategy", "ItemOverwrite") // Upsert semantics
      .option("spark.cosmos.write.bulk.enabled", "true") // Enable bulk mode
      .option("spark.cosmos.write.bulk.maxPendingOperations", "1000")

    val finalBuilder = if (isEmulator) {
      logger.info("Detected emulator endpoint, using Gateway mode")
      writeBuilder.option("spark.cosmos.useGatewayMode", "true")
    } else {
      logger.info("Using Direct mode for production endpoint")
      writeBuilder
    }

    finalBuilder.mode("append").save()

    logger.info("Cosmos bulk write completed")
  }
}
