package ai.chronon.integrations.cloud_azure

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._
import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.online.KVStore.GetRequest
import com.azure.cosmos.CosmosAsyncClient

/**
 * Integration test for end-to-end bulkLoad against real Cosmos DB.
 * Disabled by default - requires real Cosmos DB instance.
 *
 * To run:
 * 1. Set environment variables:
 *    - COSMOS_ENDPOINT: Your Cosmos DB account endpoint
 *    - COSMOS_KEY: Your Cosmos DB master key
 *    - COSMOS_DATABASE: Database name (default: chronon)
 * 2. Run: ./mill cloud_azure.test.testOnly "ai.chronon.integrations.cloud_azure.CosmosKVStoreBulkLoadIntegrationTest"
 */
class CosmosKVStoreBulkLoadIntegrationTest extends AnyFlatSpec with Matchers {

  ignore should "bulk put batch IR data from Spark to production Cosmos DB" in {
    println("=== Starting Cosmos DB Bulk Load Integration Test with Synthetic Data ===")

    // Get connection details from environment
    val endpoint = sys.env.getOrElse("COSMOS_ENDPOINT",
      throw new IllegalArgumentException("COSMOS_ENDPOINT environment variable required"))
    val key = sys.env.getOrElse("COSMOS_KEY",
      throw new IllegalArgumentException("COSMOS_KEY environment variable required"))
    val databaseName = sys.env.getOrElse("COSMOS_DATABASE", "chronon")

    println(s"Endpoint: $endpoint")
    println(s"Database: $databaseName")

    val dataset = "TEST_INTEGRATION_BULK_LOAD"
    val batchDataset = s"${dataset}_BATCH"

    // Create KV store using the factory
    val conf = Map(
      "cosmos.endpoint" -> endpoint,
      "cosmos.key" -> key,
      "cosmos.database" -> databaseName,
      "cosmos.preferred.regions" -> "Central US"
    )

    val kvStore = CosmosKVStoreFactory.create(conf)

    // Create container with production settings (autoscale throughput)
    val createProps = Map(
      "cosmos.endpoint" -> endpoint,
      "autoscale" -> 4000  // Use autoscale with 4000 max RU/s for the test
    )

    println(s"Creating dataset: $batchDataset")
    kvStore.create(batchDataset, createProps)

    // Create Spark session
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("CosmosIntegrationTest")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
      .enableHiveSupport()
      .getOrCreate()

    try {
      import spark.implicits._

      // Create synthetic test data (500 records to test reasonable volume)
      println("Creating synthetic test data (500 records)...")
      val testData = (1 to 500).map { i =>
        val key = s"integration_test_key_$i"
        val value = s"""{"metric": $i, "name": "test_$i", "timestamp": ${System.currentTimeMillis()}}"""
        (key.getBytes("UTF-8"), value.getBytes("UTF-8"), "2024-10-15")
      }

      val batchDf = testData.toDF("key_bytes", "value_bytes", "ds")

      // Create temporary table
      val tempTable = "integration_test_bulk_upload"
      batchDf.write.mode("overwrite").saveAsTable(tempTable)

      // Prepare data for Cosmos upload
      val partition = "2024-10-15"
      val tableUtils = ai.chronon.spark.catalog.TableUtils(spark)
      val dataDf = tableUtils.sql(
        s"SELECT key_bytes, value_bytes, '$batchDataset' as dataset FROM $tempTable WHERE ds = '$partition'"
      )

      // Calculate timestamp (partition + 1 day)
      val partitionSpec = ai.chronon.api.PartitionSpec("ds", "yyyy-MM-dd", WindowUtils.Day.millis)
      val endDsPlusOne = partitionSpec.epochMillis(partition) + partitionSpec.spanMillis

      // Transform data
      println("Transforming data for Cosmos DB...")
      val ttl = 432000 // 5 days
      val transformedDf = Spark2CosmosLoader.buildTransformedDataFrame(dataDf, endDsPlusOne, ttl, spark)

      println(s"Transformed ${transformedDf.count()} records")
      transformedDf.printSchema()

      // Get container name
      val containerName = CosmosKVStore.mapDatasetToContainer(batchDataset)

      // Write to Cosmos DB
      println(s"Writing to Cosmos container: $containerName")
      Spark2CosmosLoader.writeToCosmosDB(
        transformedDf,
        endpoint,
        key,
        databaseName,
        containerName
      )

      println("Bulk write completed successfully!")

      // Verify data was written correctly by reading back a sample
      println("\nVerifying written data...")
      val sampleKeys = Seq("integration_test_key_1", "integration_test_key_250", "integration_test_key_500")
      val getRequests = sampleKeys.map(k => GetRequest(k.getBytes("UTF-8"), batchDataset, None, None))
      val getResults = Await.result(kvStore.multiGet(getRequests), 30.seconds)

      getResults.size shouldBe 3
      getResults.foreach { result =>
        result.values.isSuccess shouldBe true
        result.values.get should not be empty
        val timedValue = result.values.get.head
        timedValue.bytes.length should be > 0
        timedValue.millis shouldBe endDsPlusOne
      }

      // Verify specific values
      val value1 = new String(getResults(0).values.get.head.bytes, "UTF-8")
      val value250 = new String(getResults(1).values.get.head.bytes, "UTF-8")
      val value500 = new String(getResults(2).values.get.head.bytes, "UTF-8")

      value1 should include("\"metric\": 1")
      value250 should include("\"metric\": 250")
      value500 should include("\"metric\": 500")

      println("\n✅ All verifications passed!")
      println(s"Successfully wrote and verified 500 records to Cosmos DB dataset: $batchDataset")

    } finally {
      spark.stop()
      println("\n=== Test completed ===")
    }
  }
}
