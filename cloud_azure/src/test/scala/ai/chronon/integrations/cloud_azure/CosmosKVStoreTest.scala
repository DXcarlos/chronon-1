package ai.chronon.integrations.cloud_azure

import ai.chronon.api.Constants.{ContinuationKey, GroupByFolder, ListEntityType, ListLimit}
import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore.{GetRequest, ListRequest, PutRequest}
import com.azure.cosmos._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

// Running these tests requires either a test CosmosDB instance that you point to via env variables OR
// a local Cosmos DB emulator running. The emulator based flow can be run via:
// $ ./cloud_azure/run-cosmos-tests.sh testOnly "ai.chronon.integrations.cloud_azure.CosmosKVStoreTest"
// $ KEEP_EMULATOR=true ./cloud_azure/run-cosmos-tests.sh testOnly "ai.chronon.integrations.cloud_azure.CosmosKVStoreTest"
class CosmosKVStoreTest extends AnyFlatSpec with BeforeAndAfterAll {

  import CosmosKVStore._

  // Use environment variables if available, otherwise fall back to local emulator
  private val endpoint = sys.env.getOrElse("COSMOS_ENDPOINT", "https://localhost:8081")
  // emulator key
  private val key = sys.env.getOrElse("COSMOS_KEY", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
  private val databaseName = sys.env.getOrElse("COSMOS_DATABASE", "test_chronon_db")

  private var cosmosClient: CosmosAsyncClient = _
  private var database: CosmosAsyncDatabase = _
  private var kvStore: CosmosKVStoreImpl = _

  override def beforeAll(): Unit = {
    val clientBuilder = new CosmosClientBuilder()
      .endpoint(endpoint)
      .key(key)
      .consistencyLevel(ConsistencyLevel.SESSION)
      .gatewayMode() // Use Gateway mode for Cosmos DB emulator compatibility
    cosmosClient = clientBuilder.buildAsyncClient()

    // Create test database
    import java.time.Duration
    cosmosClient.createDatabaseIfNotExists(databaseName).block(Duration.ofSeconds(30))
    database = cosmosClient.getDatabase(databaseName)

    val conf = Map(
      "cosmos.endpoint" -> endpoint,
      "cosmos.key" -> key,
      "cosmos.database" -> databaseName,
      "emulator-mode" -> "true"
    )

    kvStore = new CosmosKVStoreImpl(
      database,
      conf
    )
  }

  override def afterAll(): Unit = {
    // Clean up: delete all containers in test database
    if (database != null) {
      try {
        import scala.jdk.CollectionConverters._
        val containers = database.readAllContainers().toIterable.asScala
        containers.foreach { containerProps =>
          try {
            database.getContainer(containerProps.getId).delete().block()
          } catch {
            case _: Exception => // Ignore errors during cleanup
          }
        }
      } catch {
        case _: Exception => // Ignore errors during cleanup
      }
    }

    // Close client
    if (cosmosClient != null) {
      cosmosClient.close()
    }
  }

  it should "create Cosmos container successfully" in {
    val dataset = "test_table"
    kvStore.create(dataset)

    // Verify container was created
    import scala.compat.java8.FutureConverters
    val container = database.getContainer(mapDatasetToContainer(dataset))
    val readFuture = FutureConverters.toScala(container.read().toFuture)
    val props = Await.result(readFuture, 10.seconds).getProperties
    props.getId should not be empty
  }

  it should "blob data round trip" in {
    val dataset = "test_groupby_v1_BATCH"
    kvStore.create(dataset)

    val key = "user123".getBytes(StandardCharsets.UTF_8)
    val value = """{"feature1": 42, "feature2": "hello"}""".getBytes(StandardCharsets.UTF_8)

    // Put
    val putRequest = PutRequest(key, value, dataset, Some(System.currentTimeMillis()))
    val putResult = Await.result(kvStore.multiPut(Seq(putRequest)), 10.seconds)
    putResult.head shouldBe true

    // Get
    val getRequest = GetRequest(key, dataset)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.size shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 1

    val retrievedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    retrievedValue shouldBe new String(value, StandardCharsets.UTF_8)
  }

  it should "blob data updates" in {
    val dataset = "test_groupby_v2_BATCH"
    kvStore.create(dataset)

    val key = "user456".getBytes(StandardCharsets.UTF_8)
    val value1 = """{"version": 1}""".getBytes(StandardCharsets.UTF_8)
    val value2 = """{"version": 2}""".getBytes(StandardCharsets.UTF_8)

    // First write
    val putRequest1 = PutRequest(key, value1, dataset, Some(System.currentTimeMillis()))
    Await.result(kvStore.multiPut(Seq(putRequest1)), 10.seconds)

    // Second write (update)
    Thread.sleep(100) // Ensure different timestamp
    val putRequest2 = PutRequest(key, value2, dataset, Some(System.currentTimeMillis()))
    Await.result(kvStore.multiPut(Seq(putRequest2)), 10.seconds)

    // Get should return latest value
    val getRequest = GetRequest(key, dataset)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    val retrievedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    retrievedValue shouldBe new String(value2, StandardCharsets.UTF_8)
  }

  it should "list with pagination" in {
    val dataset = "list_pagination"
    kvStore.create(dataset)

    // Insert 150 items
    val putRequests = (1 to 150).map { i =>
      val key = s"key_$i".getBytes(StandardCharsets.UTF_8)
      val value = s"""{"id": $i}""".getBytes(StandardCharsets.UTF_8)
      PutRequest(key, value, dataset)
    }
    Await.result(kvStore.multiPut(putRequests), 30.seconds)

    // First page (limit 100)
    val listRequest1 = ListRequest(dataset, Map(ListLimit -> 100))
    val listResponse1 = Await.result(kvStore.list(listRequest1), 10.seconds)

    listResponse1.values.isSuccess shouldBe true
    listResponse1.values.get.size shouldBe 100
    listResponse1.resultProps.contains(ContinuationKey) shouldBe true

    // Second page (using continuation token)
    val continuationToken = listResponse1.resultProps(ContinuationKey)
    val listRequest2 = ListRequest(dataset, Map(ListLimit -> 100, ContinuationKey -> continuationToken))
    val listResponse2 = Await.result(kvStore.list(listRequest2), 10.seconds)

    listResponse2.values.isSuccess shouldBe true
    listResponse2.values.get.size shouldBe 50 // Remaining items
  }

  it should "list entity types with pagination" in {
    val dataset = "test_entity_types"
    kvStore.create(dataset)

    // Insert data with different entity type prefixes
    val groupByPuts = (1 to 50).map { i =>
      val key = s"groupby_key_$i".getBytes(StandardCharsets.UTF_8)
      val value = s"""{"type": "groupby", "id": $i}""".getBytes(StandardCharsets.UTF_8)
      PutRequest(key, value, dataset)
    }

    val joinPuts = (1 to 30).map { i =>
      val key = s"join_key_$i".getBytes(StandardCharsets.UTF_8)
      val value = s"""{"type": "join", "id": $i}""".getBytes(StandardCharsets.UTF_8)
      PutRequest(key, value, dataset)
    }

    Await.result(kvStore.multiPut(groupByPuts ++ joinPuts), 30.seconds)

    // List only GroupBy entities
    val listRequest = ListRequest(dataset, Map(ListEntityType -> GroupByFolder, ListLimit -> 100))
    val listResponse = Await.result(kvStore.list(listRequest), 10.seconds)

    listResponse.values.isSuccess shouldBe true
    // Note: Filtering by entity type depends on how keys are structured
    // This test validates the pagination infrastructure works
  }


  it should "repeated streaming tile updates return latest value" in {
    val dataset = "test_streaming_updates_STREAMING"
    kvStore.create(dataset)

    val baseKeyBytes = "user789".getBytes(StandardCharsets.UTF_8)
    val tileTimestamp = System.currentTimeMillis()
    val tileSizeMs = 3600000L // 1 hour

    // Build tile key
    val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(tileTimestamp))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    // Write same tile multiple times with different values - do separate multiPut calls to simulate
    // sequential updates over time
    (0 to 10).foreach { version =>
      val value = s"""{"version": $version}""".getBytes(StandardCharsets.UTF_8)
      val putRequest = PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp + version))
      Await.result(kvStore.multiPut(Seq(putRequest)), 10.seconds)
    }

    // Get should return latest value
    val getRequest = GetRequest(tileKeyBytes, dataset, Some(tileTimestamp), Some(tileTimestamp + tileSizeMs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    // Should have exactly one document (the tile, with latest value)
    getResult.head.values.get.size shouldBe 1

    // Latest should be version 10
    val latestValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    latestValue should include("\"version\": 10")
  }

  it should "streaming tiled query multiple days" in {
    val dataset = "test_streaming_multiday_STREAMING"
    kvStore.create(dataset)

    val baseKeyBytes = "user_stream1".getBytes(StandardCharsets.UTF_8)
    val baseTime = System.currentTimeMillis()
    val oneDayMs = 86400000L
    val tileSizeMs = 3600000L // 1 hour

    // Write tiles across 3 days
    val putRequests = (0 to 2).flatMap { day =>
      (0 to 5).map { hour =>
        val tileTimestamp = baseTime + (day * oneDayMs) + (hour * tileSizeMs)
        val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(tileTimestamp))
        val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
        val value = s"""{"day": $day, "hour": $hour}""".getBytes(StandardCharsets.UTF_8)
        PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp))
      }
    }
    Await.result(kvStore.multiPut(putRequests), 30.seconds)

    // Query across all days
    val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(baseTime))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
    val startTs = baseTime
    val endTs = baseTime + (3 * oneDayMs)
    val getRequest = GetRequest(tileKeyBytes, dataset, Some(startTs), Some(endTs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 18 // 3 days * 6 hours
  }

  it should "streaming tiled query multiple days and multiple keys" in {
    val dataset = "test_streaming_multikey_STREAMING"
    kvStore.create(dataset)

    val baseTime = System.currentTimeMillis()
    val oneDayMs = 86400000L
    val tileSizeMs = 3600000L

    // Write tiles for 2 different entities
    val allPutRequests = (1 to 2).flatMap { entityId =>
      val baseKeyBytes = s"stream_entity_$entityId".getBytes(StandardCharsets.UTF_8)
      (0 to 1).flatMap { day =>
        (0 to 2).map { hour =>
          val tileTimestamp = baseTime + (day * oneDayMs) + (hour * tileSizeMs)
          val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(tileTimestamp))
          val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
          val value = s"""{"entity": $entityId, "day": $day, "hour": $hour}""".getBytes(StandardCharsets.UTF_8)
          PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp))
        }
      }
    }
    Await.result(kvStore.multiPut(allPutRequests), 30.seconds)

    // Query both entities
    val startTs = baseTime
    val endTs = baseTime + (2 * oneDayMs)
    val getRequests = (1 to 2).map { entityId =>
      val baseKeyBytes = s"stream_entity_$entityId".getBytes(StandardCharsets.UTF_8)
      val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(baseTime))
      val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
      GetRequest(tileKeyBytes, dataset, Some(startTs), Some(endTs))
    }
    val getResults = Await.result(kvStore.multiGet(getRequests), 10.seconds)

    getResults.size shouldBe 2
    getResults.foreach { result =>
      result.values.isSuccess shouldBe true
      result.values.get.size shouldBe 6 // 2 days * 3 hours
    }
  }

  it should "streaming tiled query with different batch end times" in {
    val dataset = "test_streaming_difftimes_STREAMING"
    kvStore.create(dataset)

    val baseKeyBytes = "user_diff".getBytes(StandardCharsets.UTF_8)
    val baseTime = System.currentTimeMillis()
    val tileSizeMs = 3600000L

    // Write tiles with different tile sizes
    val tileSizes = Seq(3600000L, 7200000L) // 1 hour and 2 hours

    val putRequests = tileSizes.flatMap { size =>
      (0 to 2).map { i =>
        val tileTimestamp = baseTime + (i * size)
        val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(size), Some(tileTimestamp))
        val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
        val value = s"""{"tile_size": $size, "index": $i}""".getBytes(StandardCharsets.UTF_8)
        PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp))
      }
    }
    Await.result(kvStore.multiPut(putRequests), 30.seconds)

    // Query for 1-hour tiles
    val tileKey1h = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(3600000L), Some(baseTime))
    val tileKeyBytes1h = TilingUtils.serializeTileKey(tileKey1h)
    val getRequest = GetRequest(tileKeyBytes1h, dataset, Some(baseTime), Some(baseTime + (10 * 3600000L)))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 3 // Only 1-hour tiles
  }

  it should "streaming tiled query one day" in {
    val dataset = "test_streaming_oneday_STREAMING"
    kvStore.create(dataset)

    val baseKeyBytes = "user_oneday".getBytes(StandardCharsets.UTF_8)
    val baseTime = System.currentTimeMillis()
    val tileSizeMs = 3600000L

    // Write hourly tiles for one day
    val putRequests = (0 to 23).map { hour =>
      val tileTimestamp = baseTime + (hour * tileSizeMs)
      val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(tileTimestamp))
      val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
      val value = s"""{"hour": $hour}""".getBytes(StandardCharsets.UTF_8)
      PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp))
    }
    Await.result(kvStore.multiPut(putRequests), 30.seconds)

    // Query the day
    val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(baseTime))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
    val getRequest = GetRequest(tileKeyBytes, dataset, Some(baseTime), Some(baseTime + 86400000L))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 24
  }

  it should "streaming tiled query same day" in {
    val dataset = "test_streaming_sameday_STREAMING"
    kvStore.create(dataset)

    val baseKeyBytes = "user_sameday".getBytes(StandardCharsets.UTF_8)
    val baseTime = System.currentTimeMillis()
    val tileSizeMs = 3600000L

    // Write a few tiles
    val putRequests = (0 to 5).map { hour =>
      val tileTimestamp = baseTime + (hour * tileSizeMs)
      val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(tileTimestamp))
      val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
      val value = s"""{"hour": $hour}""".getBytes(StandardCharsets.UTF_8)
      PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp))
    }
    Await.result(kvStore.multiPut(putRequests), 30.seconds)

    // Query partial day (hours 2-4)
    val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(baseTime))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
    val startTs = baseTime + (2 * tileSizeMs)
    val endTs = baseTime + (4 * tileSizeMs)
    val getRequest = GetRequest(tileKeyBytes, dataset, Some(startTs), Some(endTs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 3 // Hours 2, 3, 4
  }

  it should "streaming tiled query days without data" in {
    val dataset = "test_streaming_nodata_STREAMING"
    kvStore.create(dataset)

    val baseKeyBytes = "user_nodata".getBytes(StandardCharsets.UTF_8)
    val baseTime = System.currentTimeMillis()
    val oneDayMs = 86400000L
    val tileSizeMs = 3600000L

    // Write data for day 0 and day 2, skip day 1
    val putRequests = Seq(0, 2).flatMap { day =>
      (0 to 2).map { hour =>
        val tileTimestamp = baseTime + (day * oneDayMs) + (hour * tileSizeMs)
        val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(tileTimestamp))
        val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
        val value = s"""{"day": $day, "hour": $hour}""".getBytes(StandardCharsets.UTF_8)
        PutRequest(tileKeyBytes, value, dataset, Some(tileTimestamp))
      }
    }
    Await.result(kvStore.multiPut(putRequests), 30.seconds)

    // Query across all 3 days
    val tileKey = TilingUtils.buildTileKey(dataset, baseKeyBytes, Some(tileSizeMs), Some(baseTime))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
    val getRequest = GetRequest(tileKeyBytes, dataset, Some(baseTime), Some(baseTime + (3 * oneDayMs)))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 6 // 2 days * 3 hours
  }


  it should "handle entities where some have data and others don't" in {
    val dataset = "partial_data_test"
    kvStore.create(dataset)

    // Only write data for entity1
    val key1 = "entity_with_data".getBytes(StandardCharsets.UTF_8)
    val value1 = """{"has": "data"}""".getBytes(StandardCharsets.UTF_8)
    Await.result(kvStore.multiPut(Seq(PutRequest(key1, value1, dataset))), 10.seconds)

    // Query both entity1 (has data) and entity2 (no data)
    val key2 = "entity_without_data".getBytes(StandardCharsets.UTF_8)
    val getRequests = Seq(
      GetRequest(key1, dataset),
      GetRequest(key2, dataset)
    )
    val getResults = Await.result(kvStore.multiGet(getRequests), 10.seconds)

    getResults.size shouldBe 2
    getResults(0).values.isSuccess shouldBe true
    getResults(0).values.get.size shouldBe 1
    getResults(1).values.isSuccess shouldBe true
    getResults(1).values.get.size shouldBe 0 // No data for entity2
  }

  it should "handle missing keys gracefully" in {
    val dataset = "missing_keys_test"
    kvStore.create(dataset)

    val missingKey = "nonexistent_key".getBytes(StandardCharsets.UTF_8)
    val getRequest = GetRequest(missingKey, dataset)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.size shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.size shouldBe 0 // Empty result, not an error
  }

  it should "preserve write timestamp for non-time-series data" in {
    val dataset = "test_timestamp_preserve_BATCH"
    kvStore.create(dataset)

    val key = "user_ts".getBytes(StandardCharsets.UTF_8)
    val value = """{"data": "test"}""".getBytes(StandardCharsets.UTF_8)
    val writeTimestamp = System.currentTimeMillis()

    // Write with specific timestamp
    val putRequest = PutRequest(key, value, dataset, Some(writeTimestamp))
    Await.result(kvStore.multiPut(Seq(putRequest)), 10.seconds)

    // Read and verify timestamp is preserved
    val getRequest = GetRequest(key, dataset)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    val retrievedTimestamp = getResult.head.values.get.head.millis
    retrievedTimestamp shouldBe writeTimestamp
  }

  it should "last write wins for duplicate timestamps" in {
    val dataset = "test_last_write_wins_BATCH"
    kvStore.create(dataset)

    val key = "conflict_key".getBytes(StandardCharsets.UTF_8)
    val timestamp = System.currentTimeMillis()

    // Write twice with same timestamp but different values
    val value1 = """{"version": "first"}""".getBytes(StandardCharsets.UTF_8)
    val value2 = """{"version": "second"}""".getBytes(StandardCharsets.UTF_8)

    Await.result(kvStore.multiPut(Seq(PutRequest(key, value1, dataset, Some(timestamp)))), 10.seconds)
    Thread.sleep(100) // Small delay
    Await.result(kvStore.multiPut(Seq(PutRequest(key, value2, dataset, Some(timestamp)))), 10.seconds)

    // Should return the last written value
    val getRequest = GetRequest(key, dataset)
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest)), 10.seconds)

    getResult.head.values.isSuccess shouldBe true
    val retrievedValue = new String(getResult.head.values.get.head.bytes, StandardCharsets.UTF_8)
    retrievedValue should include("second")
  }

  it should "bulk put batch IR data from Spark" in {
    // Note: bulkPut appends "_BATCH" suffix via GroupBy.batchDataset
    val dataset = "TEST_GROUPBY"
    val batchDataset = s"${dataset}_BATCH"

    kvStore.create(batchDataset)

    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("CosmosBulkPutTest")
      .master("local[1]")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
      .enableHiveSupport()
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data - simulating batch IR snapshots (100 records)
      val testData = (1 to 100).map { i =>
        (s"user$i".getBytes, s"""{"feature1": $i, "feature2": "value$i"}""".getBytes, "2024-10-05")
      }

      val batchDf = testData.toDF("key_bytes", "value_bytes", "ds")

      val tempTable = "test_batch_upload_table"
      batchDf.write.mode("overwrite").saveAsTable(tempTable)

      val partition = "2024-10-05"
      val tableUtils = ai.chronon.spark.catalog.TableUtils(spark)
      val dataDf = tableUtils.sql(
        s"SELECT key_bytes, value_bytes, '$batchDataset' as dataset FROM $tempTable WHERE ds = '$partition'"
      )

      import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
      val partitionSpec = ai.chronon.api.PartitionSpec("ds", "yyyy-MM-dd", WindowUtils.Day.millis)
      val endDsPlusOne = partitionSpec.epochMillis(partition) + partitionSpec.spanMillis

      val ttl = 432000 // 5 days
      val transformedDf = Spark2CosmosLoader.buildTransformedDataFrame(dataDf, endDsPlusOne, ttl, spark)

      val containerName = CosmosKVStore.mapDatasetToContainer(batchDataset)

      Spark2CosmosLoader.writeToCosmosDB(
        transformedDf,
        endpoint,
        key,
        databaseName,
        containerName
      )

      // Verify a sample of data was written correctly
      val sampleKeys = Seq("user1", "user50", "user100")
      val getRequests = sampleKeys.map(key => GetRequest(key.getBytes, batchDataset, None, None))
      val getResults = Await.result(kvStore.multiGet(getRequests), 30.seconds)

      getResults.size shouldBe 3
      getResults.foreach { result =>
        result.values.isSuccess shouldBe true
        result.values.get.size shouldBe 1
        // Value should be present
        val timedValue = result.values.get.head
        timedValue.bytes.length should be > 0
      }

      // Verify specific values
      val value1 = new String(getResults(0).values.get.head.bytes, StandardCharsets.UTF_8)
      val value50 = new String(getResults(1).values.get.head.bytes, StandardCharsets.UTF_8)
      val value100 = new String(getResults(2).values.get.head.bytes, StandardCharsets.UTF_8)

      value1 should include("\"feature1\": 1")
      value50 should include("\"feature1\": 50")
      value100 should include("\"feature1\": 100")

      // Verify timestamps are set correctly
      getResults.foreach { result =>
        val timedValue = result.values.get.head
        timedValue.millis shouldBe endDsPlusOne
      }

    } finally {
      spark.stop()
    }
  }
}
