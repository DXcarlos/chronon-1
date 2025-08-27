package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Constants.{ContinuationKey, GroupByFolder, JoinFolder, ListEntityType, ListLimit}
import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.PutRequest
import com.google.api.core.ApiFutures
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.rpc.{ServerStreamingCallable, UnaryCallable}
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.models.{Query, Row, RowMutation}
import com.google.cloud.bigtable.emulator.v2.Emulator
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{when, withSettings}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.immutable.NumericRange
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class EmulatorWrapper {
  private var emulator: Emulator = null

  /** Initializes the Bigtable emulator before a test runs. */
  def before(): Unit = {
    emulator = Emulator.createBundled
    emulator.start()
  }

  /** Stops the Bigtable emulator after a test finishes. */
  def after(): Unit = {
    emulator.stop()
    emulator = null
  }

  def getPort: Int = emulator.getPort
}

class BigTableKVStoreTest extends AnyFlatSpec with BeforeAndAfter {

  import BigTableKVStore._

  private val emulatorWrapper = new EmulatorWrapper

  private var dataClient: BigtableDataClient = _
  private var adminClient: BigtableTableAdminClient = _

  private val projectId = "test-project"
  private val instanceId = "test-instance"

  before {
    emulatorWrapper.before()

    // Configure settings to use emulator
    val dataSettings = BigtableDataSettings
      .newBuilderForEmulator(emulatorWrapper.getPort)
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()

    val adminSettings = BigtableTableAdminSettings
      .newBuilderForEmulator(emulatorWrapper.getPort)
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()

    // Create clients
    dataClient = BigtableDataClient.create(dataSettings)
    adminClient = BigtableTableAdminClient.create(adminSettings)

  }

  after {
    // Close clients to avoid gRPC channel warnings
    if (dataClient != null) {
      dataClient.close()
    }
    if (adminClient != null) {
      adminClient.close()
    }
    emulatorWrapper.after()
  }

  it should "big table creation" in {
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    val dataset = "test-table"
    kvStore.create(dataset)
    adminClient.listTables().asScala.contains(dataset) shouldBe true
    // another create should not fail
    kvStore.create(dataset)
  }

  it should "fail big table creation if missing admin client" in {
    val kvStore = new BigTableKVStoreImpl(dataClient, None)
    val dataset = "test-table"
    an[IllegalStateException] should be thrownBy kvStore.create(dataset)
  }

  // Test write & read of a simple blob dataset
  it should "blob data round trip" in {
    val dataset = "models"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    val key1 = "alice"
    val key2 = "bob"
    // some blob json payloads
    val value1 = """{"name": "alice", "age": 30}"""
    val value2 = """{"name": "bob", "age": 40}"""

    val putReq1 = PutRequest(key1.getBytes, value1.getBytes, dataset, None)
    val putReq2 = PutRequest(key2.getBytes, value2.getBytes, dataset, None)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2)), 1.second)
    putResults shouldBe Seq(true, true)

    // let's try and read these
    val getReq1 = GetRequest(key1.getBytes, dataset, None, None)
    val getReq2 = GetRequest(key2.getBytes, dataset, None, None)

    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq1)), 1.second)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq2)), 1.second)

    getResult1.size shouldBe 1
    validateBlobValueExpectedPayload(getResult1.head, value1)
    getResult2.size shouldBe 1
    validateBlobValueExpectedPayload(getResult2.head, value2)
  }

  it should "blob data updates" in {
    val dataset = "models"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    val key1 = "alice"
    // some blob json payloads
    val value = """{"name": "alice", "age": 30}"""

    val putReq = PutRequest(key1.getBytes, value.getBytes, dataset, None)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq)), 1.second)
    putResults shouldBe Seq(true)

    // let's try and read this record
    val getReq = GetRequest(key1.getBytes, dataset, None, None)

    val getResult = Await.result(kvStore.multiGet(Seq(getReq)), 1.second)

    getResult.size shouldBe 1
    validateBlobValueExpectedPayload(getResult.head, value)

    // let's now mutate this record
    val valueUpdated = """{"name": "alice", "age": 35}"""
    val putReqUpdated = PutRequest(key1.getBytes, valueUpdated.getBytes, dataset, None)

    val putResultsUpdated = Await.result(kvStore.multiPut(Seq(putReqUpdated)), 1.second)
    putResultsUpdated shouldBe Seq(true)
    // and read & verify
    val getResultUpdated = Await.result(kvStore.multiGet(Seq(getReq)), 1.second)

    getResultUpdated.size shouldBe 1
    validateBlobValueExpectedPayload(getResultUpdated.head, valueUpdated)
  }

  it should "list with pagination" in {
    val dataset = "models"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    val putReqs = (0 until 100).map { i =>
      val key = s"key-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }

    val putResults = Await.result(kvStore.multiPut(putReqs), 1.second)
    putResults.foreach(r => r shouldBe true)

    // let's try and read these
    val limit = 10
    val listReq1 = ListRequest(dataset, Map(ListLimit -> limit))

    val listResult1 = Await.result(kvStore.list(listReq1), 1.second)
    listResult1.values.isSuccess shouldBe true
    listResult1.resultProps.contains(ContinuationKey) shouldBe true
    val listValues1 = listResult1.values.get
    listValues1.size shouldBe limit

    // another call, bigger limit
    val limit2 = 1000
    val continuationKey = listResult1.resultProps(ContinuationKey)
    val listReq2 = ListRequest(dataset, Map(ListLimit -> limit2, ContinuationKey -> continuationKey))
    val listResult2 = Await.result(kvStore.list(listReq2), 1.second)
    listResult2.values.isSuccess shouldBe true
    listResult2.resultProps.contains(ContinuationKey) shouldBe false
    val listValues2 = listResult2.values.get
    listValues2.size shouldBe (putReqs.size - limit)

    // lets collect all the keys and confirm we got everything
    val allKeys = (listValues1 ++ listValues2).map(v => new String(v.keyBytes, StandardCharsets.UTF_8))
    allKeys.toSet shouldBe putReqs
      .map(r => new String(buildRowKey(r.keyBytes, r.dataset), StandardCharsets.UTF_8))
      .toSet
  }

  it should "list entity types with pagination" in {
    val dataset = "metadata"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    val putGrpByReqs = (0 until 50).map { i =>
      val key = s"$GroupByFolder/gbkey-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }

    val putJoinReqs = (0 until 50).map { i =>
      val key = s"$JoinFolder/joinkey-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }

    val putResults = Await.result(kvStore.multiPut(putGrpByReqs ++ putJoinReqs), 1.second)
    putResults.foreach(r => r shouldBe true)

    // let's try and read just the joins
    val limit = 10
    val listReq1 = ListRequest(dataset, Map(ListLimit -> limit, ListEntityType -> JoinFolder))

    val listResult1 = Await.result(kvStore.list(listReq1), 1.second)
    listResult1.values.isSuccess shouldBe true
    listResult1.resultProps.contains(ContinuationKey) shouldBe true
    val listValues1 = listResult1.values.get
    listValues1.size shouldBe limit

    // another call, bigger limit
    val limit2 = 1000
    val continuationKey = listResult1.resultProps(ContinuationKey)
    val listReq2 = ListRequest(dataset, Map(ListLimit -> limit2, ContinuationKey -> continuationKey))
    val listResult2 = Await.result(kvStore.list(listReq2), 1.second)
    listResult2.values.isSuccess shouldBe true
    listResult2.resultProps.contains(ContinuationKey) shouldBe false
    val listValues2 = listResult2.values.get
    listValues2.size shouldBe (putJoinReqs.size - limit)

    // lets collect all the keys and confirm we got everything
    val allKeys = (listValues1 ++ listValues2).map(v => new String(v.keyBytes, StandardCharsets.UTF_8))
    allKeys.toSet shouldBe putJoinReqs
      .map(r => new String(buildRowKey(r.keyBytes, r.dataset), StandardCharsets.UTF_8))
      .toSet
  }

  it should "multiput failures" in {
    val mockDataClient = mock[BigtableDataClient](withSettings().mockMaker("mock-maker-inline"))
    val mockAdminClient = mock[BigtableTableAdminClient]
    val kvStoreWithMocks = new BigTableKVStoreImpl(mockDataClient, Some(mockAdminClient))

    val failedFuture = ApiFutures.immediateFailedFuture[Void](new RuntimeException("some BT exception on read"))
    when(mockDataClient.mutateRowAsync(any[RowMutation])).thenReturn(failedFuture)

    val dataset = "models"
    val key1 = "alice"
    val key2 = "bob"
    // some blob json payloads
    val value1 = """{"name": "alice", "age": 30}"""
    val value2 = """{"name": "bob", "age": 40}"""

    val putReq1 = PutRequest(key1.getBytes, value1.getBytes, dataset, None)
    val putReq2 = PutRequest(key2.getBytes, value2.getBytes, dataset, None)

    val putResults = Await.result(kvStoreWithMocks.multiPut(Seq(putReq1, putReq2)), 1.second)
    putResults shouldBe Seq(false, false)
  }

  it should "multiget failures" in {
    val mockDataClient = mock[BigtableDataClient](withSettings().mockMaker("mock-maker-inline"))
    val mockAdminClient = mock[BigtableTableAdminClient]
    val kvStoreWithMocks = new BigTableKVStoreImpl(mockDataClient, Some(mockAdminClient))
    val serverStreamingCallable = mock[ServerStreamingCallable[Query, Row]]
    val unaryCallable = mock[UnaryCallable[Query, util.List[Row]]]

    val dataset = "models"
    val key1 = "alice"
    val key2 = "bob"
    val getReq1 = GetRequest(key1.getBytes, dataset, None, None)
    val getReq2 = GetRequest(key2.getBytes, dataset, None, None)

    when(mockDataClient.readRowsCallable()).thenReturn(serverStreamingCallable)
    when(serverStreamingCallable.all()).thenReturn(unaryCallable)
    val failedFuture =
      ApiFutures.immediateFailedFuture[util.List[Row]](new RuntimeException("some BT exception on read"))
    when(unaryCallable.futureCall(any[Query])).thenReturn(failedFuture)

    val getResult = Await.result(kvStoreWithMocks.multiGet(Seq(getReq1, getReq2)), 1.second)

    getResult.size shouldBe 2
    getResult.foreach { r =>
      r.values.isFailure shouldBe true
    }
    getResult.map(v => new String(v.request.keyBytes, StandardCharsets.UTF_8)).toSet shouldBe Set(key1, key2)
  }

  // Test write and query of a simple time series dataset
  it should "time series query_multiple days" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key".getBytes, tsRange, fakePayload)

    // query in time range: 10/05/24 00:00 to 10/10
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728518400000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  it should "multiple dataset time series query_one day" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key".getBytes, tsRange, fakePayload)

    // query in time range: 10/05/24 00:00 to 10/06/24 00:00
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728172800000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  it should "multiple dataset time series query_same day" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key".getBytes, tsRange, fakePayload)

    // query in time range: 10/05/24 00:00 to 10/05/24 22:20
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728166800000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  it should "multiple dataset time series query_days without data" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature": "123""""
    val dataStartTs = 1728000000000L
    val dataEndTs = 1729036800000L
    val tsRange = (dataStartTs until dataEndTs by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key".getBytes, tsRange, fakePayload)

    // query in time range: 10/15/24 00:00 to 10/30/24 00:00
    val queryStartsTs = 1728950400000L
    val queryEndTs = 1730246400000L
    val getRequest1 = GetRequest("my_key".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    // we expect results to only cover the time range where we have data
    val expectedTimeSeriesPoints = (queryStartsTs until dataEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTimeSeriesPoints, fakePayload)
  }

  // Test write and query of a simple time series dataset across multiple keys
  it should "handle multiple key time series query_multiple days" in {
    val dataset = "TILE_SUMMARIES"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16 and write out payloads for key1
    val fakePayload1 = """{"name": "my_key1", "my_feature": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key1".getBytes, tsRange, fakePayload1)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16 and write out payloads for key2
    val fakePayload2 = """{"name": "my_key2", "my_feature": "456""""
    writeGeneratedTimeSeriesData(kvStore, dataset, "my_key2".getBytes, tsRange, fakePayload2)

    // query in time range: 10/05/24 00:00 to 10/10
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728518400000L
    val getRequest1 = GetRequest("my_key1".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getRequest2 = GetRequest("my_key2".getBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 1.second)
    getResult.size shouldBe 2
    val expectedTimeSeriesPoints = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult.head, expectedTimeSeriesPoints, fakePayload1)
    validateTimeSeriesValueExpectedPayload(getResult.last, expectedTimeSeriesPoints, fakePayload2)
  }

  // Test repeated writes to the same streaming tile - should return the latest value
  it should "repeated streaming tile updates return latest value" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // tile timestamp - 10/04/24 00:00
    val tileTimestamp = 1728000000000L
    val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(tileTimestamp))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    // write a series of updates to the tile to mimic streaming updates
    for (i <- 0 to 10) {
      val fakePayload = s"""{"name": "my_key", "my_feature_ir": "$i""""
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(tileTimestamp + i * 1000), fakePayload)
    }

    // query in time range: 10/04/24 00:00 to 10/04/24 10:00 (we just expect the one tile though)
    val queryStartsTs = 1728000000000L
    val queryEndTs = 1728036000000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)

    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTiles = Seq(tileTimestamp)
    val expectedPayload = """{"name": "my_key", "my_feature_ir": "10"""" // latest value
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, expectedPayload)
  }

  // Test write and query of a simple tiled dataset across multiple days
  it should "streaming tiled query_multiple days" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/05/24 00:00 to 10/10
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728518400000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)

    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  // Test write and query of a simple tiled dataset across multiple days with multiple keys at once
  it should "streaming tiled query_multiple days and multiple keys" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16 for key1
    val fakePayload1 = """{"name": "my_key1", "my_feature_ir": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload1, "my_key1")

    val fakePayload2 = """{"name": "my_key2", "my_feature_ir": "456""""
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload2, "my_key2")

    // query in time range: 10/05/24 00:00 to 10/10
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728518400000L
    // read key1
    val readTileKey1 = TilingUtils.buildTileKey(dataset, "my_key1".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)

    // and key2
    val readTileKey2 = TilingUtils.buildTileKey(dataset, "my_key2".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 1.second)

    getResult.size shouldBe 2
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult.head, expectedTiles, fakePayload1)
    validateTimeSeriesValueExpectedPayload(getResult.last, expectedTiles, fakePayload2)
  }

  // handle case where the two keys have different batch end times
  it should "streaming tiled query_multiple days and multiple keys with different batch end times" in {
    val dataset1 = "GROUPBY_A_STREAMING"
    val dataset2 = "GROUPBY_B_STREAMING"
    val btTable = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(btTable)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16 for key1
    val fakePayload1 = """{"name": "my_key1", "my_feature_ir": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    generateAndWriteTimeSeriesData(kvStore, dataset1, tsRange, fakePayload1, "my_key1")

    val fakePayload2 = """{"name": "my_key2", "my_feature_ir": "456""""
    generateAndWriteTimeSeriesData(kvStore, dataset2, tsRange, fakePayload2, "my_key2")

    // read key1
    val readTileKey1 = TilingUtils.buildTileKey(dataset1, "my_key1".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)

    // and key2
    val readTileKey2 = TilingUtils.buildTileKey(dataset2, "my_key2".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    // query in time range: 10/05/24 00:00 to 10/10 for key1
    val queryStartsTs1 = 1728086400000L
    val queryEndTs1 = 1728518400000L
    val getRequest1 = GetRequest(readKeyBytes1, dataset1, Some(queryStartsTs1), Some(queryEndTs1))

    // query in time range: 10/10/24 00:00 to 10/11 for key2
    val queryStartsTs2 = 1728518400000L
    val queryEndTs2 = 1728604800000L
    val getRequest2 = GetRequest(readKeyBytes2, dataset2, Some(queryStartsTs2), Some(queryEndTs2))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 1.second)

    getResult.size shouldBe 2

    // map dataset to result
    val datasetToResult = getResult.map { r =>
      (r.request.dataset, r)
    }.toMap

    // validate two sets of tiles
    val expectedTilesKey1Tiles = (queryStartsTs1 to queryEndTs1 by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(datasetToResult(dataset1), expectedTilesKey1Tiles, fakePayload1)

    val expectedTilesKey2Tiles = (queryStartsTs2 to queryEndTs2 by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(datasetToResult(dataset2), expectedTilesKey2Tiles, fakePayload2)
  }

  // Test write and query of a simple tiled dataset for one full day
  it should "streaming tiled query_one day" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/05/24 00:00 to 10/06/24 00:00
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728172800000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)

    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  // Test write and query of a simple tiled dataset for a subset of a day
  it should "streaming tiled query_same day" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123""""
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/05/24 00:00 to 10/05/24 22:20
    val queryStartsTs = 1728086400000L
    val queryEndTs = 1728166800000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)

    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    val expectedTiles = (queryStartsTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  it should "streaming tiled query_days without data" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123""""
    val dataStartTs = 1728000000000L
    val dataEndTs = 1729036800000L
    val tsRange = (dataStartTs until dataEndTs by 1.hour.toMillis)
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/15/24 00:00 to 10/30/24 00:00
    val queryStartsTs = 1728950400000L
    val queryEndTs = 1730246400000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)

    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartsTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.second)
    getResult1.size shouldBe 1
    // we expect results to only cover the time range where we have data
    val expectedTiles = (queryStartsTs until dataEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  private def writeGeneratedTimeSeriesData(kvStore: BigTableKVStoreImpl,
                                           dataset: String,
                                           keyBytes: Array[Byte],
                                           tsRange: Seq[Long],
                                           payload: String): Unit = {
    val points = Seq.fill(tsRange.size)(payload)
    val putRequests = tsRange.zip(points).map { case (ts, point) =>
      PutRequest(keyBytes, point.getBytes, dataset, Some(ts))
    }

    val putResult = Await.result(kvStore.multiPut(putRequests), 1.second)
    putResult.length shouldBe tsRange.length
    putResult.foreach(r => r shouldBe true)
  }

  private def generateAndWriteTimeSeriesData(kvStore: BigTableKVStoreImpl,
                                             dataset: String,
                                             tsRange: NumericRange[Long],
                                             fakePayload: String,
                                             key: String): Unit = {
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, key.getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }
  }

  private def validateBlobValueExpectedPayload(response: GetResponse, expectedPayload: String): Unit = {
    for (
      tSeq <- response.values;
      tv <- tSeq
    ) {
      tSeq.length shouldBe 1
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      jsonStr shouldBe expectedPayload
    }
  }

  private def validateTimeSeriesValueExpectedPayload(response: GetResponse,
                                                     expectedTimestamps: Seq[Long],
                                                     expectedPayload: String): Unit = {
    for (tSeq <- response.values) {
      tSeq.map(_.millis).toSet shouldBe expectedTimestamps.toSet
      tSeq.map(v => new String(v.bytes, StandardCharsets.UTF_8)).foreach(v => v shouldBe expectedPayload)
      tSeq.length shouldBe expectedTimestamps.length
    }
  }
}
