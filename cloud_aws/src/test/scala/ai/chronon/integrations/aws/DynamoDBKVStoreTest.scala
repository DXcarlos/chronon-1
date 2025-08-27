package ai.chronon.integrations.aws

import ai.chronon.api.Constants.{ContinuationKey, ListLimit}
import ai.chronon.online.KVStore._
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.collection.Seq
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object DDBTestUtils {

  // different types of tables to store
  case class Model(modelId: String, modelName: String, online: Boolean)
  case class TimeSeries(joinName: String, featureName: String, tileTs: Long, metric: String, summary: Array[Double])

}

class DynamoDBKVStoreTest extends AnyFlatSpec with BeforeAndAfterAll {

  import DDBTestUtils._
  import DynamoDBKVStoreConstants._

  implicit val modelEncoder: Encoder[Model] = deriveEncoder[Model]
  implicit val modelDecoder: Decoder[Model] = deriveDecoder[Model]
  implicit val tsEncoder: Encoder[TimeSeries] = deriveEncoder[TimeSeries]
  implicit val tsDecoder: Decoder[TimeSeries] = deriveDecoder[TimeSeries]

  var server: DynamoDBProxyServer = _
  var client: DynamoDbClient = _
  var kvStoreImpl: DynamoDBKVStoreImpl = _

  def modelKeyEncoder(model: Model): Array[Byte] = {
    model.modelId.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
  }

  def modelValueEncoder(model: Model): Array[Byte] = {
    model.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
  }

  def timeSeriesKeyEncoder(timeSeries: TimeSeries): Array[Byte] = {
    (timeSeries.joinName, timeSeries.featureName).asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
  }

  def timeSeriesValueEncoder(series: TimeSeries): Array[Byte] = {
    series.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
  }

  override def beforeAll(): Unit = {
    // Start the local DynamoDB instance
    server = ServerRunner.createServerFromCommandLineArgs(Array("-inMemory", "-port", "8000"))
    server.start()

    // Create the DynamoDbClient
    client = DynamoDbClient
      .builder()
      .endpointOverride(URI.create("http://localhost:8000"))
      .region(Region.US_WEST_2)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("dummy", "dummy")
        ))
      .build()
  }

  override def afterAll(): Unit = {
//    client.close()
//    server.stop()
  }

  // Test creation of a table with primary keys only (e.g. model)
  it should "create p key only table" in {
    val dataset = "models"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)
    // Verify that the table exists
    val tables = client.listTables().tableNames()
    tables.contains(dataset) shouldBe true
    // try another create for an existing table, should not fail
    kvStore.create(dataset, props)
  }

  // Test creation of a table with primary + sort keys (e.g. time series)
  it should "create p key and sort key table" in {
    val dataset = "timeseries"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)
    // Verify that the table exists
    val tables = client.listTables().tableNames()
    tables.contains(dataset) shouldBe true
    // here too, another create should not fail
    kvStore.create(dataset, props)
  }

  // Test table scan with pagination
  it should "table scan with pagination" in {
    val dataset = "models"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val putReqs = (0 until 100).map { i =>
      val model = Model(s"my_model_$i", s"test model $i", online = true)
      buildModelPutRequest(model, dataset)
    }

    val putResults = Await.result(kvStore.multiPut(putReqs), 1.minute)
    putResults.length shouldBe putReqs.length
    putResults.foreach(r => r shouldBe true)

    // call list - first call is only for 10 elements
    val listReq1 = ListRequest(dataset, Map(ListLimit -> 10))
    val listResults1 = Await.result(kvStore.list(listReq1), 1.minute)
    listResults1.resultProps.contains(ContinuationKey) shouldBe true
    validateExpectedListResponse(listResults1.values, 10)

    // call list - with continuation key
    val listReq2 =
      ListRequest(dataset, Map(ListLimit -> 100, ContinuationKey -> listResults1.resultProps(ContinuationKey)))
    val listResults2 = Await.result(kvStore.list(listReq2), 1.minute)
    listResults2.resultProps.contains(ContinuationKey) shouldBe false
    validateExpectedListResponse(listResults2.values, 100)
  }

  // Test write & read of a simple blob dataset
  it should "blob data round trip" in {
    val dataset = "models"
    val props = Map(isTimedSorted -> "false")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    val model1 = Model("my_model_1", "test model 1", online = true)
    val model2 = Model("my_model_2", "test model 2", online = true)
    val model3 = Model("my_model_3", "test model 3", online = false)

    val putReq1 = buildModelPutRequest(model1, dataset)
    val putReq2 = buildModelPutRequest(model2, dataset)
    val putReq3 = buildModelPutRequest(model3, dataset)

    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2, putReq3)), 1.minute)
    putResults shouldBe Seq(true, true, true)

    // let's try and read these
    val getReq1 = buildModelGetRequest(model1, dataset)
    val getReq2 = buildModelGetRequest(model2, dataset)
    val getReq3 = buildModelGetRequest(model3, dataset)

    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq1)), 1.minute)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq2)), 1.minute)
    val getResult3 = Await.result(kvStore.multiGet(Seq(getReq3)), 1.minute)

    validateExpectedModelResponse(model1, getResult1)
    validateExpectedModelResponse(model2, getResult2)
    validateExpectedModelResponse(model3, getResult3)
  }

  // Test write and query of a time series dataset
  it should "time series query" in {
    val dataset = "timeseries"
    val props = Map(isTimedSorted -> "true")
    val kvStore = new DynamoDBKVStoreImpl(client)
    kvStore.create(dataset, props)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16
    val tsRange = (1728000000000L until 1729036800000L by 1.hour.toMillis)
    val points = tsRange.map(ts => TimeSeries("my_join", "my_feature_1", ts, "my_metric", Array(1.0, 2.0, 3.0)))

    // write to the kv store and confirm the writes were successful
    val putRequests = points.map(p => buildTSPutRequest(p, dataset))
    val putResult = Await.result(kvStore.multiPut(putRequests), 1.minute)
    putResult.length shouldBe tsRange.length
    putResult.foreach(r => r shouldBe true)

    // query in time range: 10/05/24 00:00 to 10/10
    val getRequest1 = buildTSGetRequest(points.head, dataset, 1728086400000L, 1728518400000L)
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 1.minute)
    validateExpectedTimeSeriesResponse(points.head, 1728086400000L, 1728518400000L, getResult1)
  }

  private def buildModelPutRequest(model: Model, dataset: String): PutRequest = {
    val keyBytes = modelKeyEncoder(model)
    val valueBytes = modelValueEncoder(model)
    PutRequest(keyBytes, valueBytes, dataset, None)
  }

  private def buildModelGetRequest(model: Model, dataset: String): GetRequest = {
    val keyBytes = modelKeyEncoder(model)
    GetRequest(keyBytes, dataset, None, None)
  }

  private def buildTSPutRequest(timeSeries: TimeSeries, dataset: String): PutRequest = {
    val keyBytes = timeSeriesKeyEncoder(timeSeries)
    val valueBytes = timeSeriesValueEncoder(timeSeries)
    PutRequest(keyBytes, valueBytes, dataset, Some(timeSeries.tileTs))
  }

  private def buildTSGetRequest(timeSeries: TimeSeries, dataset: String, startTs: Long, endTs: Long): GetRequest = {
    val keyBytes = timeSeriesKeyEncoder(timeSeries)
    GetRequest(keyBytes, dataset, Some(startTs), Some(endTs))
  }

  private def validateExpectedModelResponse(expectedModel: Model, response: Seq[GetResponse]): Unit = {
    response.length shouldBe 1
    for (
      tSeq <- response.head.values;
      tv <- tSeq
    ) {
      tSeq.length shouldBe 1
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      val returnedModel = decode[Model](jsonStr)
      returnedModel match {
        case Right(model) =>
          model shouldBe expectedModel
        case Left(error) =>
          fail(s"Decoding failed with error: $error")
      }
    }
  }

  private def validateExpectedListResponse(response: Try[Seq[ListValue]], maxElements: Int): Unit = {
    response match {
      case Success(mSeq) =>
        mSeq.length <= maxElements shouldBe true
        mSeq.foreach { modelKV =>
          val jsonStr = new String(modelKV.valueBytes, StandardCharsets.UTF_8)
          val returnedModel = decode[Model](jsonStr)
          val returnedKeyJsonStr = new String(modelKV.keyBytes, StandardCharsets.UTF_8)
          val returnedKey = decode[String](returnedKeyJsonStr)
          returnedModel.isRight shouldBe true
          returnedKey.isRight shouldBe true
          returnedModel.right.get.modelId shouldBe returnedKey.right.get
        }
      case Failure(exception) =>
        fail(s"List response failed with exception: $exception")
    }
  }

  private def validateExpectedTimeSeriesResponse(expectedTSBase: TimeSeries,
                                                 startTs: Long,
                                                 endTs: Long,
                                                 response: Seq[GetResponse]): Unit = {
    response.length shouldBe 1
    val expectedTimeSeriesPoints = (startTs to endTs by 1.hour.toMillis).map(ts => expectedTSBase.copy(tileTs = ts))
    for (
      tSeq <- response.head.values;
      (tv, ev) <- tSeq.zip(expectedTimeSeriesPoints)
    ) {
      tSeq.length shouldBe expectedTimeSeriesPoints.length
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      val returnedTimeSeries = decode[TimeSeries](jsonStr)
      returnedTimeSeries match {
        case Right(ts) =>
          // we just match the join name and timestamps for simplicity
          ts.tileTs shouldBe ev.tileTs
          ts.joinName shouldBe ev.joinName
        case Left(error) =>
          fail(s"Decoding failed with error: $error")
      }
    }
  }
}
