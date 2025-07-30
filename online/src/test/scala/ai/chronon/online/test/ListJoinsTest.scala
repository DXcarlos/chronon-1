package ai.chronon.online.test

import ai.chronon.api.Constants.{ContinuationKey, MetadataDataset}
import org.mockito.ArgumentMatchers.any
import ai.chronon.online.KVStore.{ListRequest, ListResponse, ListValue}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.{Api, KVStore}
import org.mockito.Answers
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Success, Try}

class ListJoinsTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfter with Matchers {

  var api: Api = _
  var kvStore: KVStore = _
  var joinKVMap: Map[Array[Byte], Array[Byte]] = _

  implicit val ec: ExecutionContext = ExecutionContext.global

  before {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    api = mock[Api]
    // The KVStore execution context is implicitly used for
    // Future compositions in the Fetcher so provision it in
    // the mock to prevent hanging.
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    when(api.genKvStore).thenReturn(kvStore)
    joinKVMap = loadJoinKVMap()
  }

  it should "return only online joins" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    when(kvStore.list(any())).thenReturn(generateListResponse())
    val resultFuture = metadataStore.listJoins()
    val result = Await.result(resultFuture, 10.seconds)
    assert(result.size == 1)
    result.toSet shouldEqual Set("risk.user_transactions.txn_join_d")
  }

  it should "fail the call on internal issues" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    when(kvStore.list(any())).thenReturn(generateBrokenListResponse())
    an[Exception] should be thrownBy Await.result(metadataStore.listJoins(), 10.seconds)
  }

  it should "paginate list calls" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))

    val responses: Seq[ListValue] = joinKVMap.map(kv => ListValue(kv._1, kv._2)).toSeq
    // we want each of these ListValue responses to be returned in a separate ListResponse so we
    // can test pagination. So we'll wrap each of these elements in a Try[Seq[..]]
    val listResponseValues: Seq[Try[Seq[ListValue]]] = responses.map(v => Success(Seq(v)))

    // first response will have a continuation key
    val first = Future(
      ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues.head, Map(ContinuationKey -> "1")))
    // second response will not have a continuation key
    val second = Future(ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues.last, Map.empty))

    when(kvStore.list(any())).thenReturn(first, second)
    val resultFuture = metadataStore.listJoins()
    val result = Await.result(resultFuture, 10.seconds)
    assert(result.size == 1)
    result.toSet shouldEqual Set("risk.user_transactions.txn_join_d")
  }

  private def loadJoinKVMap(): Map[Array[Byte], Array[Byte]] = {
    val paths = Seq(
      // first is online = false
      "joins/user_transactions.txn_join_a",
      // this one is online = true
      "joins/user_transactions.txn_join_d"
    )

    paths.map { path =>
      val inputStream = getClass.getClassLoader.getResourceAsStream(path)
      if (inputStream == null) {
        throw new IllegalArgumentException(s"Resource not found: $path")
      }
      val src = Source.fromInputStream(inputStream)
      (path.getBytes(StandardCharsets.UTF_8), src.mkString.getBytes(StandardCharsets.UTF_8))
    }.toMap
  }

  private def generateListResponse(): Future[ListResponse] = {
    val listResponseValues: Try[Seq[ListValue]] = Success(joinKVMap.map(kv => ListValue(kv._1, kv._2)).toSeq)
    Future(ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues, Map.empty))
  }

  private def generateBrokenListResponse(): Future[ListResponse] = {
    // we expect things to fail as 'broken_value' is not a valid join
    val listResponseValues: Try[Seq[ListValue]] = Success(Seq(ListValue("some_key".getBytes, "broken_value".getBytes)))
    Future(ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues, Map.empty))
  }

}
