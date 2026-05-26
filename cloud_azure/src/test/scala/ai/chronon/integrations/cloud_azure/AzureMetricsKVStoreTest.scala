package ai.chronon.integrations.cloud_azure

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse, ListRequest, ListResponse, PutRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import scala.util.Success

class AzureMetricsKVStoreTest extends AnyFlatSpec with Matchers with ScalaFutures {

  private class RecordingKVStore extends KVStore {
    var createdDatasets: Seq[String] = Seq.empty
    var getDatasets: Seq[String] = Seq.empty
    var putDatasets: Seq[String] = Seq.empty
    var listedDatasets: Seq[String] = Seq.empty

    override def create(dataset: String): Unit =
      createdDatasets = createdDatasets :+ dataset

    override def create(dataset: String, props: Map[String, Any]): Unit =
      createdDatasets = createdDatasets :+ dataset

    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
      getDatasets = getDatasets ++ requests.map(_.dataset)
      Future.successful(requests.map(request => GetResponse(request, Success(Seq.empty))))
    }

    override def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]] = {
      putDatasets = putDatasets ++ requests.map(_.dataset)
      Future.successful(requests.map(_ => true))
    }

    override def list(request: ListRequest): Future[ListResponse] = {
      listedDatasets = listedDatasets :+ request.dataset
      Future.successful(ListResponse(request, Success(Seq.empty), Map.empty))
    }

    override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {}
  }

  "AzureMetricsKVStore" should "normalize partition stats writes and reads to the configured batch dataset" in {
    val delegate = new RecordingKVStore
    val store = new AzureMetricsKVStore(delegate, "DATA_QUALITY_METRICS")

    store.create("some_output_table_BATCH")
    store.multiPut(Seq(PutRequest(Array[Byte](1), Array[Byte](2), "warehouse.table_BATCH"))).futureValue
    store.multiGet(Seq(GetRequest(Array[Byte](1), "DATA_QUALITY_METRICS"))).futureValue
    store.list(ListRequest("DATA_QUALITY_METRICS", Map.empty)).futureValue

    delegate.createdDatasets shouldBe Seq("DATA_QUALITY_METRICS_BATCH")
    delegate.putDatasets shouldBe Seq("DATA_QUALITY_METRICS_BATCH")
    delegate.getDatasets shouldBe Seq("DATA_QUALITY_METRICS_BATCH")
    delegate.listedDatasets shouldBe Seq("DATA_QUALITY_METRICS_BATCH")
  }

  it should "preserve streaming dataset normalization" in {
    val delegate = new RecordingKVStore
    val store = new AzureMetricsKVStore(delegate, "DATA_QUALITY_METRICS")

    store.multiGet(Seq(GetRequest(Array[Byte](1), "anything_STREAMING"))).futureValue
    store.multiPut(Seq(PutRequest(Array[Byte](1), Array[Byte](2), "anything_STREAMING"))).futureValue

    delegate.getDatasets shouldBe Seq("DATA_QUALITY_METRICS_STREAMING")
    delegate.putDatasets shouldBe Seq("DATA_QUALITY_METRICS_STREAMING")
  }
}
