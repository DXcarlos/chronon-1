package ai.chronon.integrations.cloud_azure

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse, ListRequest, ListResponse, PutRequest}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class AzureMetricsKVStore(delegate: KVStore, tableBaseName: String) extends KVStore {
  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[cloud_azure] def canonicalDataset(dataset: String): String =
    if (dataset.endsWith("_STREAMING")) s"${tableBaseName}_STREAMING"
    else s"${tableBaseName}_BATCH"

  override def create(dataset: String): Unit =
    delegate.create(canonicalDataset(dataset))

  override def create(dataset: String, props: Map[String, Any]): Unit =
    delegate.create(canonicalDataset(dataset), props)

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
    delegate.multiGet(requests.map(request => request.copy(dataset = canonicalDataset(request.dataset))))

  override def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]] =
    delegate.multiPut(requests.map(request => request.copy(dataset = canonicalDataset(request.dataset))))

  override def list(request: ListRequest): Future[ListResponse] =
    delegate.list(request.copy(dataset = canonicalDataset(request.dataset)))

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit =
    delegate.bulkPut(sourceOfflineTable, destinationOnlineDataSet, partition)
}
