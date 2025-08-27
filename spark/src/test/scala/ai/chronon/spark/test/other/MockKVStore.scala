package ai.chronon.spark.test.other

import ai.chronon.online.KVStore

import scala.collection.mutable
import scala.concurrent.Future
import scala.collection.Seq

class MockKVStore() extends KVStore with Serializable {
  val num_puts: mutable.Map[String, Int] = collection.mutable.Map[String, Int]()

  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit =
    throw new UnsupportedOperationException("Not implemented in mock")
  def create(dataset: String): Unit = {
    num_puts(dataset) = 0
  }
  def multiGet(requests: Seq[ai.chronon.online.KVStore.GetRequest])
      : scala.concurrent.Future[Seq[ai.chronon.online.KVStore.GetResponse]] =
    throw new UnsupportedOperationException("Not implemented in mock")
  def multiPut(keyValueDatasets: Seq[ai.chronon.online.KVStore.PutRequest]): scala.concurrent.Future[Seq[Boolean]] = {
    logger.info(s"Triggering multiput for ${keyValueDatasets.size}: rows")
    for (req <- keyValueDatasets if (!req.keyBytes.isEmpty && !req.valueBytes.isEmpty)) num_puts(req.dataset) += 1

    val futureResponses = keyValueDatasets.map { req =>
      if (!req.keyBytes.isEmpty && !req.valueBytes.isEmpty) Future { true }
      else Future { false }
    }
    Future.sequence(futureResponses)
  }

  def show(): Unit = {
    num_puts.foreach(x => logger.info(s"Ran ${x._2} non-empty put actions for dataset ${x._1}"))

  }
}
