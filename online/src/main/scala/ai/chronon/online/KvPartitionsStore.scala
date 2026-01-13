package ai.chronon.online

import ai.chronon.api.{Constants, PartitionSpec}
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Store for managing partition metadata in KV store.
  */
class KvPartitionsStore(kvStore: KVStore, dataset: String)(implicit ec: ExecutionContext) {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Store partition information for a table.
    */
  def put(table: String, kvPartitions: KvPartitions)(implicit partitionSpec: PartitionSpec): Future[Boolean] = {
    val payload = kvPartitions.serialize
    logger.info(s"Storing partitions for table '$table': $payload")
    val putRequest = PutRequest(table.getBytes(Constants.UTF8), payload.getBytes(Constants.UTF8), dataset)
    kvStore.put(putRequest)
  }

  /** Retrieve partition information for a single table.
    */
  def get(table: String)(implicit partitionSpec: PartitionSpec): Future[Option[KvPartitions]] = {
    val getRequest = GetRequest(table.getBytes(Constants.UTF8), dataset)
    kvStore.get(getRequest).map(parseResponse).recover { case e =>
      logger.warn(s"Error retrieving partitions from KV store for table $table", e)
      None
    }
  }

  /** Retrieve partition information for multiple tables.
    */
  def multiGet(tables: Seq[String])(implicit
      partitionSpec: PartitionSpec): Future[Map[String, Option[KvPartitions]]] = {
    if (tables.isEmpty) {
      Future.successful(Map.empty)
    } else {
      val distinctTables = tables.distinct
      val getRequests = distinctTables.map(t => GetRequest(t.getBytes(Constants.UTF8), dataset))

      kvStore
        .multiGet(getRequests)
        .map { responses =>
          responses.map { response =>
            val t = new String(response.request.keyBytes, Constants.UTF8)
            t -> parseResponse(response)
          }.toMap
        }
        .recover { case e =>
          logger.warn(s"Error with KV store multiGet request for tables: ${tables.mkString(", ")}", e)
          tables.map(_ -> None).toMap
        }
    }
  }

  private def parseResponse(response: KVStore.GetResponse)(implicit
      partitionSpec: PartitionSpec): Option[KvPartitions] = {
    val table = new String(response.request.keyBytes, Constants.UTF8)
    response.values match {
      case Success(timedValues) if timedValues.nonEmpty =>
        val timedValue = timedValues.maxBy(_.millis)
        val payload = new String(timedValue.bytes, Constants.UTF8)
        KvPartitions.deserialize(payload, timedValue.millis) match {
          case Some(kvPartitions) =>
            logger.debug(
              s"Retrieved ${kvPartitions.partitions.size} partitions for table $table (semanticHash: ${kvPartitions.semanticHash})")
            Some(kvPartitions)
          case None =>
            logger.warn(s"Failed to parse partitions for table $table: $payload")
            None
        }

      case Success(_) =>
        logger.debug(s"No partitions found in KV store for table $table")
        None

      case Failure(e) =>
        logger.warn(s"Error retrieving partition values from KV store for table $table", e)
        None
    }
  }
}
