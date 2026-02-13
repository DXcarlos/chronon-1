package ai.chronon.integrations.cloud_azure

import ai.chronon.integrations.cloud_azure.CosmosKVStoreConstants._
import java.security.MessageDigest

object CosmosKVStore {

  // Table type enumeration
  sealed trait TableType
  case object BatchTable extends TableType
  case object StreamingTable extends TableType
  case object MetadataTable extends TableType

  /** Determine the table type based on dataset naming convention
    * - Ends with "_BATCH" -> BatchTable
    * - Ends with "_STREAMING" -> StreamingTable
    * - Default -> MetadataTable
    */
  def getTableType(dataset: String): TableType = {
    dataset match {
      case d if d.endsWith("_BATCH")     => BatchTable
      case d if d.endsWith("_STREAMING") => StreamingTable
      case _                             => MetadataTable
    }
  }

  /** Map dataset name to container name
    * - GROUPBY_BATCH: shared container for all *_BATCH datasets
    * - GROUPBY_STREAMING: shared container for all *_STREAMING datasets
    * - Others: 1:1 mapping (container name = dataset name)
    * Note: container names are lowercased as Cosmos DB seems to occasionally hit issues
    * creating tables with uppercase letters.
    */
  def mapDatasetToContainer(dataset: String): String = {
    val containerName = getTableType(dataset) match {
      case BatchTable     => GroupByBatchContainer
      case StreamingTable => GroupByStreamingContainer
      case _              => dataset // 1:1 mapping for metadata
    }
    containerName.toLowerCase
  }

  /** Build a partition key hash from key bytes using SHA-256
    * This enables us to ensure we don't hit partition key size limits as well as
    * distributing keys more evenly across partitions.
    */
  def buildKeyHash(keyBytes: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(keyBytes)
    hashBytes.map("%02x".format(_)).mkString.take(16)
  }

  def buildBatchDocumentId(dataset: String, keyHash: String): String = {
    s"${dataset}_$keyHash"
  }

  def buildTimeSeriesDocumentId(dataset: String, keyHash: String, tsMillis: Long, tileSizeMs: Long): String = {
    s"${dataset}_${keyHash}_${tsMillis}_${tileSizeMs}"
  }

  def roundToDay(tsMillis: Long): Long = {
    val millisPerDay = 86400000L // 24 * 60 * 60 * 1000
    tsMillis - (tsMillis % millisPerDay)
  }
}
