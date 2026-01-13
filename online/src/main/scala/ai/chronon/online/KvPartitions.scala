package ai.chronon.online

import ai.chronon.api.{Constants, PartitionRange, PartitionSpec}
import com.google.gson.Gson

import scala.util.Try

/** Represents partition information stored in KV store, including
  * the list of partitions, a timestamp, and an optional semantic hash.
  */
case class KvPartitions(
    partitions: Seq[String],
    timestamp: Long = System.currentTimeMillis(),
    semanticHash: Option[String] = None
) {

  /** Serialize KvPartitions to JSON payload for KV store.
    * Format: {"partitions": "<collapsed_partitions>", "semantic_hash": "<hash>"}
    */
  def serialize(implicit partitionSpec: PartitionSpec): String = {
    val payloadMap = new java.util.HashMap[String, String]()
    payloadMap.put(KvPartitions.PayloadPartitionsKey, toCollapsedString)
    semanticHash.foreach(payloadMap.put(Constants.SemanticHashKey, _))
    KvPartitions.gson.toJson(payloadMap)
  }

  private def toCollapsedString(implicit partitionSpec: PartitionSpec): String = {
    PartitionRange.collapsedPrint(partitions)(partitionSpec)
  }
}

object KvPartitions {
  val PayloadPartitionsKey = "partitions"
  private[online] val gson = new Gson()

  /** Deserialize KvPartitions from a payload string retrieved from KV store.
    * Supports both new JSON format with semantic hash and legacy format (just collapsed partitions string).
    */
  def deserialize(payload: String, timestamp: Long)(implicit partitionSpec: PartitionSpec): Option[KvPartitions] = {
    // Try new JSON format first
    Try {
      val payloadMap = gson.fromJson(payload, classOf[java.util.Map[String, String]])
      val collapsedPartitions = payloadMap.get(PayloadPartitionsKey)
      val semanticHash = Option(payloadMap.get(Constants.SemanticHashKey))
      val partitions = PartitionRange.expandDates(collapsedPartitions)(partitionSpec)
      KvPartitions(partitions, timestamp, semanticHash)
    }.toOption.orElse {
      // Fall back to legacy format (just collapsed partitions string, no JSON wrapper)
      Try {
        val partitions = PartitionRange.expandDates(payload)(partitionSpec)
        KvPartitions(partitions, timestamp, None)
      }.toOption
    }
  }
}
