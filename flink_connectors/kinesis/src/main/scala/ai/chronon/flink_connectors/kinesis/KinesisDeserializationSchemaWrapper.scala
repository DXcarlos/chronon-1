package ai.chronon.flink_connectors.kinesis

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema
import org.apache.flink.util.Collector

import java.util

/** Bridges a Collector-based [[org.apache.flink.api.common.serialization.DeserializationSchema]]
  * into the [[org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema]]
  * interface expected by [[org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer]].
  *
  * This wrapper is necessary as:
  *   - FlinkKinesisConsumer does accept a plain DeserializationSchema, but it internally wraps it
  *     in its own KinesisDeserializationSchemaWrapper which performs a runtime reflection check: if
  *     the schema's deserialize(byte[], Collector) method is not the default implementation, it
  *     throws an IllegalArgumentException. All Chronon deserialization schemas
  *     (SourceIdentityDeserializationSchema, SourceProjectionDeserializationSchema) override that
  *     method as their primary path, so they are rejected.
  *   - This wrapper sidesteps the check by implementing KinesisDeserializationSchema directly and
  *     delegating to the underlying schema's Collector-based deserialize internally.
  *
  * Why it emits Array[T] rather than a single T:
  *   - The KinesisDeserializationSchema.deserialize returns a single value per Kinesis
  *     record. Chronon schemas can emit multiple records from a single message (e.g. before / after rows,
  *      multiple rows from explode). Returning only the first record would silently drop the rest.
  *   - We collect all records emitted by the underlying schema into an Array and the source handles flatMapping
  *    it back into individual records of type T.
  */
class KinesisDeserializationSchemaWrapper[T](deserializationSchema: DeserializationSchema[T])
    extends KinesisDeserializationSchema[Array[T]] {

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    deserializationSchema.open(context)
  }

  override def deserialize(
      recordValue: Array[Byte],
      partitionKey: String,
      seqNum: String,
      approxArrivalTimestamp: Long,
      stream: String,
      shardId: String
  ): Array[T] = {
    val results = new util.ArrayList[T]()
    val collector = new Collector[T] {
      override def collect(record: T): Unit = results.add(record)
      override def close(): Unit = {}
    }

    deserializationSchema.deserialize(recordValue, collector)

    // Array[T] is Object[] at runtime for all reference types T
    results.toArray().asInstanceOf[Array[T]]
  }

  override def getProducedType: TypeInformation[Array[T]] =
    ObjectArrayTypeInfo.getInfoFor(deserializationSchema.getProducedType)
}
