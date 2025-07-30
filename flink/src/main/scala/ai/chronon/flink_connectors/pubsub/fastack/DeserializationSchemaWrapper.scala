package ai.chronon.flink_connectors.pubsub.fastack

import com.google.pubsub.v1.PubsubMessage
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema
import org.apache.flink.util.Collector

// Thin wrapper around a Flink DeserializationSchema to allow it to work with PubSub message wrapper objects
class DeserializationSchemaWrapper[T](deserializationSchema: DeserializationSchema[T])
    extends PubSubDeserializationSchema[T] {

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    deserializationSchema.open(context)
  }

  override def isEndOfStream(nextElement: T): Boolean = deserializationSchema.isEndOfStream(nextElement)

  override def deserialize(message: PubsubMessage): T = {
    throw new UnsupportedOperationException(
      "Use the deserialize(message: PubSubMessage, out: Collector[T]) method instead.");
  }

  override def deserialize(message: PubsubMessage, out: Collector[T]): Unit = {
    deserializationSchema.deserialize(message.getData.toByteArray, out)
  }

  override def getProducedType: TypeInformation[T] = deserializationSchema.getProducedType
}
