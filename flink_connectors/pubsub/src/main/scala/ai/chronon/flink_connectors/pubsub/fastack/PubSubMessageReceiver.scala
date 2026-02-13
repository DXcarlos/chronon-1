package ai.chronon.flink_connectors.pubsub.fastack

import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver}
import com.google.pubsub.v1.PubsubMessage
import org.apache.flink.util.Collector

// Callback that is invoked by the PubSub subscriber whenever a new message is pulled from Pub/Sub.
class PubSubMessageReceiver[OUT](deserializationSchema: PubSubDeserializationSchema[OUT]) extends MessageReceiver {

  private var collector: Collector[OUT] = _
  // The receive message can be called by multiple PubSub threads - the downstream deser schema and other Flink ops
  // expect only one thread (the operator thread) to call the collector, so we need to synchronize access
  private val lockObject = new Object()

  def open(collector: Collector[OUT]): Unit = {
    require(collector != null, "Collector cannot be null")
    this.collector = collector
  }

  override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
    require(collector != null, "Collector must be initialized before receiving messages")
    if (message.getData != null && message.getData.toByteArray != null) {
      lockObject.synchronized {
        deserializationSchema.deserialize(message, collector)
      }
      consumer.ack()
    }
  }
}
