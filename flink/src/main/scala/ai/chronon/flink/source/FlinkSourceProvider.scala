package ai.chronon.flink.source

import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.serialization.DeserializationSchema

object FlinkSourceProvider {
  def build[T](props: Map[String, String],
               deserializationSchema: DeserializationSchema[T],
               topicInfo: TopicInfo): FlinkSource[T] = {
    topicInfo.messageBus match {
      case "kafka" =>
        new KafkaFlinkSource(props, deserializationSchema, topicInfo)
      case "pubsub" =>
        loadSourceViaReflection("ai.chronon.flink_connectors.pubsub.PubSubFlinkSource",
                                props,
                                deserializationSchema,
                                topicInfo)
      case "kinesis" =>
        loadSourceViaReflection("ai.chronon.flink_connectors.kinesis.KinesisFlinkSource",
                                props,
                                deserializationSchema,
                                topicInfo)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported message bus: ${topicInfo.messageBus}")
    }
  }

  // Sources like PubSub and Kinesis are loaded via reflection so that the Flink module doesn't depend on
  // their connector modules and pull in cloud-specific deps (e.g. GCP libs when running on AWS)
  private def loadSourceViaReflection[T](className: String,
                                         props: Map[String, String],
                                         deserializationSchema: DeserializationSchema[T],
                                         topicInfo: TopicInfo): FlinkSource[T] = {
    val cl = Thread.currentThread().getContextClassLoader
    val cls = cl.loadClass(className)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props, deserializationSchema, topicInfo)
    onlineImpl.asInstanceOf[FlinkSource[T]]
  }
}
