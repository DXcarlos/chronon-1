package ai.chronon.flink.deser

import ai.chronon.flink.deser.CustomSchemaSerDe.ProviderClass
import ai.chronon.flink.deser.SchemaRegistrySerDe.RegistryHostKey
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.SerDe

// Provider for the relevant SerDe implementation for Flink.
object FlinkSerDeProvider {
  private val SerDeKey = "serde"

  private val PubsubSchemaSerDeClass = "ai.chronon.flink_connectors.pubsub.PubSubSchemaSerDe"
  private val SchemaRegistrySerDeClass = "ai.chronon.flink.deser.SchemaRegistrySerDe"

  // If the user explicitly provides serde, we use that, else we fallback to inferring based on the topic params to cover
  // legacy clients that don't set serde atm.
  def build(topicInfo: TopicInfo): SerDe = {
    val serDe = topicInfo.params.get(SerDeKey)
    val className = serDe match {
      case Some("custom") =>
        topicInfo.params.getOrElse(ProviderClass, throw new IllegalArgumentException(s"$ProviderClass not set"))
      case Some("schema_registry") => SchemaRegistrySerDeClass
      case Some("pubsub_schema")   => PubsubSchemaSerDeClass
      // Some users might not set the serde explicitly but are using the schema registry host key, so we fallback to that.
      case None if topicInfo.params.contains(RegistryHostKey) => SchemaRegistrySerDeClass
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported SerDe type: $serDe. Supported values are 'custom', 'schema_registry' or 'pubsub_schema'.")
    }
    loadSchemaSerDe(className, topicInfo)
  }

  private def loadSchemaSerDe(className: String, topicInfo: TopicInfo): SerDe = {
    val cl = Thread.currentThread().getContextClassLoader // Use Flink's classloader
    val cls = cl.loadClass(className)
    val constructor = cls.getConstructors.apply(0)
    val schemaSerDe = constructor.newInstance(topicInfo)
    schemaSerDe.asInstanceOf[SerDe]
  }
}
