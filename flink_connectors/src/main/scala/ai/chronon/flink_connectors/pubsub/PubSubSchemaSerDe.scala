package ai.chronon.flink_connectors.pubsub

import ai.chronon.api.StructType
import com.google.pubsub.v1.SchemaName
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, AvroConversions, AvroSerDe, Mutation, SerDe}
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.pubsub.v1.SchemaServiceClient
import org.apache.avro.Schema

class PubSubSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import PubSubSchemaSerDe._

  protected[flink_connectors] def buildPubsubSchemaClient(): SchemaServiceClient = {
    SchemaServiceClient.create()
  }

  lazy val (avroSchemaStr, chrononSchema) = retrieveTopicSchema(topicInfo)

  private def retrieveTopicSchema(topicInfo: TopicInfo): (String, StructType) = {
    val schemaClient = buildPubsubSchemaClient()
    val projectName = topicInfo.params.getOrElse(ProjectKey, throw new IllegalArgumentException(s"$ProjectKey not set"))
    val schemaId = topicInfo.params.getOrElse(SchemaIdKey, throw new IllegalArgumentException(s"$SchemaIdKey not set"))
    val schemaName = SchemaName.of(projectName, schemaId)
    val schema =
      try {
        schemaClient.getSchema(schemaName)
      } catch {
        case e: NotFoundException =>
          throw new IllegalArgumentException(s"Schema not found - project: $projectName, schemaId: $schemaId", e)
        case e: Exception =>
          throw new IllegalStateException(s"Failed retrieving schema - project: $projectName, schemaId: $schemaId", e)
      } finally {
        schemaClient.close()
      }

    require(schema.getType == com.google.pubsub.v1.Schema.Type.AVRO,
            s"Unsupported schema type: ${schema.getType}. Only Avro is supported.")
    val avroSchema: Schema = AvroCodec.of(schema.getDefinition).schema
    val chrononSchema: StructType = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]
    (schema.getDefinition, chrononSchema)
  }

  override def schema: StructType = chrononSchema

  lazy val avroSerDe = {
    val avroSchema = AvroCodec.of(avroSchemaStr).schema
    new AvroSerDe(avroSchema)
  }

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    avroSerDe.fromBytes(bytes)
  }
}

object PubSubSchemaSerDe {
  val ProjectKey = "project"
  val SchemaIdKey = "schemaId"
}
