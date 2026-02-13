package ai.chronon.flink_connectors.pubsub

import ai.chronon.online.TopicInfo
import com.google.api.gax.rpc.{NotFoundException, StatusCode}
import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.pubsub.v1.{Schema, SchemaName}
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AnyFlatSpec

class MockPubSubSchemaSerDe(topicInfo: TopicInfo, mockSchemaClient: SchemaServiceClient) extends PubSubSchemaSerDe(topicInfo) {
  override def buildPubsubSchemaClient(): SchemaServiceClient = {
    mockSchemaClient
  }
}

class PubSubSchemaSerDeSpec extends AnyFlatSpec {
  it should "fail if the schema is not found" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val statusCode = mock[StatusCode]
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenThrow(new NotFoundException(new IllegalArgumentException(), statusCode, true))

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] {
      pubSubSchemaSerDe.schema
    }
  }

  it should "fail if the schema type is not AVRO" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.PROTOCOL_BUFFER).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    assertThrows[IllegalArgumentException] {
      pubSubSchemaSerDe.schema
    }
  }

  it should "succeed if the schema is found and is of type AVRO" in {
    val topicInfo = TopicInfo("test-topic", "pubsub", Map(PubSubSchemaSerDe.ProjectKey -> "test-project", PubSubSchemaSerDe.SchemaIdKey -> "test-schema"))
    val mockedSchemaClient = mock[SchemaServiceClient]
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    val schema = Schema.newBuilder().setName("test-schema").setType(Schema.Type.AVRO).setDefinition(avroSchemaStr).build()
    when(mockedSchemaClient.getSchema(any[SchemaName]())).thenReturn(schema)

    val pubSubSchemaSerDe = new MockPubSubSchemaSerDe(topicInfo, mockedSchemaClient)
    val deSerSchema = pubSubSchemaSerDe.schema
    assert(deSerSchema != null)
  }
}
