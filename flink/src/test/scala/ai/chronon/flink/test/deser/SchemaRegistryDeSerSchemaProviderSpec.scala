package ai.chronon.flink.test.deser

import ai.chronon.api.{IntType, StringType, StructField}
import ai.chronon.flink.deser.SchemaRegistrySerDe
import ai.chronon.flink.deser.SchemaRegistrySerDe.{Proto3DefaultAsNullKey, RegistryHostKey, SchemaRegistryWireFormat}
import ai.chronon.online.TopicInfo
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.avro.{AvroSchema, AvroSchemaProvider}
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema, ProtobufSchemaProvider}
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class MockSchemaRegistrySerDe(topicInfo: TopicInfo, mockSchemaRegistryClient: MockSchemaRegistryClient)
    extends SchemaRegistrySerDe(topicInfo) {
  override def buildSchemaRegistryClient(schemeString: String,
                                         registryHost: String,
                                         maybePortString: Option[String]): MockSchemaRegistryClient =
    mockSchemaRegistryClient
}

class SchemaRegistrySerDeSpec extends AnyFlatSpec {

  private val avroSchemaProvider: SchemaProvider = new AvroSchemaProvider
  private val protoSchemaProvider: SchemaProvider = new ProtobufSchemaProvider
  val schemaRegistryClient = new MockSchemaRegistryClient(Seq(avroSchemaProvider, protoSchemaProvider).asJava)

  it should "fail if the schema subject is not found" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)
    assertThrows[IllegalArgumentException] {
      schemaRegistrySchemaProvider.schema
    }
  }

  it should "succeed if we look up an avro schema that is present" in {
    val topicInfo = TopicInfo("test-topic-avro", "kafka", Map(RegistryHostKey -> "localhost"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("test-topic-avro-value", new AvroSchema(avroSchemaStr))
    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  it should "succeed if we look up an avro schema using injected subject" in {
    val avroSchemaStr =
      "{ \"type\": \"record\", \"name\": \"test1\", \"fields\": [ { \"type\": \"string\", \"name\": \"field1\" }, { \"type\": \"int\", \"name\": \"field2\" }]}"
    schemaRegistryClient.register("my-subject", new AvroSchema(avroSchemaStr))

    val topicInfo = TopicInfo("another-topic", "kafka", Map(RegistryHostKey -> "localhost", "subject" -> "my-subject"))
    val schemaRegistrySchemaProvider =
      new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val deserSchema = schemaRegistrySchemaProvider.schema
    assert(deserSchema != null)
  }

  // ============== Proto3 Tests ==============

  it should "succeed if we look up a proto3 schema" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin
    schemaRegistryClient.register("test-topic-proto3-value", new ProtobufSchema(proto3SchemaStr))

    val topicInfo = TopicInfo("test-topic-proto3", "kafka", Map(RegistryHostKey -> "localhost"))
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto3 messages" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-topic-proto3-deser-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-topic-proto3-deser",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("username"), "alice")
      .setField(descriptor.findFieldByName("user_id"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "alice")
    assert(mutation.after(1) == 42)
  }

  it should "handle proto3DefaultAsNull parameter for proto3 schemas" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-proto3-defaults-value", protobufSchema)

    val topicInfoWithNull = TopicInfo(
      "test-proto3-defaults",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false", Proto3DefaultAsNullKey -> "true")
    )
    val serDeWithNull = new MockSchemaRegistrySerDe(topicInfoWithNull, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    val topicInfoWithoutNull = TopicInfo(
      "test-proto3-defaults",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false", Proto3DefaultAsNullKey -> "false")
    )
    val serDeWithoutNull = new MockSchemaRegistrySerDe(topicInfoWithoutNull, schemaRegistryClient)

    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)
    assert(mutationWithoutNull.after(0) == "")
    assert(mutationWithoutNull.after(1) == 0)
  }

  it should "handle wire format with 5-byte header for proto3" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message WireFormatTest {
        |  string value = 1;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto3SchemaStr)
    schemaRegistryClient.register("test-wire-format-proto3-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-wire-format-proto3",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "true")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("value"), "test")
      .build()

    val wireFormatBytes = Array[Byte](0x00, 0x00, 0x00, 0x00, 0x01) ++ message.toByteArray

    val mutation = serDe.fromBytes(wireFormatBytes)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
  }

  // ============== Proto2 Tests ==============

  it should "succeed if we look up a proto2 schema" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin
    schemaRegistryClient.register("test-topic-proto2-value", new ProtobufSchema(proto2SchemaStr))

    val topicInfo = TopicInfo("test-topic-proto2", "kafka", Map(RegistryHostKey -> "localhost"))
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize proto2 messages with required and optional fields" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    schemaRegistryClient.register("test-topic-proto2-deser-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-topic-proto2-deser",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "bob")
      .setField(descriptor.findFieldByName("id"), 123)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "bob")
    assert(mutation.after(1) == 123)
  }

  it should "handle proto2 unset optional fields as null" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message OptionalTest {
        |  required string name = 1;
        |  optional int32 value = 2;
        |}""".stripMargin
    val protobufSchema = new ProtobufSchema(proto2SchemaStr)
    schemaRegistryClient.register("test-proto2-optional-value", protobufSchema)

    val topicInfo = TopicInfo(
      "test-proto2-optional",
      "kafka",
      Map(RegistryHostKey -> "localhost", SchemaRegistryWireFormat -> "false")
    )
    val serDe = new MockSchemaRegistrySerDe(topicInfo, schemaRegistryClient)

    val descriptor = protobufSchema.toDescriptor()
    val messageWithOnlyRequired = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "test")
      .build()

    val mutation = serDe.fromBytes(messageWithOnlyRequired.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "test")
    assert(mutation.after(1) == null)
  }
}
