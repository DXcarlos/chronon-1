package ai.chronon.online.serde

import ai.chronon.api._
import com.google.protobuf.{ByteString, DynamicMessage}
import com.google.protobuf.DescriptorProtos._
import com.google.protobuf.Descriptors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ProtobufSerDeTest extends AnyFlatSpec with Matchers {

  private def buildSimpleDescriptor(name: String,
                                    fields: Seq[(String, FieldDescriptorProto.Type)]): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName(s"$name.proto")
      .setPackage("test")
      .setSyntax("proto3")

    val messageBuilder = DescriptorProto.newBuilder().setName(name)
    fields.zipWithIndex.foreach { case ((fieldName, fieldType), idx) =>
      messageBuilder.addField(
        FieldDescriptorProto
          .newBuilder()
          .setName(fieldName)
          .setNumber(idx + 1)
          .setType(fieldType)
          .build()
      )
    }
    fileDescProtoBuilder.addMessageType(messageBuilder.build())

    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName(name)
  }

  private def buildProto2Descriptor(
      name: String,
      fields: Seq[(String, FieldDescriptorProto.Type, FieldDescriptorProto.Label)]
  ): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName(s"$name.proto")
      .setPackage("test")
      .setSyntax("proto2")

    val messageBuilder = DescriptorProto.newBuilder().setName(name)
    fields.zipWithIndex.foreach { case ((fieldName, fieldType, label), idx) =>
      messageBuilder.addField(
        FieldDescriptorProto
          .newBuilder()
          .setName(fieldName)
          .setNumber(idx + 1)
          .setType(fieldType)
          .setLabel(label)
          .build()
      )
    }
    fileDescProtoBuilder.addMessageType(messageBuilder.build())

    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName(name)
  }

  behavior of "ProtobufSerDe"

  it should "create schema from descriptor" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.name shouldBe "TestMessage"
    serDe.schema.fields.length shouldBe 2
    serDe.schema.fields(0) shouldBe StructField("string_field", StringType)
    serDe.schema.fields(1) shouldBe StructField("int_field", IntType)
  }

  it should "deserialize protobuf bytes to Mutation" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDe = new ProtobufSerDe(descriptor)

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("string_field"), "hello")
      .setField(descriptor.findFieldByName("int_field"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.schema shouldBe serDe.schema
    mutation.before shouldBe null
    mutation.after should not be null
    mutation.after(0) shouldBe "hello"
    mutation.after(1) shouldBe 42
  }

  it should "handle empty message" in {
    val descriptor = buildSimpleDescriptor("EmptyMessage", Seq.empty)

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.fields shouldBe empty
    serDe.schema.name shouldBe "EmptyMessage"

    val emptyBytes = DynamicMessage.newBuilder(descriptor).build().toByteArray
    val mutation = serDe.fromBytes(emptyBytes)

    mutation.schema shouldBe serDe.schema
    mutation.before shouldBe null
    mutation.after should not be null
    mutation.after.length shouldBe 0
  }

  it should "handle invalid protobuf bytes" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDe = new ProtobufSerDe(descriptor)

    val truncatedVarInt = Array[Byte](0x08, 0x80.toByte)

    intercept[com.google.protobuf.InvalidProtocolBufferException] {
      serDe.fromBytes(truncatedVarInt)
    }
  }

  it should "support proto3DefaultAsNull parameter" in {
    val descriptor = buildSimpleDescriptor("TestMessage",
                                           Seq(
                                             "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
                                             "int_field" -> FieldDescriptorProto.Type.TYPE_INT32
                                           ))

    val serDeWithNull = new ProtobufSerDe(descriptor, proto3DefaultAsNull = true)
    val serDeWithoutNull = new ProtobufSerDe(descriptor, proto3DefaultAsNull = false)

    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)

    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)

    mutationWithoutNull.after(0) shouldBe ""
    mutationWithoutNull.after(1) shouldBe 0
  }

  it should "handle all primitive types" in {
    val descriptor = buildSimpleDescriptor(
      "Primitives",
      Seq(
        "bool_field" -> FieldDescriptorProto.Type.TYPE_BOOL,
        "int32_field" -> FieldDescriptorProto.Type.TYPE_INT32,
        "int64_field" -> FieldDescriptorProto.Type.TYPE_INT64,
        "float_field" -> FieldDescriptorProto.Type.TYPE_FLOAT,
        "double_field" -> FieldDescriptorProto.Type.TYPE_DOUBLE,
        "string_field" -> FieldDescriptorProto.Type.TYPE_STRING,
        "bytes_field" -> FieldDescriptorProto.Type.TYPE_BYTES
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.fields.length shouldBe 7
    serDe.schema.fields(0).fieldType shouldBe BooleanType
    serDe.schema.fields(1).fieldType shouldBe IntType
    serDe.schema.fields(2).fieldType shouldBe LongType
    serDe.schema.fields(3).fieldType shouldBe FloatType
    serDe.schema.fields(4).fieldType shouldBe DoubleType
    serDe.schema.fields(5).fieldType shouldBe StringType
    serDe.schema.fields(6).fieldType shouldBe BinaryType

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("bool_field"), true)
      .setField(descriptor.findFieldByName("int32_field"), 42)
      .setField(descriptor.findFieldByName("int64_field"), 123456789L)
      .setField(descriptor.findFieldByName("float_field"), 3.14f)
      .setField(descriptor.findFieldByName("double_field"), 2.718281828)
      .setField(descriptor.findFieldByName("string_field"), "hello")
      .setField(descriptor.findFieldByName("bytes_field"), ByteString.copyFromUtf8("bytes"))
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.after(0) shouldBe true
    mutation.after(1) shouldBe 42
    mutation.after(2) shouldBe 123456789L
    mutation.after(3) shouldBe 3.14f
    mutation.after(4) shouldBe 2.718281828
    mutation.after(5) shouldBe "hello"
    mutation.after(6) shouldBe "bytes".getBytes
  }

  // ============== Proto2 Tests ==============

  behavior of "ProtobufSerDe with proto2"

  it should "create schema from proto2 descriptor with required and optional fields" in {
    val descriptor = buildProto2Descriptor(
      "Proto2Message",
      Seq(
        ("required_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("optional_int", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.name shouldBe "Proto2Message"
    serDe.schema.fields.length shouldBe 2
    serDe.schema.fields(0) shouldBe StructField("required_string", StringType)
    serDe.schema.fields(1) shouldBe StructField("optional_int", IntType)
  }

  it should "deserialize proto2 messages with required fields" in {
    val descriptor = buildProto2Descriptor(
      "RequiredFields",
      Seq(
        ("name", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("id", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_REQUIRED)
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "alice")
      .setField(descriptor.findFieldByName("id"), 123)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.after should not be null
    mutation.after(0) shouldBe "alice"
    mutation.after(1) shouldBe 123
  }

  it should "handle proto2 unset optional fields as null" in {
    val descriptor = buildProto2Descriptor(
      "OptionalFields",
      Seq(
        ("required_name", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("optional_value", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("optional_text", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("required_name"), "test")
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.after should not be null
    mutation.after(0) shouldBe "test"
    assert(mutation.after(1) == null)
    assert(mutation.after(2) == null)
  }

  it should "handle proto2 set optional fields with values" in {
    val descriptor = buildProto2Descriptor(
      "SetOptionalFields",
      Seq(
        ("required_name", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("optional_value", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("required_name"), "test")
      .setField(descriptor.findFieldByName("optional_value"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.after should not be null
    mutation.after(0) shouldBe "test"
    mutation.after(1) shouldBe 42
  }

  it should "handle proto2 all primitive types with labels" in {
    val descriptor = buildProto2Descriptor(
      "Proto2Primitives",
      Seq(
        ("req_bool", FieldDescriptorProto.Type.TYPE_BOOL, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("opt_int32", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_int64", FieldDescriptorProto.Type.TYPE_INT64, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_float", FieldDescriptorProto.Type.TYPE_FLOAT, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_double", FieldDescriptorProto.Type.TYPE_DOUBLE, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_bytes", FieldDescriptorProto.Type.TYPE_BYTES, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    serDe.schema.fields.length shouldBe 7
    serDe.schema.fields(0).fieldType shouldBe BooleanType
    serDe.schema.fields(1).fieldType shouldBe IntType
    serDe.schema.fields(2).fieldType shouldBe LongType
    serDe.schema.fields(3).fieldType shouldBe FloatType
    serDe.schema.fields(4).fieldType shouldBe DoubleType
    serDe.schema.fields(5).fieldType shouldBe StringType
    serDe.schema.fields(6).fieldType shouldBe BinaryType

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("req_bool"), true)
      .setField(descriptor.findFieldByName("opt_int32"), 42)
      .setField(descriptor.findFieldByName("opt_int64"), 123456789L)
      .setField(descriptor.findFieldByName("opt_float"), 3.14f)
      .setField(descriptor.findFieldByName("opt_double"), 2.718281828)
      .setField(descriptor.findFieldByName("opt_string"), "hello")
      .setField(descriptor.findFieldByName("opt_bytes"), ByteString.copyFromUtf8("bytes"))
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)

    mutation.after(0) shouldBe true
    mutation.after(1) shouldBe 42
    mutation.after(2) shouldBe 123456789L
    mutation.after(3) shouldBe 3.14f
    mutation.after(4) shouldBe 2.718281828
    mutation.after(5) shouldBe "hello"
    mutation.after(6) shouldBe "bytes".getBytes
  }

  it should "handle proto2 with only optional fields set to defaults (null)" in {
    val descriptor = buildProto2Descriptor(
      "AllOptional",
      Seq(
        ("opt_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_int", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_bool", FieldDescriptorProto.Type.TYPE_BOOL, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val serDe = new ProtobufSerDe(descriptor)

    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()
    val mutation = serDe.fromBytes(emptyMessage.toByteArray)

    mutation.after should not be null
    assert(mutation.after(0) == null)
    assert(mutation.after(1) == null)
    assert(mutation.after(2) == null)
  }

  it should "proto3DefaultAsNull should not affect proto2 optional field behavior" in {
    val descriptor = buildProto2Descriptor(
      "Proto2NullTest",
      Seq(
        ("opt_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("opt_int", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val serDeWithNull = new ProtobufSerDe(descriptor, proto3DefaultAsNull = true)
    val serDeWithoutNull = new ProtobufSerDe(descriptor, proto3DefaultAsNull = false)

    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutationWithNull = serDeWithNull.fromBytes(emptyMessage.toByteArray)
    val mutationWithoutNull = serDeWithoutNull.fromBytes(emptyMessage.toByteArray)

    assert(mutationWithNull.after(0) == null)
    assert(mutationWithNull.after(1) == null)
    assert(mutationWithoutNull.after(0) == null)
    assert(mutationWithoutNull.after(1) == null)
  }
}
