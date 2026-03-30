package ai.chronon.online.serde

import ai.chronon.api._
import com.google.protobuf.{ByteString, DynamicMessage}
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ProtobufConversionsTest extends AnyFlatSpec with Matchers {

  behavior of "ProtobufConversions.toChrononSchema"

  it should "convert primitive types correctly" in {
    val descriptor = TestProtoSchemas.simpleDescriptor(
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

    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.name shouldBe "Primitives"
    schema.fields.length shouldBe 7

    schema.fields(0) shouldBe StructField("bool_field", BooleanType)
    schema.fields(1) shouldBe StructField("int32_field", IntType)
    schema.fields(2) shouldBe StructField("int64_field", LongType)
    schema.fields(3) shouldBe StructField("float_field", FloatType)
    schema.fields(4) shouldBe StructField("double_field", DoubleType)
    schema.fields(5) shouldBe StructField("string_field", StringType)
    schema.fields(6) shouldBe StructField("bytes_field", BinaryType)
  }

  it should "convert unsigned integer types to LongType to preserve values >= 2^31" in {
    val descriptor = TestProtoSchemas.simpleDescriptor(
      "UnsignedTypes",
      Seq(
        "uint32_field" -> FieldDescriptorProto.Type.TYPE_UINT32,
        "fixed32_field" -> FieldDescriptorProto.Type.TYPE_FIXED32,
        "sint32_field" -> FieldDescriptorProto.Type.TYPE_SINT32,
        "sfixed32_field" -> FieldDescriptorProto.Type.TYPE_SFIXED32
      )
    )

    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.fields.length shouldBe 4
    // uint32 and fixed32 should be LongType to handle values >= 2^31
    schema.fields(0) shouldBe StructField("uint32_field", LongType)
    schema.fields(1) shouldBe StructField("fixed32_field", LongType)
    // sint32 and sfixed32 are signed, so they stay as IntType
    schema.fields(2) shouldBe StructField("sint32_field", IntType)
    schema.fields(3) shouldBe StructField("sfixed32_field", IntType)
  }

  it should "convert nested message types" in {
    val descriptor = TestProtoSchemas.nestedDescriptor()
    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.name shouldBe "User"
    schema.fields.length shouldBe 3

    schema.fields(0) shouldBe StructField("name", StringType)
    schema.fields(1) shouldBe StructField("age", IntType)

    val addressType = schema.fields(2).fieldType.asInstanceOf[StructType]
    addressType.name shouldBe "Address"
    addressType.fields.length shouldBe 2
    addressType.fields(0) shouldBe StructField("street", StringType)
    addressType.fields(1) shouldBe StructField("city", StringType)
  }

  it should "convert repeated fields to ListType" in {
    val descriptor = TestProtoSchemas.repeatedDescriptor()
    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.fields.length shouldBe 2
    schema.fields(0) shouldBe StructField("ids", ListType(IntType))
    schema.fields(1) shouldBe StructField("names", ListType(StringType))
  }

  it should "convert map fields to MapType" in {
    val descriptor = TestProtoSchemas.mapDescriptor()
    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.fields.length shouldBe 1
    schema.fields(0) shouldBe StructField("preferences", MapType(StringType, StringType))
  }

  behavior of "ProtobufConversions.toChrononRow"

  it should "convert primitive values correctly" in {
    val descriptor = TestProtoSchemas.simpleDescriptor(
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

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    row(0) shouldBe true
    row(1) shouldBe 42
    row(2) shouldBe 123456789L
    row(3) shouldBe 3.14f
    row(4) shouldBe 2.718281828
    row(5) shouldBe "hello"
    row(6) shouldBe "bytes".getBytes
  }

  it should "convert unsigned integer values >= 2^31 correctly" in {
    val descriptor = TestProtoSchemas.simpleDescriptor("UnsignedValues",
                                           Seq(
                                             "uint32_field" -> FieldDescriptorProto.Type.TYPE_UINT32,
                                             "fixed32_field" -> FieldDescriptorProto.Type.TYPE_FIXED32
                                           ))

    // Use a value >= 2^31 (e.g., 3_000_000_000 which is 0xB2D05E00)
    // When stored as signed int, this would be negative (-1_294_967_296)
    val largeUnsignedValue = 3000000000L
    val asSignedInt = largeUnsignedValue.toInt // This will be negative when interpreted as signed

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("uint32_field"), asSignedInt)
      .setField(descriptor.findFieldByName("fixed32_field"), asSignedInt)
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    // Values should be converted back to unsigned long representation
    row(0) shouldBe largeUnsignedValue
    row(1) shouldBe largeUnsignedValue
  }

  it should "convert nested messages" in {
    val descriptor = TestProtoSchemas.nestedDescriptor()
    val addressDescriptor = descriptor.findNestedTypeByName("Address")

    val address = DynamicMessage
      .newBuilder(addressDescriptor)
      .setField(addressDescriptor.findFieldByName("street"), "123 Main St")
      .setField(addressDescriptor.findFieldByName("city"), "San Francisco")
      .build()

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "John")
      .setField(descriptor.findFieldByName("age"), 30)
      .setField(descriptor.findFieldByName("address"), address)
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    row(0) shouldBe "John"
    row(1) shouldBe 30

    val addressRow = row(2).asInstanceOf[Array[Any]]
    addressRow(0) shouldBe "123 Main St"
    addressRow(1) shouldBe "San Francisco"
  }

  it should "convert repeated fields to ArrayList" in {
    val descriptor = TestProtoSchemas.repeatedDescriptor()

    val message = DynamicMessage
      .newBuilder(descriptor)
      .addRepeatedField(descriptor.findFieldByName("ids"), 1)
      .addRepeatedField(descriptor.findFieldByName("ids"), 2)
      .addRepeatedField(descriptor.findFieldByName("ids"), 3)
      .addRepeatedField(descriptor.findFieldByName("names"), "a")
      .addRepeatedField(descriptor.findFieldByName("names"), "b")
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    val ids = row(0).asInstanceOf[java.util.ArrayList[Any]]
    ids.asScala should contain theSameElementsInOrderAs Seq(1, 2, 3)

    val names = row(1).asInstanceOf[java.util.ArrayList[Any]]
    names.asScala should contain theSameElementsInOrderAs Seq("a", "b")
  }

  it should "convert map fields to HashMap" in {
    val descriptor = TestProtoSchemas.mapDescriptor()
    val mapEntryDescriptor = descriptor.findNestedTypeByName("PreferencesEntry")

    val entry1 = DynamicMessage
      .newBuilder(mapEntryDescriptor)
      .setField(mapEntryDescriptor.findFieldByName("key"), "theme")
      .setField(mapEntryDescriptor.findFieldByName("value"), "dark")
      .build()

    val entry2 = DynamicMessage
      .newBuilder(mapEntryDescriptor)
      .setField(mapEntryDescriptor.findFieldByName("key"), "language")
      .setField(mapEntryDescriptor.findFieldByName("value"), "en")
      .build()

    val message = DynamicMessage
      .newBuilder(descriptor)
      .addRepeatedField(descriptor.findFieldByName("preferences"), entry1)
      .addRepeatedField(descriptor.findFieldByName("preferences"), entry2)
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    val map = row(0).asInstanceOf[java.util.Map[Any, Any]]
    map.get("theme") shouldBe "dark"
    map.get("language") shouldBe "en"
  }

  it should "handle empty repeated fields" in {
    val descriptor = TestProtoSchemas.repeatedDescriptor()
    val message = DynamicMessage.newBuilder(descriptor).build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    val ids = row(0).asInstanceOf[java.util.ArrayList[Any]]
    ids.size() shouldBe 0

    val names = row(1).asInstanceOf[java.util.ArrayList[Any]]
    names.size() shouldBe 0
  }

  behavior of "ProtobufConversions with proto2 syntax"

  it should "convert proto2 primitive types with required/optional labels" in {
    val descriptor = TestProtoSchemas.simpleDescriptorProto2(
      "PrimitivesProto2",
      Seq(
        ("required_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("optional_int", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("optional_bool", FieldDescriptorProto.Type.TYPE_BOOL, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.name shouldBe "PrimitivesProto2"
    schema.fields.length shouldBe 3
    schema.fields(0) shouldBe StructField("required_string", StringType)
    schema.fields(1) shouldBe StructField("optional_int", IntType)
    schema.fields(2) shouldBe StructField("optional_bool", BooleanType)
  }

  it should "convert proto2 nested message types" in {
    val descriptor = TestProtoSchemas.nestedDescriptorProto2()
    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.name shouldBe "UserProto2"
    schema.fields.length shouldBe 3

    schema.fields(0) shouldBe StructField("name", StringType)
    schema.fields(1) shouldBe StructField("age", IntType)

    val addressType = schema.fields(2).fieldType.asInstanceOf[StructType]
    addressType.name shouldBe "AddressProto2"
    addressType.fields.length shouldBe 2
    addressType.fields(0) shouldBe StructField("street", StringType)
    addressType.fields(1) shouldBe StructField("city", StringType)
  }

  it should "convert proto2 repeated fields to ListType" in {
    val descriptor = TestProtoSchemas.repeatedDescriptorProto2()
    val schema = ProtobufConversions.toChrononSchema(descriptor)

    schema.fields.length shouldBe 2
    schema.fields(0) shouldBe StructField("ids", ListType(IntType))
    schema.fields(1) shouldBe StructField("names", ListType(StringType))
  }

  it should "convert proto2 message values correctly" in {
    val descriptor = TestProtoSchemas.simpleDescriptorProto2(
      "PrimitivesProto2",
      Seq(
        ("required_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("optional_int", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("optional_bool", FieldDescriptorProto.Type.TYPE_BOOL, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("required_string"), "hello")
      .setField(descriptor.findFieldByName("optional_int"), 42)
      .setField(descriptor.findFieldByName("optional_bool"), true)
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    row(0) shouldBe "hello"
    row(1) shouldBe 42
    row(2) shouldBe true
  }

  it should "convert proto2 nested messages" in {
    val descriptor = TestProtoSchemas.nestedDescriptorProto2()
    val addressDescriptor = descriptor.findNestedTypeByName("AddressProto2")

    val address = DynamicMessage
      .newBuilder(addressDescriptor)
      .setField(addressDescriptor.findFieldByName("street"), "456 Oak Ave")
      .setField(addressDescriptor.findFieldByName("city"), "Oakland")
      .build()

    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "Jane")
      .setField(descriptor.findFieldByName("age"), 25)
      .setField(descriptor.findFieldByName("address"), address)
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    row(0) shouldBe "Jane"
    row(1) shouldBe 25

    val addressRow = row(2).asInstanceOf[Array[Any]]
    addressRow(0) shouldBe "456 Oak Ave"
    addressRow(1) shouldBe "Oakland"
  }

  it should "convert proto2 repeated fields to ArrayList" in {
    val descriptor = TestProtoSchemas.repeatedDescriptorProto2()

    val message = DynamicMessage
      .newBuilder(descriptor)
      .addRepeatedField(descriptor.findFieldByName("ids"), 10)
      .addRepeatedField(descriptor.findFieldByName("ids"), 20)
      .addRepeatedField(descriptor.findFieldByName("names"), "x")
      .addRepeatedField(descriptor.findFieldByName("names"), "y")
      .addRepeatedField(descriptor.findFieldByName("names"), "z")
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    val ids = row(0).asInstanceOf[java.util.ArrayList[Any]]
    ids.asScala should contain theSameElementsInOrderAs Seq(10, 20)

    val names = row(1).asInstanceOf[java.util.ArrayList[Any]]
    names.asScala should contain theSameElementsInOrderAs Seq("x", "y", "z")
  }

  it should "handle proto2 unset optional fields as null (proto2 tracks field presence)" in {
    val descriptor = TestProtoSchemas.simpleDescriptorProto2(
      "PrimitivesProto2",
      Seq(
        ("required_string", FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.Label.LABEL_REQUIRED),
        ("optional_int", FieldDescriptorProto.Type.TYPE_INT32, FieldDescriptorProto.Label.LABEL_OPTIONAL),
        ("optional_bool", FieldDescriptorProto.Type.TYPE_BOOL, FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
    )

    // Only set required field, leave optional fields unset
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("required_string"), "required_value")
      .build()

    val schema = ProtobufConversions.toChrononSchema(descriptor)
    val row = ProtobufConversions.toChrononRow(message, schema)

    row(0) shouldBe "required_value"
    // In proto2, unset optional fields return null (proto2 tracks field presence)
    assert(row(1) == null)
    assert(row(2) == null)
  }
}
