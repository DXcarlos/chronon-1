package ai.chronon.flink.test.deser

import ai.chronon.api.{Accuracy, Builders, GroupBy, StructType => ChrononStructType}
import ai.chronon.online.serde.{Mutation, ProtobufSerDe, SerDe}
import com.google.protobuf.{ByteString, Descriptors, DynamicMessage}
import com.google.protobuf.DescriptorProtos._

import scala.collection.JavaConverters._

/** In-memory protobuf SerDe provider for testing.
  * Wraps ProtobufSerDe to provide a SerDe for dynamically-built descriptors.
  */
class InMemoryProtobufDeserializationSchemaProvider(descriptor: Descriptors.Descriptor) extends SerDe {

  private val delegate = new ProtobufSerDe(descriptor)

  override def schema: ChrononStructType = delegate.schema

  override def fromBytes(message: Array[Byte]): Mutation = delegate.fromBytes(message)
}

/** Helper object for creating test protobuf messages and schemas.
  */
object ProtobufObjectCreator {

  /** Creates a User-like descriptor matching the UserAvroSchema for testing.
    */
  def createUserDescriptor(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("user_test_proto3.proto")
      .setPackage("ai.chronon.flink.test")
      .setSyntax("proto3")

    val addressBuilder = DescriptorProto
      .newBuilder()
      .setName("Address")
      .addField(
        FieldDescriptorProto.newBuilder().setName("street").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("city").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("country")
          .setNumber(3)
          .setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("postal_code")
          .setNumber(4)
          .setType(FieldDescriptorProto.Type.TYPE_STRING))

    val preferencesEntryBuilder = DescriptorProto
      .newBuilder()
      .setName("PreferencesEntry")
      .setOptions(MessageOptions.newBuilder().setMapEntry(true))
      .addField(
        FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_STRING))

    val userBuilder = DescriptorProto
      .newBuilder()
      .setName("User")
      .addField(
        FieldDescriptorProto.newBuilder().setName("id").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_INT32))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("username")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("tags")
          .setNumber(3)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("address")
          .setNumber(4)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("Address"))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("preferences")
          .setNumber(5)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("PreferencesEntry")
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("last_login_timestamp")
          .setNumber(6)
          .setType(FieldDescriptorProto.Type.TYPE_INT64))
      .addField(FieldDescriptorProto
        .newBuilder()
        .setName("is_active")
        .setNumber(7)
        .setType(FieldDescriptorProto.Type.TYPE_BOOL))
      .addNestedType(addressBuilder)
      .addNestedType(preferencesEntryBuilder)

    fileDescProtoBuilder.addMessageType(userBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("User")
  }

  /** Creates dummy User protobuf bytes for testing.
    */
  def createDummyUserRecordBytes(): Array[Byte] = {
    val userDescriptor = createUserDescriptor()
    val addressDescriptor = userDescriptor.findNestedTypeByName("Address")
    val preferencesEntryDescriptor = userDescriptor.findNestedTypeByName("PreferencesEntry")

    val address = DynamicMessage
      .newBuilder(addressDescriptor)
      .setField(addressDescriptor.findFieldByName("street"), "123 Main St")
      .setField(addressDescriptor.findFieldByName("city"), "San Francisco")
      .setField(addressDescriptor.findFieldByName("country"), "USA")
      .setField(addressDescriptor.findFieldByName("postal_code"), "94105")
      .build()

    val pref1 = DynamicMessage
      .newBuilder(preferencesEntryDescriptor)
      .setField(preferencesEntryDescriptor.findFieldByName("key"), "theme")
      .setField(preferencesEntryDescriptor.findFieldByName("value"), "dark")
      .build()

    val pref2 = DynamicMessage
      .newBuilder(preferencesEntryDescriptor)
      .setField(preferencesEntryDescriptor.findFieldByName("key"), "language")
      .setField(preferencesEntryDescriptor.findFieldByName("value"), "en")
      .build()

    DynamicMessage
      .newBuilder(userDescriptor)
      .setField(userDescriptor.findFieldByName("id"), 12345)
      .setField(userDescriptor.findFieldByName("username"), "johndoe")
      .addRepeatedField(userDescriptor.findFieldByName("tags"), "active")
      .addRepeatedField(userDescriptor.findFieldByName("tags"), "premium")
      .addRepeatedField(userDescriptor.findFieldByName("tags"), "verified")
      .setField(userDescriptor.findFieldByName("address"), address)
      .addRepeatedField(userDescriptor.findFieldByName("preferences"), pref1)
      .addRepeatedField(userDescriptor.findFieldByName("preferences"), pref2)
      .setField(userDescriptor.findFieldByName("last_login_timestamp"), System.currentTimeMillis())
      .setField(userDescriptor.findFieldByName("is_active"), true)
      .build()
      .toByteArray
  }

  def makeMetadataOnlyGroupBy(): GroupBy = {
    Builders.GroupBy(
      sources = Seq.empty,
      metaData = Builders.MetaData(
        name = "user-count-proto"
      ),
      accuracy = Accuracy.TEMPORAL
    )
  }

  def makeGroupBy(projections: Map[String, String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = projections,
            wheres = filters,
            timeColumn = "last_login_timestamp",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("username"),
      aggregations = Seq.empty,
      metaData = Builders.MetaData(
        name = "user-groupby-proto"
      ),
      accuracy = Accuracy.TEMPORAL
    )

  // ============== Proto2 Support ==============

  /** Creates a User-like descriptor using proto2 syntax.
    * Key differences from proto3:
    * - Fields have explicit LABEL_OPTIONAL or LABEL_REQUIRED
    * - No native map support (uses repeated message without map_entry option)
    * - Syntax set to "proto2"
    */
  def createUserDescriptorProto2(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("user_test_proto2.proto")
      .setPackage("ai.chronon.flink.test.proto2")
      .setSyntax("proto2")

    val addressBuilder = DescriptorProto
      .newBuilder()
      .setName("AddressProto2")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("street")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("city")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("country")
          .setNumber(3)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("postal_code")
          .setNumber(4)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))

    // PreferenceEntry as a regular message (not map_entry) for proto2 map simulation
    val preferenceEntryBuilder = DescriptorProto
      .newBuilder()
      .setName("PreferenceEntry")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("key")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("value")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED))

    val userBuilder = DescriptorProto
      .newBuilder()
      .setName("UserProto2")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("id")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_INT32)
          .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("username")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("tags")
          .setNumber(3)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("address")
          .setNumber(4)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("AddressProto2")
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("preferences")
          .setNumber(5)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("PreferenceEntry")
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("last_login_timestamp")
          .setNumber(6)
          .setType(FieldDescriptorProto.Type.TYPE_INT64)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("is_active")
          .setNumber(7)
          .setType(FieldDescriptorProto.Type.TYPE_BOOL)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addNestedType(addressBuilder)
      .addNestedType(preferenceEntryBuilder)

    fileDescProtoBuilder.addMessageType(userBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("UserProto2")
  }

  /** Creates dummy User protobuf bytes using proto2 descriptor for testing.
    * Note: Proto2 preferences are a repeated message (list), not a map.
    */
  def createDummyUserRecordBytesProto2(): Array[Byte] = {
    val userDescriptor = createUserDescriptorProto2()
    val addressDescriptor = userDescriptor.findNestedTypeByName("AddressProto2")
    val preferenceEntryDescriptor = userDescriptor.findNestedTypeByName("PreferenceEntry")

    val address = DynamicMessage
      .newBuilder(addressDescriptor)
      .setField(addressDescriptor.findFieldByName("street"), "123 Main St")
      .setField(addressDescriptor.findFieldByName("city"), "San Francisco")
      .setField(addressDescriptor.findFieldByName("country"), "USA")
      .setField(addressDescriptor.findFieldByName("postal_code"), "94105")
      .build()

    val pref1 = DynamicMessage
      .newBuilder(preferenceEntryDescriptor)
      .setField(preferenceEntryDescriptor.findFieldByName("key"), "theme")
      .setField(preferenceEntryDescriptor.findFieldByName("value"), "dark")
      .build()

    val pref2 = DynamicMessage
      .newBuilder(preferenceEntryDescriptor)
      .setField(preferenceEntryDescriptor.findFieldByName("key"), "language")
      .setField(preferenceEntryDescriptor.findFieldByName("value"), "en")
      .build()

    DynamicMessage
      .newBuilder(userDescriptor)
      .setField(userDescriptor.findFieldByName("id"), 12345)
      .setField(userDescriptor.findFieldByName("username"), "johndoe")
      .addRepeatedField(userDescriptor.findFieldByName("tags"), "active")
      .addRepeatedField(userDescriptor.findFieldByName("tags"), "premium")
      .addRepeatedField(userDescriptor.findFieldByName("tags"), "verified")
      .setField(userDescriptor.findFieldByName("address"), address)
      .addRepeatedField(userDescriptor.findFieldByName("preferences"), pref1)
      .addRepeatedField(userDescriptor.findFieldByName("preferences"), pref2)
      .setField(userDescriptor.findFieldByName("last_login_timestamp"), System.currentTimeMillis())
      .setField(userDescriptor.findFieldByName("is_active"), true)
      .build()
      .toByteArray
  }

  def makeGroupByProto2(projections: Map[String, String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = projections,
            wheres = filters,
            timeColumn = "last_login_timestamp",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("username"),
      aggregations = Seq.empty,
      metaData = Builders.MetaData(
        name = "user-groupby-proto2"
      ),
      accuracy = Accuracy.TEMPORAL
    )
}
