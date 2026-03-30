package ai.chronon.online.serde

import com.google.protobuf.Descriptors
import com.google.protobuf.DescriptorProtos._

/**
  * Proto schema definitions for ProtobufConversionsTest.
  *
  * These helpers build proto descriptors programmatically. The equivalent .proto definitions are:
  *
  * {{{
  * // primitives.proto
  * syntax = "proto3";
  * message Primitives {
  *   bool bool_field = 1;
  *   int32 int32_field = 2;
  *   int64 int64_field = 3;
  *   float float_field = 4;
  *   double double_field = 5;
  *   string string_field = 6;
  *   bytes bytes_field = 7;
  * }
  *
  * // nested.proto
  * syntax = "proto3";
  * message User {
  *   string name = 1;
  *   int32 age = 2;
  *   Address address = 3;
  *   message Address {
  *     string street = 1;
  *     string city = 2;
  *   }
  * }
  *
  * // repeated.proto
  * syntax = "proto3";
  * message ListMessage {
  *   repeated int32 ids = 1;
  *   repeated string names = 2;
  * }
  *
  * // map.proto
  * syntax = "proto3";
  * message MapMessage {
  *   map<string, string> preferences = 1;
  * }
  * }}}
  */
object TestProtoSchemas {

  def simpleDescriptor(name: String,
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

  def nestedDescriptor(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("nested.proto")
      .setPackage("test")
      .setSyntax("proto3")

    val addressBuilder = DescriptorProto
      .newBuilder()
      .setName("Address")
      .addField(
        FieldDescriptorProto.newBuilder().setName("street").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("city").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_STRING))

    val userBuilder = DescriptorProto
      .newBuilder()
      .setName("User")
      .addField(
        FieldDescriptorProto.newBuilder().setName("name").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("age").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_INT32))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("address")
          .setNumber(3)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("Address"))
      .addNestedType(addressBuilder)

    fileDescProtoBuilder.addMessageType(userBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("User")
  }

  def repeatedDescriptor(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("repeated.proto")
      .setPackage("test")
      .setSyntax("proto3")

    val messageBuilder = DescriptorProto
      .newBuilder()
      .setName("ListMessage")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("ids")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_INT32)
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("names")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))

    fileDescProtoBuilder.addMessageType(messageBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("ListMessage")
  }

  def mapDescriptor(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("map.proto")
      .setPackage("test")
      .setSyntax("proto3")

    val mapEntryBuilder = DescriptorProto
      .newBuilder()
      .setName("PreferencesEntry")
      .setOptions(MessageOptions.newBuilder().setMapEntry(true))
      .addField(
        FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
      .addField(
        FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_STRING))

    val messageBuilder = DescriptorProto
      .newBuilder()
      .setName("MapMessage")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("preferences")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("PreferencesEntry")
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
      .addNestedType(mapEntryBuilder)

    fileDescProtoBuilder.addMessageType(messageBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("MapMessage")
  }

  // Proto2 variants

  def simpleDescriptorProto2(
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

  def nestedDescriptorProto2(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("nested_proto2.proto")
      .setPackage("test")
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
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("city")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )

    val userBuilder = DescriptorProto
      .newBuilder()
      .setName("UserProto2")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("name")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
      )
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("age")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_INT32)
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("address")
          .setNumber(3)
          .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
          .setTypeName("AddressProto2")
          .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
      )
      .addNestedType(addressBuilder)

    fileDescProtoBuilder.addMessageType(userBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("UserProto2")
  }

  def repeatedDescriptorProto2(): Descriptors.Descriptor = {
    val fileDescProtoBuilder = FileDescriptorProto
      .newBuilder()
      .setName("repeated_proto2.proto")
      .setPackage("test")
      .setSyntax("proto2")

    val messageBuilder = DescriptorProto
      .newBuilder()
      .setName("ListMessageProto2")
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("ids")
          .setNumber(1)
          .setType(FieldDescriptorProto.Type.TYPE_INT32)
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
      )
      .addField(
        FieldDescriptorProto
          .newBuilder()
          .setName("names")
          .setNumber(2)
          .setType(FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
      )

    fileDescProtoBuilder.addMessageType(messageBuilder.build())
    val fileDescProto = fileDescProtoBuilder.build()
    val fileDesc = Descriptors.FileDescriptor.buildFrom(fileDescProto, Array.empty)
    fileDesc.findMessageTypeByName("ListMessageProto2")
  }
}
