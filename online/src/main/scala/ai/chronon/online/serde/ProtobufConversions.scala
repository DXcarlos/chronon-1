package ai.chronon.online.serde

import ai.chronon.api._
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.{ByteString, Message}

import java.util
import scala.collection.JavaConverters._

/** Utilities for converting protobuf messages and schemas to Chronon types.
  *
  * Limitations:
  *   - Self-referential messages are not supported (will throw IllegalArgumentException)
  *   - oneof fields are treated as regular optional fields
  *   - Extensions are not supported
  *   - Any/unknown fields are ignored
  */
object ProtobufConversions {

  def toChrononSchema(descriptor: Descriptor): StructType = {
    toChrononSchemaInternal(descriptor, Set.empty)
  }

  private def toChrononSchemaInternal(descriptor: Descriptor, visited: Set[String]): StructType = {
    val fullName = descriptor.getFullName
    if (visited.contains(fullName)) {
      throw new IllegalArgumentException(
        s"Cycle detected in protobuf schema: '$fullName' is self-referential. " +
          s"Self-referential protobuf messages are not supported."
      )
    }
    val newVisited = visited + fullName
    val fields = descriptor.getFields.asScala.map { field =>
      StructField(field.getName, fieldToChrononTypeInternal(field, newVisited))
    }.toArray
    StructType(descriptor.getName, fields)
  }

  def fieldToChrononType(field: FieldDescriptor): DataType = {
    fieldToChrononTypeInternal(field, Set.empty)
  }

  private def fieldToChrononTypeInternal(field: FieldDescriptor, visited: Set[String]): DataType = {
    if (field.isMapField) {
      val keyType = fieldToChrononTypeInternal(field.getMessageType.findFieldByName("key"), visited)
      val valueType = fieldToChrononTypeInternal(field.getMessageType.findFieldByName("value"), visited)
      MapType(keyType, valueType)
    } else if (field.isRepeated) {
      ListType(scalarToChrononType(field, visited))
    } else {
      scalarToChrononType(field, visited)
    }
  }

  private def scalarToChrononType(field: FieldDescriptor, visited: Set[String]): DataType = {
    import FieldDescriptor.JavaType._
    import FieldDescriptor.Type
    field.getJavaType match {
      case BOOLEAN => BooleanType
      case INT     =>
        // uint32 and fixed32 can hold values >= 2^31, so use LongType to preserve unsigned semantics
        field.getType match {
          case Type.UINT32 | Type.FIXED32 => LongType
          case _                          => IntType
        }
      case LONG        => LongType
      case FLOAT       => FloatType
      case DOUBLE      => DoubleType
      case STRING      => StringType
      case BYTE_STRING => BinaryType
      case ENUM        => StringType // Enums are represented as their string name
      case MESSAGE     => toChrononSchemaInternal(field.getMessageType, visited)
    }
  }

  def toChrononRow(message: Message, schema: StructType, proto3DefaultAsNull: Boolean = false): Array[Any] = {
    val descriptor = message.getDescriptorForType
    val result = new Array[Any](schema.fields.length)

    schema.fields.zipWithIndex.foreach { case (structField, idx) =>
      val field = descriptor.findFieldByName(structField.name)
      if (field != null) {
        result(idx) = getFieldValue(message, field, structField.fieldType, proto3DefaultAsNull)
      }
    }
    result
  }

  private def getFieldValue(
      message: Message,
      field: FieldDescriptor,
      dataType: DataType,
      proto3DefaultAsNull: Boolean
  ): Any = {
    if (field.isMapField) {
      getMapFieldValue(message, field, dataType.asInstanceOf[MapType], proto3DefaultAsNull)
    } else if (field.isRepeated) {
      getRepeatedFieldValue(message, field, dataType.asInstanceOf[ListType], proto3DefaultAsNull)
    } else {
      getScalarFieldValue(message, field, dataType, proto3DefaultAsNull)
    }
  }

  private def getScalarFieldValue(
      message: Message,
      field: FieldDescriptor,
      dataType: DataType,
      proto3DefaultAsNull: Boolean
  ): Any = {
    // For proto2 or proto3 with explicit optional, check hasField
    // For proto3 implicit presence, field is always "set" with default value
    val hasField = message.hasField(field)

    // If field is not set and we should treat defaults as null
    if (!hasField && proto3DefaultAsNull && !field.hasPresence) {
      return null
    }

    // If field has presence tracking (proto2 optional or proto3 explicit optional) and is not set
    if (field.hasPresence && !hasField) {
      return null
    }

    val value = message.getField(field)
    convertValue(value, dataType, proto3DefaultAsNull)
  }

  private def getRepeatedFieldValue(
      message: Message,
      field: FieldDescriptor,
      listType: ListType,
      proto3DefaultAsNull: Boolean
  ): util.ArrayList[Any] = {
    val count = message.getRepeatedFieldCount(field)
    val result = new util.ArrayList[Any](count)

    for (i <- 0 until count) {
      val value = message.getRepeatedField(field, i)
      result.add(convertValue(value, listType.elementType, proto3DefaultAsNull))
    }
    result
  }

  private def getMapFieldValue(
      message: Message,
      field: FieldDescriptor,
      mapType: MapType,
      proto3DefaultAsNull: Boolean
  ): util.Map[Any, Any] = {
    val count = message.getRepeatedFieldCount(field)
    val result = new util.HashMap[Any, Any](count)
    val entryDescriptor = field.getMessageType
    val keyField = entryDescriptor.findFieldByName("key")
    val valueField = entryDescriptor.findFieldByName("value")

    for (i <- 0 until count) {
      val entry = message.getRepeatedField(field, i).asInstanceOf[Message]
      // Keys cannot be null in protobuf maps, so proto3DefaultAsNull=false for keys
      val key = convertValue(entry.getField(keyField), mapType.keyType, proto3DefaultAsNull = false)
      val value = convertValue(entry.getField(valueField), mapType.valueType, proto3DefaultAsNull)
      result.put(key, value)
    }
    result
  }

  private def convertValue(value: Any, dataType: DataType, proto3DefaultAsNull: Boolean): Any = {
    if (value == null) return null

    (value, dataType) match {
      case (b: java.lang.Boolean, BooleanType)  => b
      case (i: java.lang.Integer, IntType)      => i
      case (i: java.lang.Integer, LongType)     => Integer.toUnsignedLong(i) // uint32/fixed32 stored as unsigned long
      case (l: java.lang.Long, LongType)        => l
      case (f: java.lang.Float, FloatType)      => f
      case (d: java.lang.Double, DoubleType)    => d
      case (s: String, StringType)              => s
      case (bs: ByteString, BinaryType)         => bs.toByteArray
      case (e: EnumValueDescriptor, StringType) => e.getName
      case (e: EnumValueDescriptor, IntType)    => e.getNumber
      case (m: Message, st: StructType)         => toChrononRow(m, st, proto3DefaultAsNull)
      case _                                    => value
    }
  }

  def createRowConverter(schema: StructType, proto3DefaultAsNull: Boolean = false): Message => Array[Any] = { message =>
    toChrononRow(message, schema, proto3DefaultAsNull)
  }
}
