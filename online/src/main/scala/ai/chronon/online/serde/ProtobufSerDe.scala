package ai.chronon.online.serde

import ai.chronon.api.{Constants, StructType}
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage

class ProtobufSerDe(descriptor: Descriptor, proto3DefaultAsNull: Boolean = false) extends SerDe {

  lazy val chrononSchema: StructType = ProtobufConversions.toChrononSchema(descriptor)

  @transient private lazy val rowConverter =
    ProtobufConversions.createRowConverter(chrononSchema, proto3DefaultAsNull)

  override def schema: StructType = chrononSchema

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val message = DynamicMessage.parseFrom(descriptor, bytes)
    val row = rowConverter(message)

    val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
    val isReversal = reversalIndex >= 0 && row(reversalIndex) != null && row(reversalIndex).asInstanceOf[Boolean]
    if (isReversal) {
      Mutation(schema, row, null)
    } else {
      Mutation(schema, null, row)
    }
  }
}
