package ai.chronon.online

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{DataType, GroupBy, StructType}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.generic.GenericData

/**
  * Encodes/decodes the windowed mega tile IR (Array[Any]) to/from bytes.
  * Unlike TileCodec which uses the unwindowed base aggregator schema,
  * MegaTileCodec uses the windowed aggregator schema — one IR slot per (agg, window) pair.
  */
class MegaTileCodec(groupBy: GroupBy, inputSchema: Seq[(String, DataType)]) {

  val rowAggregator: RowAggregator = TileCodec.buildWindowedRowAggregator(groupBy, inputSchema)
  val irSchema: StructType = StructType.from(s"${groupBy.getMetaData.cleanName}_MEGA_TILE_IR", rowAggregator.irSchema)
  val avroSchema: String = AvroConversions.fromChrononSchema(irSchema).toString()

  private val encodeFn: Any => Array[Byte] = AvroConversions.encodeBytes(irSchema, null)

  @transient private lazy val rowConverter: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(irSchema)

  def encode(ir: Array[Any]): Array[Byte] = encodeFn(rowAggregator.normalize(ir))

  def decode(bytes: Array[Byte]): Array[Any] = {
    val codec = AvroCodec.of(avroSchema)
    val record = codec.decode(bytes).asInstanceOf[GenericData.Record]
    val ir = rowConverter(record)
    rowAggregator.denormalize(ir)
  }
}
