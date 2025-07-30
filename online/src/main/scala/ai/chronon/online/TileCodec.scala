/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.BooleanType
import ai.chronon.api.DataType
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.GroupBy
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.StructType
import ai.chronon.online.serde._
import org.apache.avro.generic.GenericData

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}

object TileCodec {
  def buildRowAggregator(groupBy: GroupBy, inputSchema: Seq[(String, DataType)]): RowAggregator = {
    // a set of Chronon groupBy aggregations needs to be flatted out to get the
    // feature column aggregations to be computed. We don't include windows in this
    // to keep the aggregation work & payload size small as the multiple windows for a given
    // counter are identical value wise within a tile (e.g. sum_1d and sum_7d are the same in a tile)
    val unpackedAggs = groupBy.aggregations.asScala.flatMap(_.unWindowed)
    new RowAggregator(inputSchema, unpackedAggs)
  }

  def buildWindowedRowAggregator(groupBy: GroupBy, inputSchema: Seq[(String, DataType)]): RowAggregator = {
    // a set of Chronon groupBy aggregations needs to be flatted out to get the
    // feature column aggregations to be computed. This version includes windows in the feature
    // columns to get the full cross product (buckets * windows) as this is useful in unit tests to compare
    // the final results
    val unpackedAggs = groupBy.aggregations.asScala.flatMap(_.unpack)
    new RowAggregator(inputSchema, unpackedAggs)
  }
}

/** TileCodec is a helper class that allows for the creation of pre-aggregated tiles of feature values.
  * These pre-aggregated tiles can be used in the serving layer to compute the final feature values along
  * with batch pre-aggregates produced by GroupByUploads.
  * The pre-aggregated tiles are serialized as Avro and indicate whether the tile is complete or not (partial aggregates)
  */
class TileCodec(groupBy: GroupBy, inputSchema: Seq[(String, DataType)]) {

  import TileCodec._
  val rowAggregator: RowAggregator = buildRowAggregator(groupBy, inputSchema)
  val windowedRowAggregator: RowAggregator = buildWindowedRowAggregator(groupBy, inputSchema)

  val windowedIrSchema: StructType = StructType.from("WindowedIr", rowAggregator.irSchema)
  val fields: Array[(String, DataType)] = Array(
    "collapsedIr" -> windowedIrSchema,
    "isComplete" -> BooleanType
  )

  val tileChrononSchema: StructType =
    StructType.from(s"${groupBy.metaData.cleanName}_TILE_IR", fields)
  val tileAvroSchema: String = AvroConversions.fromChrononSchema(tileChrononSchema).toString()
  private val irToBytesFn = AvroConversions.encodeBytes(tileChrononSchema, null)

  def makeTileIr(ir: Array[Any], isComplete: Boolean): Array[Byte] = {
    val normalizedIR = rowAggregator.normalize(ir)
    val tileIr: Array[Any] = Array(normalizedIR, Boolean.box(isComplete))
    irToBytesFn(tileIr)
  }

  @transient private lazy val rowConverter = AvroConversions.genericRecordToChrononRowConverter(windowedIrSchema)

  def decodeTileIr(tileIr: Array[Byte]): (Array[Any], Boolean) = {
    val tileAvroCodec: AvroCodec = AvroCodec.of(tileAvroSchema)
    val decodedTileIr = tileAvroCodec.decode(tileIr)
    val collapsedIr = decodedTileIr
      .get("collapsedIr")
      .asInstanceOf[GenericData.Record]

    val ir = rowConverter(collapsedIr)
    val denormalizedIr = rowAggregator.denormalize(ir)
    val expandedWindowedIr = expandWindowedTileIr(denormalizedIr)
    val isComplete = decodedTileIr.get("isComplete").asInstanceOf[Boolean]
    (expandedWindowedIr, isComplete)
  }

  // cache these mapping out of hot-path
  private case class ExpanderMapping(irPos: Int, bucketPos: Int)

  private val expanderMappings: Array[ExpanderMapping] = {
    val mappingsBuffer = mutable.ArrayBuffer.empty[ExpanderMapping]
    var irPos = 0
    var bucketPos = 0
    groupBy.aggregations.asScala.foreach { aggr =>
      val buckets = Option(aggr.buckets)
        .map(_.toScala)
        .getOrElse(Seq(null))
      val windows = Option(aggr.windows)
        .map(_.toScala)
        .getOrElse(Seq(WindowUtils.Unbounded))
      // for each aggregation we have 1/more buckets and 1/more windows
      // we need to iterate over the baseIr and clone a given counter's values n times where
      // n is the number of windows for that counter
      for (_ <- buckets) {
        for (_ <- windows) {
          mappingsBuffer.append(ExpanderMapping(irPos, bucketPos))
          irPos += 1
        }
        bucketPos += 1
      }
    }
    mappingsBuffer.toArray
  }

  // method that takes a tile IR in the unwindowed form and expands it to the windowed form
  // as an example: [myfield_sum, myfield_average] -> [myfield_sum_1d, myfield_sum_7d, myfield_average_1d, myfield_average_7d]
  def expandWindowedTileIr(baseIr: Array[Any]): Array[Any] = {
    val flattenedIr = windowedRowAggregator.init

    expanderMappings.foreach { case ExpanderMapping(irPos, bucketPos) =>
      flattenedIr(irPos) = rowAggregator.columnAggregators(bucketPos).clone(baseIr(bucketPos))
    }

    flattenedIr
  }
}
