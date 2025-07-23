package ai.chronon.spark.join

import ai.chronon.aggregator.windowing.{FiveMinuteResolution, HopsAggregator, Resolution, SawtoothAggregator}
import ai.chronon.api
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.{Constants, TsUtils}
import ai.chronon.online.serde.{RowWrapper, SparkConversions}
import ai.chronon.spark.join.SawtoothUdf.sawtoothAggregate
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row => SparkRow, types => spark}

import java.util
import scala.collection.mutable

class CGenericRow(val values: Array[Any]) extends SparkRow {

  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def toSeq: Seq[Any] = values.clone()

  override def copy(): CGenericRow = this
}

case class AggregationInfo(hopsAggregator: HopsAggregator,
                           sawtoothAggregator: SawtoothAggregator,
                           leftTimeIndex: Int,
                           rightTimeIndex: Int,
                           leftSchema: spark.StructType,
                           aggregateChrononSchema: api.StructType,
                           outputSparkSchema: spark.StructType,
                           resolution: Resolution) {

  type Rows = mutable.WrappedArray[SparkRow]

  def aggregate(leftRows: Rows, rightRows: Rows): Array[CGenericRow] = {
    sawtoothAggregate(this)(leftRows, rightRows)
  }

  val minResolution: Long = resolution.hopSizes.min

  @inline
  def leftBucketer(r: SparkRow): Long = {
    TsUtils.round(r.getLong(leftTimeIndex), minResolution)
  }

  @inline
  def rightBucketer(r: SparkRow): Long = {
    TsUtils.round(r.getLong(rightTimeIndex), minResolution)
  }

  @inline
  def leftRowWrapper(r: SparkRow): RowWrapper = {
    new RowWrapper(r, leftTimeIndex)
  }

  @inline
  def rightRowWrapper(r: SparkRow): RowWrapper = {
    new RowWrapper(r, rightTimeIndex)
  }
}

object AggregationInfo {
  def from(groupBy: api.GroupBy,
           minQueryTs: Long,
           leftSchema: spark.StructType,
           rightSchema: spark.StructType,
           resolution: Resolution = FiveMinuteResolution): AggregationInfo = {

    val specs = groupBy.aggregations.toScala.toArray
    val schema = SparkConversions.toChrononSchema(rightSchema)

    val hopsAggregator =
      new HopsAggregator(minQueryTs, specs, schema, resolution)
    val sawtoothAggregator =
      new SawtoothAggregator(specs, schema, resolution)

    val rightTimeIndex = rightSchema.indexWhere(_.name == Constants.TimeColumn)
    val leftTimeIndex = leftSchema.indexWhere(_.name == Constants.TimeColumn)

    val aggregateSchema: api.StructType = api.StructType("",
                                                         sawtoothAggregator.windowedAggregator.outputSchema
                                                           .map { case (name, typ) =>
                                                             api.StructField(name, typ)
                                                           })

    val aggregateSparkSchema: spark.StructType = SparkConversions.fromChrononSchema(aggregateSchema)

    val outputSparkSchema = spark.StructType(leftSchema ++ aggregateSparkSchema)

    AggregationInfo(
      hopsAggregator = hopsAggregator,
      sawtoothAggregator = sawtoothAggregator,
      leftTimeIndex = leftTimeIndex,
      rightTimeIndex = rightTimeIndex,
      leftSchema = leftSchema,
      aggregateChrononSchema = aggregateSchema,
      outputSparkSchema = outputSparkSchema,
      resolution = resolution
    )
  }
}
