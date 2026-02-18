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

package ai.chronon.spark.stats

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.row.StatsGenerator
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.online.serde.{SparkConversions, AvroConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.GenericRowHandler
import ai.chronon.spark.catalog.TableUtils
import org.apache.datasketches.kll.KllFloatsSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.{Column, DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.util.Try

class StatsCompute(inputDf: DataFrame, keys: Seq[String], name: String) extends Serializable {

  protected val noKeysDf: DataFrame = inputDf.select(
    inputDf.columns
      .filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
  implicit val tableUtils: TableUtils = TableUtils(inputDf.sparkSession)

  val timeColumns: Seq[String] =
    if (inputDf.columns.contains(api.Constants.TimeColumn)) Seq(api.Constants.TimeColumn, tableUtils.partitionColumn)
    else Seq(tableUtils.partitionColumn)
  val metrics: Seq[StatsGenerator.MetricTransform] =
    StatsGenerator.buildMetrics(SparkConversions.toChrononSchema(noKeysDf.schema))
  lazy val selectedDf: DataFrame = noKeysDf
    .select(
      timeColumns.map(col).toSeq ++ metrics
        .map(m =>
          m.expression match {
            case StatsGenerator.InputTransform.IsNull      => functions.col(m.name).isNull
            case StatsGenerator.InputTransform.IsZero      => functions.col(m.name) === 0
            case StatsGenerator.InputTransform.Raw         => functions.col(m.name)
            case StatsGenerator.InputTransform.RawToString => functions.col(m.name).cast("string")
            case StatsGenerator.InputTransform.One         => functions.lit(true)
          })
        .toSeq: _*)
    .toDF(timeColumns.toSeq ++ metrics.map(m => s"${m.name}${m.suffix}").toSeq: _*)

  /** Given a summary Dataframe that computed the stats. Add derived data (example: null rate, median, etc) */
  def addDerivedMetrics(df: DataFrame, aggregator: RowAggregator): DataFrame = {
    val nullColumns = df.columns.filter(p => p.startsWith(StatsGenerator.nullSuffix))
    val withNullRatesDF = nullColumns.foldLeft(df) { (tmpDf, column) =>
      tmpDf.withColumn(
        s"${StatsGenerator.nullRateSuffix}${column.stripPrefix(StatsGenerator.nullSuffix)}",
        tmpDf.col(column) / tmpDf.col(Seq(StatsGenerator.totalColumn, api.Operation.COUNT).mkString("_"))
      )
    }

    val percentiles = aggregator.aggregationParts.filter(_.operation == api.Operation.APPROX_PERCENTILE)
    val percentileColumns = percentiles.map(_.outputColumnName)
    import org.apache.spark.sql.functions.udf
    val percentileFinalizerUdf = udf((s: Array[Byte]) =>
      Try(
        KllFloatsSketch
          .heapify(Memory.wrap(s))
          .getQuantiles(StatsGenerator.finalizedPercentilesMerged)
          .zip(StatsGenerator.finalizedPercentilesMerged)
          .map(f => f._2.toString -> f._1.toString)
          .toMap).toOption)
    val addedPercentilesDf = percentileColumns.foldLeft(withNullRatesDF) { (tmpDf, column) =>
      tmpDf.withColumn(s"${column}_finalized", percentileFinalizerUdf(col(column)))
    }
    addedPercentilesDf.withTimeBasedColumn(tableUtils.partitionColumn)
  }

  /** Navigate the dataframe and compute statistics partitioned by date stamp
    *
    * Partitioned by day version of the normalized summary. Useful for scheduling a job that computes daily stats.
    * Returns a DataFrame that can be encoded for KvStore upload, fetching and merging, or stored in hive.
    *
    * For entity on the left we use daily partition as the key. For events we bucket by timeBucketMinutes (def. 1 hr)
    * Since the stats are mergeable coarser granularities can be obtained through fetcher merging.
    *
    * TODO: REFACTOR - Consider computing IRs using pure SQL aggregations instead of row-by-row updates.
    *   Current approach uses aggregateByKey which processes rows one at a time via updateWithReturn.
    *   Proposed approach:
    *     1. Use SQL aggregations (COUNT, COUNT IF, SUM, MIN, MAX, etc.) to compute aggregated values
    *     2. Build the normalized IRs for the aggregators after data is aggregated
    *     3. This would leverage Spark SQL's optimizations and avoid row-by-row processing overhead
    *   Benefits: Better performance for large datasets, easier to optimize/debug via SQL plans
    */
  def dailySummary(aggregator: RowAggregator, sample: Double = 1.0, timeBucketMinutes: Long = 60): DataFrame = {
    val keySparkSchema = SparkConversions.fromChrononSchema(api.Constants.StatsKeySchema)
    val valueSparkSchema = SparkConversions.fromChrononSchema(aggregator.irSchema)
    val flatSchema = StructType(keySparkSchema ++ valueSparkSchema :+ StructField(api.Constants.TimeColumn, LongType))
    val flatZSchema = flatSchema.toChrononSchema("Flat")

    val partitionIdx = selectedDf.schema.fieldIndex(tableUtils.partitionColumn)
    val partitionSpec = tableUtils.partitionSpec
    val bucketMs = timeBucketMinutes * 1000 * 60
    val tsIdx =
      if (selectedDf.columns.contains(api.Constants.TimeColumn)) selectedDf.schema.fieldIndex(api.Constants.TimeColumn)
      else -1
    val isTimeBucketed = tsIdx >= 0 && timeBucketMinutes > 0
    val keyName: Any = name

    val statsAgg = new StatsAggregator(aggregator.inputSchema, aggregator.aggregationParts).toColumn.name("agg")
    val tupleEncoder: Encoder[(Long, api.Row)] = Encoders.kryo[(Long, api.Row)]
    val keyEncoder: Encoder[Long] = Encoders.scalaLong
    val chrononRowEncoder: Encoder[api.Row] = Encoders.kryo[api.Row]
    val rowEncoder = ExpressionEncoder(flatSchema)

    selectedDf
      .sample(sample)
      .map { row =>
        val chrononRow = SparkConversions.toChrononRow(row, tsIdx)
        val ts =
          if (isTimeBucketed) (chrononRow.ts / bucketMs) * bucketMs
          else partitionSpec.epochMillis(row.getString(partitionIdx))
        (ts, chrononRow: api.Row)
      }(tupleEncoder)
      .groupByKey(_._1)(keyEncoder)
      .mapValues(_._2)(chrononRowEncoder)
      .agg(statsAgg)
      .map { case (ts: Long, v: Array[Any]) =>
        val keys = Array(keyName)
        val all = new Array[Any](keys.length + v.length + 1)
        System.arraycopy(keys, 0, all, 0, keys.length)
        System.arraycopy(v, 0, all, keys.length, v.length)
        all(all.length - 1) = ts
        SparkConversions.toSparkRow(all, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]: Row
      }(rowEncoder)
      .toDF()
  }

  /** Compute cardinality map for all columns in the DataFrame.
    * Returns a map from column name to approximate unique count.
    */
  def computeCardinalityMap(): Map[String, Long] = {
    import org.apache.spark.sql.functions._
    val cardinalityExprs = noKeysDf.columns.map { colName =>
      approx_count_distinct(col(colName)).as(colName)
    }
    val cardinalityRow = noKeysDf.agg(cardinalityExprs.head, cardinalityExprs.tail: _*).collect().head
    noKeysDf.columns.map { colName =>
      colName -> cardinalityRow.getAs[Long](colName)
    }.toMap
  }
}

class EnhancedStatsCompute(inputDf: DataFrame, keys: Seq[String], name: String, cardinalityThreshold: Int = 100)
    extends StatsCompute(inputDf, keys, name) {

  /** Compute cardinality-aware enhanced metrics */
  lazy val cardinalityMap: Map[String, Long] = computeCardinalityMap()

  lazy val enhancedMetrics: Seq[StatsGenerator.MetricTransform] =
    StatsGenerator.buildEnhancedMetrics(
      SparkConversions.toChrononSchema(noKeysDf.schema),
      cardinalityMap,
      cardinalityThreshold
    )

  lazy val enhancedSelectedDf: DataFrame = {
    // Deduplicate metrics by (name, suffix, expression) to avoid duplicate columns
    // Multiple operations (MAX, MIN, AVG, etc.) can share the same source column
    val uniqueColumnDefs = enhancedMetrics
      .groupBy(m => (m.name, m.suffix, m.expression))
      .map { case ((name, suffix, expression), _) =>
        val colExpr = expression match {
          case StatsGenerator.InputTransform.IsNull      => functions.col(name).isNull
          case StatsGenerator.InputTransform.IsZero      => functions.col(name) === 0
          case StatsGenerator.InputTransform.Raw         => functions.col(name)
          case StatsGenerator.InputTransform.RawToString => functions.col(name).cast("string")
          case StatsGenerator.InputTransform.One         => functions.lit(true)
        }
        (s"$name$suffix", colExpr)
      }
      .toSeq
      .sortBy(_._1) // Sort for deterministic ordering

    noKeysDf
      .select(timeColumns.map(col).toSeq ++ uniqueColumnDefs.map(_._2): _*)
      .toDF(timeColumns.toSeq ++ uniqueColumnDefs.map(_._1): _*)
  }

  /** Enhanced daily summary with cardinality-aware metrics.
    * Generates hourly tiles with comprehensive statistics for numeric and categorical columns.
    *
    * TODO: REFACTOR - Consider computing IRs using pure SQL aggregations instead of row-by-row updates.
    *   Current approach uses aggregateByKey which processes rows one at a time via updateWithReturn.
    *   Proposed approach:
    *     1. Use SQL aggregations (COUNT, COUNT IF, SUM, MIN, MAX, etc.) to compute aggregated values
    *     2. Build the normalized IRs for the aggregators after data is aggregated
    *     3. This would leverage Spark SQL's optimizations and avoid row-by-row processing overhead
    *   Benefits: Better performance for large datasets, easier to optimize/debug via SQL plans
    */
  def enhancedDailySummary(sample: Double = 1.0, timeBucketMinutes: Long = 60): (DataFrame, Map[String, String]) = {
    val selectedSchema = api.StructType.from(name, SparkConversions.toChrononSchema(enhancedSelectedDf.schema))
    val enhancedAggregator = StatsGenerator.buildAggregator(enhancedMetrics, selectedSchema)

    val keySparkSchema = SparkConversions.fromChrononSchema(api.Constants.StatsKeySchema)
    val valueSparkSchema = SparkConversions.fromChrononSchema(enhancedAggregator.irSchema)
    val flatSchema = StructType(keySparkSchema ++ valueSparkSchema :+ StructField(api.Constants.TimeColumn, LongType))
    val flatZSchema = flatSchema.toChrononSchema("Flat")

    val partitionIdx = enhancedSelectedDf.schema.fieldIndex(tableUtils.partitionColumn)
    val partitionSpec = tableUtils.partitionSpec
    val bucketMs = timeBucketMinutes * 1000 * 60
    val tsIdx =
      if (enhancedSelectedDf.columns.contains(api.Constants.TimeColumn))
        enhancedSelectedDf.schema.fieldIndex(api.Constants.TimeColumn)
      else -1
    val isTimeBucketed = tsIdx >= 0 && timeBucketMinutes > 0
    val keyName: Any = name

    val statsAgg =
      new StatsAggregator(enhancedAggregator.inputSchema, enhancedAggregator.aggregationParts).toColumn.name("agg")
    val tupleEncoder: Encoder[(Long, api.Row)] = Encoders.kryo[(Long, api.Row)]
    val keyEncoder: Encoder[Long] = Encoders.scalaLong
    val chrononRowEncoder: Encoder[api.Row] = Encoders.kryo[api.Row]
    val rowEncoder = ExpressionEncoder(flatSchema)

    val flatDf = enhancedSelectedDf
      .sample(sample)
      .map { row =>
        val chrononRow = SparkConversions.toChrononRow(row, tsIdx)
        val ts =
          if (isTimeBucketed) (chrononRow.ts / bucketMs) * bucketMs
          else partitionSpec.epochMillis(row.getString(partitionIdx))
        (ts, chrononRow: api.Row)
      }(tupleEncoder)
      .groupByKey(_._1)(keyEncoder)
      .mapValues(_._2)(chrononRowEncoder)
      .agg(statsAgg)
      .map { case (ts: Long, v: Array[Any]) =>
        val keys = Array(keyName)
        val all = new Array[Any](keys.length + v.length + 1)
        System.arraycopy(keys, 0, all, 0, keys.length)
        System.arraycopy(v, 0, all, keys.length, v.length)
        all(all.length - 1) = ts
        SparkConversions.toSparkRow(all, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]: Row
      }(rowEncoder)
      .toDF()

    // Prepare metadata for reconstructing the aggregator
    enhancedSelectedDf.printSchema()
    val noKeysSchema = api.StructType.from(name, SparkConversions.toChrononSchema(noKeysDf.schema))

    val cardinalityMapJson = cardinalityMap.map { case (k, v) => s""""$k":$v""" }.mkString("{", ",", "}")
    val selectedSchemaJson = AvroConversions.fromChrononSchema(selectedSchema).toString(true)
    val noKeysSchemaJson = AvroConversions.fromChrononSchema(noKeysSchema).toString(true)

    val metadata = Map(
      "cardinalityMap" -> cardinalityMapJson,
      "selectedSchema" -> selectedSchemaJson,
      "noKeysSchema" -> noKeysSchemaJson
    )

    (flatDf, metadata)
  }
}

private[stats] class StatsAggregator(inputSchema: Seq[(String, api.DataType)],
                                     aggregationParts: Seq[api.AggregationPart])
    extends Aggregator[api.Row, Array[Any], Array[Any]]
    with Serializable {

  // RowAggregator contains non-serializable internals (TimedDispatcher) so recreate lazily
  @transient private lazy val rowAggregator = new RowAggregator(inputSchema, aggregationParts)

  override def zero: Array[Any] = rowAggregator.init
  override def reduce(buf: Array[Any], input: api.Row): Array[Any] = rowAggregator.updateWithReturn(buf, input)
  override def merge(b1: Array[Any], b2: Array[Any]): Array[Any] = rowAggregator.merge(b1, b2)
  override def finish(buf: Array[Any]): Array[Any] = rowAggregator.normalize(buf)
  override def bufferEncoder: Encoder[Array[Any]] = Encoders.kryo[Array[Any]]
  override def outputEncoder: Encoder[Array[Any]] = Encoders.kryo[Array[Any]]
}
