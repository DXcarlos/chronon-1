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

package ai.chronon.spark.other

import ai.chronon.aggregator.row.StatsGenerator
import ai.chronon.aggregator.test.Column
import ai.chronon.api._
import ai.chronon.online.serde.SparkConversions.toChrononSchema
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.StatsCompute
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.DataFrameGen
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class StatsComputeTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  lazy val spark: SparkSession = SparkSessionBuilder.build("StatsComputeTest", local = true)
  implicit val tableUtils: TableUtils = TableUtils(spark)
  val namespace: String = "stats_compute_test"

  it should "summary" in {
    val data = Seq(
      ("1", Some(1L), Some(1.0), Some("a")),
      ("1", Some(1L), None, Some("b")),
      ("2", Some(2L), None, None),
      ("3", None, None, Some("d"))
    )
    val columns = Seq("keyId", "value", "double_value", "string_value")
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*).withColumn(tableUtils.partitionColumn, lit("2022-04-09"))
    val stats = new StatsCompute(df, Seq("keyId"), "test")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics, StructType.from("test", toChrononSchema(stats.selectedDf.schema)))
    val result = stats.dailySummary(aggregator).toFlatDf
    stats.addDerivedMetrics(result, aggregator).show()
  }

  it should "snapshot summary" in {
    tableUtils.createDatabase(namespace)
    val data = Seq(
      ("1", Some(1L), Some(1.0), Some("a")),
      ("1", Some(1L), None, Some("b")),
      ("2", Some(2L), None, None),
      ("3", None, None, Some("d"))
    )
    val columns = Seq("keyId", "value", "double_value", "string_value")
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark
      .createDataFrame(rdd)
      .toDF(columns: _*)
      .withColumn(tableUtils.partitionColumn, lit("2022-04-09"))
      .drop(Constants.TimeColumn)
    val stats = new StatsCompute(df, Seq("keyId"), "snapshotTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics, StructType.from("test", toChrononSchema(stats.selectedDf.schema)))
    val result = stats.dailySummary(aggregator).toFlatDf
    stats.addDerivedMetrics(result, aggregator).save(s"$namespace.testTablenameSnapshot")
  }

  it should "generated summary" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen.events(spark, schema, 100000, 10)
    val stats = new StatsCompute(df, Seq("user"), "generatedTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics,
                                     StructType.from("generatedTest", toChrononSchema(stats.selectedDf.schema)))
    val daily = stats.dailySummary(aggregator, timeBucketMinutes = 0).toFlatDf

    logger.info("Daily Stats")
    daily.show()
    val bucketed = stats
      .dailySummary(aggregator)
      .toFlatDf
      .replaceWithReadableTime(Seq(Constants.TimeColumn), false)

    logger.info("Bucketed Stats")
    bucketed.show()

    val denormalized = stats.addDerivedMetrics(bucketed, aggregator)
    logger.info("With Derived Data")
    denormalized.show(truncate = false)
  }

  it should "generated summary no ts" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen
      .events(spark, schema, 100000, 10)
      .drop(Constants.TimeColumn)
    val stats = new StatsCompute(df, Seq("user"), "noTsTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics,
                                     StructType.from("noTsTest", toChrononSchema(stats.selectedDf.schema)))
    val daily = stats.dailySummary(aggregator, timeBucketMinutes = 0).toFlatDf

    logger.info("Daily Stats")
    daily.show()
    val bucketed = stats.dailySummary(aggregator).toFlatDf

    logger.info("Bucketed Stats")
    bucketed.show()

    val denormalized = stats.addDerivedMetrics(bucketed, aggregator)
    logger.info("With Derived Data")
    denormalized.show(truncate = false)
  }

  /** Test to make sure aggregations are generated when it makes sense.
    * Example, percentiles are not currently supported for byte.
    */
  it should "generated summary byte" in {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val byteSample = 1.toByte
    val df = DataFrameGen
      .events(spark, schema, 100000, 10)
      .withColumn("byte_column", lit(byteSample))
    val stats = new StatsCompute(df, Seq("user"), "byteTest")
    val aggregator =
      StatsGenerator.buildAggregator(stats.metrics,
                                     StructType.from("byteTest", toChrononSchema(stats.selectedDf.schema)))
    val daily = stats.dailySummary(aggregator, timeBucketMinutes = 0).toFlatDf

    logger.info("Daily Stats")
    daily.show()
    val bucketed = stats
      .dailySummary(aggregator)
      .toFlatDf
      .replaceWithReadableTime(Seq(Constants.TimeColumn), false)

    logger.info("Bucketed Stats")
    bucketed.show()

    val denormalized = stats.addDerivedMetrics(bucketed, aggregator)
    logger.info("With Derived Data")
    denormalized.show(truncate = false)
  }
}
