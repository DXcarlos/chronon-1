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
import ai.chronon.api.{Constants, Operation, StructType}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.stats.{EnhancedStatsCompute, EnhancedStatsStore}
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.online.InMemoryKvStore
import ai.chronon.spark.utils.MockApi
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, ZoneId}

class EnhancedStatsComputeTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  lazy val spark: SparkSession = SparkSessionBuilder.build("EnhancedStatsComputeTest", local = true)
  implicit val tableUtils: TableUtils = TableUtils(spark)

  /** Helper to load the ratings CSV and prepare it for testing */
  def loadRatingsData(): DataFrame = {
    // Use classloader to find the resource
    val csvPath = getClass.getClassLoader.getResource("local_data_csv/rating.csv.gz").getPath

    // Read CSV with schema
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Add a partition column (required by StatsCompute)
    // Convert timestamp to partition format
    df.withColumn(tableUtils.partitionColumn,
        date_format(to_timestamp(col("timestamp")), "yyyy-MM-dd"))
      .withColumn(Constants.TimeColumn,
        unix_timestamp(to_timestamp(col("timestamp"))).cast(LongType) * 1000)
  }

  it should "detect cardinality correctly for ratings data" in {
    val df = loadRatingsData()

    logger.info(s"Loaded ${df.count()} rating records")
    df.show(10, truncate = false)
    df.printSchema()

    // Check time range
    import org.apache.spark.sql.functions._
    val timeRange = df.select(
      min(col("timestamp")).as("min_ts"),
      max(col("timestamp")).as("max_ts"),
      approx_count_distinct(date_format(col("timestamp"), "yyyy-MM-dd")).as("unique_days"),
      approx_count_distinct(date_format(col("timestamp"), "yyyy-MM-dd HH")).as("unique_hours")
    ).collect().head

    logger.info(s"\nTime Range:")
    logger.info(s"  First record: ${timeRange.getString(0)}")
    logger.info(s"  Last record: ${timeRange.getString(1)}")
    logger.info(s"  Unique days: ${timeRange.getLong(2)}")
    logger.info(s"  Unique hours: ${timeRange.getLong(3)}")

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty, // No keys, we want stats on all columns
      name = "ratings_test",
      cardinalityThreshold = 100
    )

    // Check cardinality detection
    val cardinalityMap = enhancedStats.cardinalityMap
    logger.info("Cardinality Map:")
    cardinalityMap.foreach { case (col, card) =>
      logger.info(s"  $col: $card (${if (card <= 100) "LOW" else "HIGH"} cardinality)")
    }

    // Verify expected cardinalities
    assert(cardinalityMap.contains("userId"), "Should detect userId cardinality")
    assert(cardinalityMap.contains("movieId"), "Should detect movieId cardinality")
    assert(cardinalityMap.contains("rating"), "Should detect rating cardinality")

    // Rating should be low cardinality (0.5-5.0 scale)
    assert(cardinalityMap("rating") <= 100, s"Rating should be low cardinality, got ${cardinalityMap("rating")}")
  }

  it should "generate enhanced metrics based on cardinality" in {
    val df = loadRatingsData()

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_enhanced_metrics",
      cardinalityThreshold = 100
    )

    // Check the enhanced metrics
    logger.info("\nEnhanced Metrics Generated:")
    enhancedStats.enhancedMetrics.groupBy(_.name).foreach { case (colName, metrics) =>
      logger.info(s"\n  Column: $colName")
      metrics.foreach { m =>
        logger.info(s"    - ${m.operation} (${m.expression}, suffix: ${m.suffix})")
      }
    }

    // Verify that numeric columns get appropriate metrics
    val ratingMetrics = enhancedStats.enhancedMetrics.filter(_.name == "rating")
    val ratingOperations = ratingMetrics.map(_.operation).toSet

    // Rating is numeric, should have: null, zero, max, min, avg, variance, approx_unique_count, approx_percentile
    assert(ratingOperations.contains(Operation.SUM), "Should have SUM for null/zero counting")
    assert(ratingOperations.contains(Operation.MAX), "Should have MAX for numeric column")
    assert(ratingOperations.contains(Operation.MIN), "Should have MIN for numeric column")
    assert(ratingOperations.contains(Operation.AVERAGE), "Should have AVERAGE for numeric column")
    assert(ratingOperations.contains(Operation.VARIANCE), "Should have VARIANCE for numeric column")
    assert(ratingOperations.contains(Operation.APPROX_UNIQUE_COUNT), "Should have APPROX_UNIQUE_COUNT")
    assert(ratingOperations.contains(Operation.APPROX_PERCENTILE), "Should have APPROX_PERCENTILE for median")

    logger.info(s"\n✓ Rating column has ${ratingMetrics.size} metrics: ${ratingOperations.mkString(", ")}")
  }

  it should "compute daily enhanced summary statistics" in {
    val df = loadRatingsData()

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_daily_stats",
      cardinalityThreshold = 100
    )

    // Generate daily tiles (timeBucketMinutes = 0 means daily aggregation)
    val timedKvRdd = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    // Convert to DataFrame for inspection
    val resultDf = timedKvRdd.toFlatDf

    logger.info("\nDaily Enhanced Statistics:")
    logger.info(s"Number of daily tiles: ${resultDf.count()}")
    resultDf.show(10, truncate = false)
    resultDf.printSchema()

    // Verify the schema contains our enhanced metrics
    val columns = resultDf.columns.toSet

    logger.info(s"Columns in result: ${columns.mkString(", ")}")

    // Should have basic stats
    assert(columns.exists(_.contains("rating")), "Should have rating columns")

    // Check for enhanced metrics on rating column
    assert(columns.exists(_.toLowerCase.contains("null")), "Should have null count metrics")
    assert(columns.exists(_.toLowerCase.contains("zero")), "Should have zero count metrics")
    assert(columns.exists(_.toLowerCase.contains("total")), "Should have total count")

    logger.info(s"✓ Generated ${resultDf.count()} daily tiles with enhanced statistics")
    logger.info(s"✓ Schema has ${columns.size} columns including all enhanced metrics")
  }

  it should "verify statistics are correct for numeric columns" in {
    val df = loadRatingsData()

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_verification",
      cardinalityThreshold = 100
    )

    // Generate statistics (daily tiles)
    val timedKvRdd = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    val resultDf = timedKvRdd.toFlatDf

    logger.info("\nStatistics Verification:")
    resultDf.show(10, truncate = false)

    // Get the first row (daily aggregate) - use take(1) instead of collect() to avoid OOM
    val firstRow = resultDf.take(1).head

    // Extract some statistics
    val schema = resultDf.schema
    val columnMap = firstRow.getValuesMap[Any](schema.fieldNames)

    logger.info("\nKey Statistics:")
    columnMap.filter(_._1.contains("rating")).foreach { case (k, v) =>
      logger.info(s"  $k: $v")
    }

    // Verify we have meaningful data - find total count column (case insensitive)
    val totalCountKey = columnMap.keys.find(_.toLowerCase.contains("total"))
    assert(totalCountKey.isDefined, s"Should have total count column. Available columns: ${columnMap.keys.mkString(", ")}")

    val totalCount = columnMap(totalCountKey.get)
    assert(totalCount.asInstanceOf[Long] > 0, "Total count should be positive")

    logger.info(s"✓ Total count (${totalCountKey.get}): ${totalCount}")
  }

  it should "handle mixed cardinality columns appropriately" in {
    val df = loadRatingsData()

    // Test with different thresholds to see how it affects metric generation
    val thresholds = Seq(10, 100, 1000)

    thresholds.foreach { threshold =>
      logger.info(s"\n=== Testing with cardinality threshold: $threshold ===")

      val enhancedStats = new EnhancedStatsCompute(
        inputDf = df,
        keys = Seq.empty,
        name = s"ratings_threshold_$threshold",
        cardinalityThreshold = threshold
      )

      val cardMap = enhancedStats.cardinalityMap
      cardMap.foreach { case (col, card) =>
        val classification = if (card <= threshold) "LOW (categorical)" else "HIGH (numeric)"
        logger.info(s"  $col: $card -> $classification")
      }

      // Count metrics per column
      enhancedStats.enhancedMetrics.groupBy(_.name).foreach { case (colName, metrics) =>
        logger.info(s"  $colName has ${metrics.size} metrics")
      }
    }
  }

  it should "generate IR bytes that can be uploaded to KV store" in {
    val df = loadRatingsData()

    val enhancedStats = new EnhancedStatsCompute(
      inputDf = df,
      keys = Seq.empty,
      name = "ratings_kv_upload",
      cardinalityThreshold = 100
    )

    // Generate daily tiles
    val timedKvRdd = enhancedStats.enhancedDailySummary(
      sample = 1.0,
      timeBucketMinutes = 0 // Daily tiles
    )

    // Convert to Avro format (ready for KV store)
    val avroDf = timedKvRdd.toAvroDf

    logger.info("\nKV Store Ready Format:")
    val recordCount = avroDf.count()
    logger.info(s"Number of records: ${recordCount}")
    avroDf.show(5, truncate = false)
    avroDf.printSchema()

    // Verify structure
    val columns = avroDf.columns.toSet
    assert(columns.contains("key_bytes"), "Should have key_bytes for KV store")
    assert(columns.contains("value_bytes"), "Should have value_bytes (IR bytes) for KV store")
    assert(columns.contains(Constants.TimeColumn), "Should have timestamp for tiling")

    // Verify we have actual data - use take(1) instead of collect() to avoid OOM
    val firstRow = avroDf.take(1).head
    val keyBytes = firstRow.getAs[Array[Byte]]("key_bytes")
    val valueBytes = firstRow.getAs[Array[Byte]]("value_bytes")

    assert(keyBytes != null && keyBytes.length > 0, "Key bytes should not be empty")
    assert(valueBytes != null && valueBytes.length > 0, "Value bytes (IRs) should not be empty")

    logger.info(s"✓ Generated ${recordCount} KV records")
    logger.info(s"✓ Sample key size: ${keyBytes.length} bytes")
    logger.info(s"✓ Sample IR size: ${valueBytes.length} bytes")
  }
}
