package ai.chronon.spark.join

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.{Column, NaiveAggregator, Timer, TestRow => TRow}
import ai.chronon.aggregator.windowing.FiveMinuteResolution
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api._
import ai.chronon.spark.join.{AggregationInfo, CGenericRow, SawtoothUdf, UnionJoin}
import ai.chronon.spark.utils.DataFrameGen
import org.apache.spark.sql.{DataFrame, types}
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConverters._

/** Performance test for SawtoothUdf that compares against a naive implementation
  * Uses DataFrameGen to generate large test datasets and focuses on LAST_K aggregation
  */
class SawtoothUdfPerformanceTest extends BaseJoinTest with Matchers {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  "SawtoothUdf.sawtoothAggregate" should "correctly compute LAST_K with large dataset" in {
    // Parameters for the test
    val numItems = 20000 // Number of items/events
    val numQueries = 20000 // Number of queries
    val k = 50 // The K value for LAST_K aggregation

    val timer = new Timer

    // Generate test data for right side (items with values and timestamps)
    // We'll sort these by timestamp to ensure they're in chronological order
    logger.info("Generating test data...")
    timer.start()

    // Create columns for the right dataframe (items)
    val rightColumns = Seq(
      Column("user_id", IntType, 1), // 10 different user IDs
      Column("item_id", StringType, 1000), // 1000 different item IDs
      Column("value", DoubleType, 1000), // Values for the items
      Column(Constants.TimeColumn, LongType, 180) // Spread over 180 days
    )

    // Generate right dataframe
    val rightDf = DataFrameGen
      .gen(spark, rightColumns, numItems)
      .dropDuplicates(Constants.TimeColumn)
      .orderBy(Constants.TimeColumn) // Sort by timestamp
      .cache()

    // Convert to SparkRows for the test
    val rightRows = rightDf.collect()

    // Generate left dataframe (queries at specific timestamps)
    val leftColumns = Seq(
      Column("user_id", IntType, 1), // Same user IDs as right
      Column("query", StringType, 100), // Different query strings
      Column(Constants.TimeColumn, LongType, 30) // Queries in the last 30 days
    )

    // For queries, we want them to be later than most items to test the time window
    val leftDf = DataFrameGen
      .gen(spark, leftColumns, numQueries)
      .orderBy(Constants.TimeColumn) // Sort by timestamp
      .cache()

    // Convert to SparkRows for the test
    val leftRows = leftDf.collect()

    timer.publish("Data generation")

    // Define schemas for test
    val leftSchema = leftDf.schema
    val rightSchema = rightDf.schema

    // Define a LAST_K aggregation operation
    val aggs: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        operation = Operation.LAST_K,
        inputColumn = "item_id",
        windows = Seq(new Window(7, TimeUnit.DAYS)), // Last 7 days
        argMap = Map("k" -> s"$k")
      )
    )

    // Create GroupBy definition
    val groupBy = Builders.GroupBy(
      sources = Seq(),
      keyColumns = Seq("user_id"),
      aggregations = aggs,
      metaData = null,
      accuracy = null
    )

    // Create the aggregators
    val minQueryTs = leftRows.minBy(_.getLong(2)).getLong(2)
    val aggregationInfo = AggregationInfo.from(
      groupBy = groupBy,
      minQueryTs = minQueryTs,
      leftSchema = types.StructType(leftSchema),
      rightSchema = types.StructType(rightSchema),
      resolution = FiveMinuteResolution
    )

    // Call the sawtoothAggregate function
    logger.info("Running SawtoothUdf.sawtoothAggregate...")
    timer.start()
    val result = SawtoothUdf.sawtoothAggregate(aggregationInfo)(leftRows, rightRows)
    timer.publish("SawtoothUdf.sawtoothAggregate")

    // Create a naive implementation to compare against
    logger.info("Running naive implementation...")
    timer.start()

    // Convert DataFrames to TestRows for naive implementation
    val rightTestRows = convertToTestRows(rightDf)
    val leftTimestamps = leftDf.select(Constants.TimeColumn).collect().map(_.getLong(0))

    // Create a naive aggregator
    val specs = aggs.map(_.unpack.head)
    val rowAggregator = new RowAggregator(
      rightSchema.map(f => f.name -> mapSparkTypeToChronon(f.dataType)),
      specs
    )

    val windows = specs.map(_.window).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      rowAggregator,
      windows,
      tailHops
    )

    // Run the naive aggregator
    val naiveResults = naiveAggregator.aggregate(rightTestRows, leftTimestamps)
    timer.publish("Naive implementation")

    // Verify the results
    logger.info("Verifying results...")
    timer.start()

    // Check that we have the right number of results
    result.size shouldBe leftRows.size

    // Function to extract the LAST_K items from a result
    def extractLastKItems(row: CGenericRow): Array[_] = {
      // Assuming LAST_K result is in column 3 (after user_id, query, ts)
      row.get(3).asInstanceOf[Array[_]]
    }

    // Convert naive results to comparable format
    val naiveItemLists = naiveResults.map { ir =>
      rowAggregator.finalize(ir)(0).asInstanceOf[util.List[_]]
    }

    // Sample some results to verify (checking all would be too verbose)
//    val sampleIndices = (0 until math.min(10, result.size)).toList

    for (i <- result.indices) {
      val sawtoothItems = extractLastKItems(result(i))
      val naiveItems = naiveItemLists(i)

      val naiveSize = Option(naiveItems).map(_.size).getOrElse(0)
      val sawtoothSize = Option(sawtoothItems).map(_.size).getOrElse(0)

      // Verify size
      sawtoothSize shouldBe naiveSize

      // Both should be limited to k or less
      sawtoothItems.size should be <= k

      val computedStr = sawtoothItems.mkString(", ")
      val expectedStr = naiveItems.asScala.mkString(", ")

      if (computedStr != expectedStr) {
        println("--")
        println(computedStr)
        println(expectedStr)
        print("--")
      }

      computedStr shouldEqual expectedStr
    }

    timer.publish("Result verification")

    // Verify performance difference
    logger.info("Test completed successfully!")
  }

  // Helper method to convert a DataFrame to TestRows
  private def convertToTestRows(df: DataFrame): Array[TRow] = {
    val tsIndex = df.schema.indexWhere(_.name == Constants.TimeColumn)

    df.collect().map { row =>
      val values = row.toSeq.toArray
      new TRow(values: _*)(tsIndex)
    }
  }

  // Helper method to map Spark SQL types to Chronon types
  private def mapSparkTypeToChronon(dataType: types.DataType): DataType = {
    dataType match {
      case _: types.IntegerType => IntType
      case _: types.LongType    => LongType
      case _: types.DoubleType  => DoubleType
      case _: types.StringType  => StringType
      case _                    => throw new IllegalArgumentException(s"Unsupported type: $dataType")
    }
  }
}
