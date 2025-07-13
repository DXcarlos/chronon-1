package ai.chronon.spark.test.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api.{
  StringType,
  LongType,
  DoubleType,
  BooleanType,
  IntType,
  DateRange,
  MetaData,
  TimeUnit,
  Window,
  Operation,
  Builders,
  JoinPart
}
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.sql.types.{
  StructField => SparkStructField,
  StructType => SparkStructType,
  StringType => SparkStringType,
  LongType => SparkLongType,
  DoubleType => SparkDoubleType
}
import scala.collection.Seq
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.planner.{JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.{JoinPartJob, MergeJob, SourceJob}
import ai.chronon.spark.test.{DataFrameGen, TableTestUtils}
import ai.chronon.spark.{Join, JoinUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

class MergeJobAnalyzeReuseTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  val spark: SparkSession = submission.SparkSessionBuilder.build("MergeJobAnalyzeReuseTest", local = true)
  private implicit val tableUtils: TableTestUtils = TableTestUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  private val namespace = "test_namespace_analyze_reuse"
  tableUtils.createDatabase(namespace)

  it should "correctly identify reusable join parts when production table has matching columns" in {
    val testName = "analyze_reuse_matching_columns"

    // Create test data tables
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("item", StringType, 10)
    )

    val groupBySchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100),
      Column("quantity", LongType, 100)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val rightTable = s"$namespace.${testName}_right"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $rightTable")

    DataFrameGen.events(spark, leftSchema, 100, 30).save(leftTable)
    DataFrameGen.events(spark, groupBySchema, 100, 30).save(rightTable)

    // Create GroupBys
    val priceGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = rightTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.price", namespace = namespace, team = "test_team")
    )

    val quantityGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = rightTable,
          query = Builders.Query(selects = Builders.Selects("quantity"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.COUNT, inputColumn = "quantity")),
      metaData = Builders.MetaData(name = s"$testName.quantity", namespace = namespace, team = "test_team")
    )

    // Create join parts
    // One with a prefix
    val priceJoinPart = Builders.JoinPart(groupBy = priceGroupBy, prefix = "price").setUseLongNames(false)
    // One without
    val quantityJoinPart = Builders.JoinPart(groupBy = quantityGroupBy).setUseLongNames(false)

    // Create production join (current setup)
    val productionJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPart, quantityJoinPart),
        metaData = Builders.MetaData(name = testName + "_prod", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    // Create new join (same as production for this test)
    val newJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPart, quantityJoinPart),
        metaData = Builders.MetaData(name = testName + "_new", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(monthAgo).setEndDate(monthAgo)

    // Create production table with expected schema
    val productionTable = productionJoin.metaData.outputTable
    spark.sql(s"DROP TABLE IF EXISTS $productionTable")

    // Create production table schema that includes all expected columns
    val productionSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("item", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("price_user_price_sum", SparkDoubleType),
        SparkStructField("user_quantity_count", SparkLongType)
      ))

    val productionData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", "item1", 1000L, monthAgo, 100.0, 5L),
          SparkRow("user2", "item2", 2000L, monthAgo, 200.0, 10L)
        )),
      productionSchema
    )

    productionData.save(productionTable)

    // Create join part tables that match the expected output
    val pricePartTable = RelevantLeftForJoinPart.fullPartTableName(newJoin, priceJoinPart)
    val quantityPartTable = RelevantLeftForJoinPart.fullPartTableName(newJoin, quantityJoinPart)

    spark.sql(s"DROP TABLE IF EXISTS $pricePartTable")
    spark.sql(s"DROP TABLE IF EXISTS $quantityPartTable")

    // Create part table schema
    val partTableSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("price_sum", SparkDoubleType)
      ))

    val quantityPartTableSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("quantity_count", SparkLongType)
      ))

    val pricePartData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", 1000L, monthAgo, 100.0),
          SparkRow("user2", 2000L, monthAgo, 200.0)
        )),
      partTableSchema
    )

    val quantityPartData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", 1000L, monthAgo, 5L),
          SparkRow("user2", 2000L, monthAgo, 10L)
        )),
      quantityPartTableSchema
    )

    pricePartData.write.mode(SaveMode.Overwrite).saveAsTable(pricePartTable)
    quantityPartData.write.mode(SaveMode.Overwrite).saveAsTable(quantityPartTable)

    // Create left DataFrame for schema compatibility check
    val leftDf = DataFrameGen.events(spark, leftSchema, 10, 1)

    // Create MergeJob with production join reference
    val mergeNode = new JoinMergeNode()
      .setJoin(newJoin)
      .setProductionJoin(productionJoin)

    val mergeMetaData = new MetaData()
      .setName(newJoin.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(priceJoinPart, quantityJoinPart))

    // Test the analyzeJoinPartsForReuse method directly
    val (selectFromProdTableAsLeft: Seq[String], joinPartsToQueryAndJoin: Seq[JoinPart]) =
      mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results
    println(s"Columns to select from production: ${selectFromProdTableAsLeft.mkString(", ")}")
    println(s"Join parts to compute: ${joinPartsToQueryAndJoin.map(_.groupBy.metaData.name).mkString(", ")}")

    // Both join parts should be reusable since production table contains all expected columns
    assertEquals("Should reuse both column sets from production table", 2, selectFromProdTableAsLeft.length)
    assertTrue("Should include price columns", selectFromProdTableAsLeft.contains("price_user_price_sum"))
    assertTrue("Should include quantity columns", selectFromProdTableAsLeft.contains("user_quantity_count"))

    // No join parts should need to be computed
    assertEquals("No join parts should need computation", 0, joinPartsToQueryAndJoin.length)
  }

  it should "correctly identify non-reusable join parts when production table is missing columns" in {
    val testName = "analyze_reuse_missing_columns"

    // Create test data tables
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("item", StringType, 10)
    )

    val groupBySchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100),
      Column("rating", DoubleType, 5)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val rightTable = s"$namespace.${testName}_right"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $rightTable")

    DataFrameGen.events(spark, leftSchema, 100, 30).save(leftTable)
    DataFrameGen.events(spark, groupBySchema, 100, 30).save(rightTable)

    // Create GroupBys
    val priceGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = rightTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.price", namespace = namespace, team = "test_team")
    )

    val ratingGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = rightTable,
          query = Builders.Query(selects = Builders.Selects("rating"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "rating")),
      metaData = Builders.MetaData(name = s"$testName.rating", namespace = namespace, team = "test_team")
    )

    // Create join parts
    val priceJoinPart = Builders.JoinPart(groupBy = priceGroupBy).setUseLongNames(false)
    val ratingJoinPart = Builders.JoinPart(groupBy = ratingGroupBy).setUseLongNames(false)

    // Create production join (only has price)
    val productionJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPart), // Only price, no rating
        metaData = Builders.MetaData(name = testName + "_prod", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    // Create new join (has both price and rating)
    val newJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPart, ratingJoinPart), // Both price and rating
        metaData = Builders.MetaData(name = testName + "_new", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(monthAgo).setEndDate(monthAgo)

    // Create production table with only price columns (missing rating)
    val productionTable = productionJoin.metaData.outputTable
    spark.sql(s"DROP TABLE IF EXISTS $productionTable")

    val productionSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("item", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("user_price_sum", SparkDoubleType),
        SparkStructField("user_ratings_avg", SparkDoubleType)
        // Note: missing rating_user_rating_average
      ))

    val productionData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", "item1", 1000L, monthAgo, 100.0, 4.5),
          SparkRow("user2", "item2", 2000L, monthAgo, 200.0, 3.8)
        )),
      productionSchema
    )

    productionData.save(productionTable)

    // Create join part tables
    val pricePartTable = RelevantLeftForJoinPart.fullPartTableName(newJoin, priceJoinPart)
    val ratingPartTable = RelevantLeftForJoinPart.fullPartTableName(newJoin, ratingJoinPart)

    spark.sql(s"DROP TABLE IF EXISTS $pricePartTable")
    spark.sql(s"DROP TABLE IF EXISTS $ratingPartTable")

    val pricePartTableSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("price_sum", SparkDoubleType)
      ))

    val ratingPartTableSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("rating_average", SparkDoubleType)
      ))

    val pricePartData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", 1000L, monthAgo, 100.0),
          SparkRow("user2", 2000L, monthAgo, 200.0)
        )),
      pricePartTableSchema
    )

    val ratingPartData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", 1000L, monthAgo, 4.5),
          SparkRow("user2", 2000L, monthAgo, 3.8)
        )),
      ratingPartTableSchema
    )

    pricePartData.write.mode(SaveMode.Overwrite).saveAsTable(pricePartTable)
    ratingPartData.write.mode(SaveMode.Overwrite).saveAsTable(ratingPartTable)

    // Create left DataFrame for schema compatibility check
    val leftDf = DataFrameGen.events(spark, leftSchema, 10, 1)

    // Create MergeJob with production join reference
    val mergeNode = new JoinMergeNode()
      .setJoin(newJoin)
      .setProductionJoin(productionJoin)

    val mergeMetaData = new MetaData()
      .setName(newJoin.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(priceJoinPart, ratingJoinPart))

    // Test the analyzeJoinPartsForReuse method directly
    val (selectFromProdTableAsLeft: Seq[String], joinPartsToQueryAndJoin: Seq[JoinPart]) =
      mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results
    println(s"Columns to select from production: ${selectFromProdTableAsLeft.mkString(", ")}")
    println(s"Join parts to compute: ${joinPartsToQueryAndJoin.map(_.groupBy.metaData.name).mkString(", ")}")

    // Only price should be reusable, rating should need computation
    assertEquals("Should reuse only price columns from production table", 1, selectFromProdTableAsLeft.length)
    assertTrue("Should include price columns", selectFromProdTableAsLeft.contains("user_price_sum"))
    assertFalse("Should not include rating columns", selectFromProdTableAsLeft.contains("user_rating_avg"))

    // Rating join part should need to be computed
    assertEquals("Rating join part should need computation", 1, joinPartsToQueryAndJoin.length)
    assertEquals("Should compute rating join part",
                 s"$testName.rating",
                 joinPartsToQueryAndJoin.head.groupBy.metaData.name)
  }

  it should "handle version mismatches in GroupBy names correctly" in {
    val testName = "analyze_reuse_version_mismatch"

    // Create test data tables
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("item", StringType, 10)
    )

    val groupBySchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val rightTable = s"$namespace.${testName}_right"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $rightTable")

    DataFrameGen.events(spark, leftSchema, 100, 30).save(leftTable)
    DataFrameGen.events(spark, groupBySchema, 100, 30).save(rightTable)

    // Create GroupBys with different versions
    val priceGroupByV0 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = rightTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.price__0", version = 0, namespace = namespace, team = "test_team")
    )

    val priceGroupByV1 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = rightTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      // We're bumping the major version here, even though the schemas as the same we should *NOT* reuse
      metaData =
        Builders.MetaData(name = s"$testName.price2__0", version = 0, namespace = namespace, team = "test_team")
    )

    // Create join parts
    val priceJoinPartV0 = Builders.JoinPart(groupBy = priceGroupByV0).setUseLongNames(false)
    val priceJoinPartV1 = Builders.JoinPart(groupBy = priceGroupByV1).setUseLongNames(false)

    // Create production join (v0)
    val productionJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPartV0),
        metaData = Builders.MetaData(name = testName + "_prod", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    // Create new join (v1) - should match base name with v0 after cleanNameWithoutVersion
    val newJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPartV1),
        metaData = Builders.MetaData(name = testName + "_new", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(monthAgo).setEndDate(monthAgo)

    // Create production table with expected schema
    val productionTable = productionJoin.metaData.outputTable
    spark.sql(s"DROP TABLE IF EXISTS $productionTable")

    val productionSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("item", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("user_price_sum", SparkDoubleType)
      ))

    val productionData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", "item1", 1000L, monthAgo, 100.0),
          SparkRow("user2", "item2", 2000L, monthAgo, 200.0)
        )),
      productionSchema
    )

    productionData.save(productionTable)

    // Create join part table for v1
    val pricePartTable = RelevantLeftForJoinPart.fullPartTableName(newJoin, priceJoinPartV1)
    spark.sql(s"DROP TABLE IF EXISTS $pricePartTable")

    val partTableSchema = SparkStructType(
      Array(
        SparkStructField("user", SparkStringType),
        SparkStructField("ts", SparkLongType),
        SparkStructField("ds", SparkStringType),
        SparkStructField("user_price_sum", SparkDoubleType)
      ))

    val pricePartData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          SparkRow("user1", 1000L, monthAgo, 100.0),
          SparkRow("user2", 2000L, monthAgo, 200.0)
        )),
      partTableSchema
    )

    pricePartData.write.mode(SaveMode.Overwrite).saveAsTable(pricePartTable)

    // Create left DataFrame for schema compatibility check
    val leftDf = DataFrameGen.events(spark, leftSchema, 10, 1)

    // Create MergeJob with production join reference
    val mergeNode = new JoinMergeNode()
      .setJoin(newJoin)
      .setProductionJoin(productionJoin)

    val mergeMetaData = new MetaData()
      .setName(newJoin.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(priceJoinPartV1))

    // Test the analyzeJoinPartsForReuse method directly
    val (selectFromProdTableAsLeft: Seq[String], joinPartsToQueryAndJoin: Seq[JoinPart]) =
      mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results
    println(s"Columns to select from production: ${selectFromProdTableAsLeft.mkString(", ")}")
    println(s"Join parts to compute: ${joinPartsToQueryAndJoin.map(_.groupBy.metaData.name).mkString(", ")}")

    assertEquals("Should NOT reuse price columns from production table due to major version difference",
                 0,
                 selectFromProdTableAsLeft.length)
    assertEquals("Join part should need computation", 1, joinPartsToQueryAndJoin.length)
  }

  it should "handle missing production table gracefully" in {
    val testName = "analyze_reuse_no_production"

    // Create test data tables
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("item", StringType, 10)
    )

    val leftTable = s"$namespace.${testName}_left"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    DataFrameGen.events(spark, leftSchema, 100, 30).save(leftTable)

    val priceGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = leftTable,
          query = Builders.Query(selects = Builders.Selects("user"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.COUNT, inputColumn = "user")),
      metaData = Builders.MetaData(name = s"$testName.price", namespace = namespace, team = "test_team")
    )

    val priceJoinPart = Builders.JoinPart(groupBy = priceGroupBy, prefix = "price").setUseLongNames(false)

    val newJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(priceJoinPart),
        metaData = Builders.MetaData(name = testName + "_new", namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(monthAgo).setEndDate(monthAgo)

    // Create left DataFrame for schema compatibility check
    val leftDf = DataFrameGen.events(spark, leftSchema, 10, 1)

    // Create MergeJob with NO production join reference
    val mergeNode = new JoinMergeNode()
      .setJoin(newJoin)
    // .setProductionJoin(null) - No production join

    val mergeMetaData = new MetaData()
      .setName(newJoin.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(priceJoinPart))

    // Test the analyzeJoinPartsForReuse method directly
    val (selectFromProdTableAsLeft: Seq[String], joinPartsToQueryAndJoin: Seq[JoinPart]) =
      mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results - should fall back to normal join computation
    println(s"Columns to select from production: ${selectFromProdTableAsLeft.mkString(", ")}")
    println(s"Join parts to compute: ${joinPartsToQueryAndJoin.map(_.groupBy.metaData.name).mkString(", ")}")

    // Should not reuse anything and compute all join parts
    assertEquals("Should not reuse any columns when no production join", 0, selectFromProdTableAsLeft.length)
    assertEquals("Should compute all join parts when no production join", 1, joinPartsToQueryAndJoin.length)
    assertEquals("Should compute the price join part",
                 s"$testName.price",
                 joinPartsToQueryAndJoin.head.groupBy.metaData.name)
  }
}
