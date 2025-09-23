package ai.chronon.spark.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.JIterableOps
import ai.chronon.api.{
  BooleanType,
  Builders,
  DateRange,
  DoubleType,
  IntType,
  JoinPart,
  LongType,
  MetaData,
  Operation,
  StringType,
  TimeUnit,
  Window
}
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.sql.types.{
  DoubleType => SparkDoubleType,
  LongType => SparkLongType,
  StringType => SparkStringType,
  StructField => SparkStructField,
  StructType => SparkStructType
}

import scala.collection.Seq
import scala.collection.JavaConverters._
import com.google.gson.Gson
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.planner.{JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.{JoinPartJob, MergeJob, SourceJob}
import ai.chronon.spark.utils.{DataFrameGen, TableTestUtils}
import ai.chronon.spark.{Join, JoinUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

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

    // Set table properties with column hashes for the production table
    val gson = new Gson()

    // Create column hashes that match what the semantic hashing would produce
    val productionColumnHashes = Map(
      "user" -> "hash_user",
      "item" -> "hash_item",
      "ts" -> "hash_ts",
      "price_user_price_sum" -> "hash_price_user_price_sum",
      "user_quantity_count" -> "hash_user_quantity_count"
    )

    productionData.save(productionTable)

    // Add table properties with column hashes
    spark.sql(s"""
      ALTER TABLE $productionTable SET TBLPROPERTIES (
        'column_hashes' = '${gson.toJson(productionColumnHashes.asJava)}'
      )
    """)

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

    // Set column hashes in the new join's metadata to match production table
    newJoin.metaData.setColumnHashes(
      Map(
        "user" -> "hash_user",
        "item" -> "hash_item",
        "ts" -> "hash_ts",
        "ds" -> "hash_ds",
        "price_user_price_sum" -> "hash_price_user_price_sum",
        "user_quantity_count" -> "hash_user_quantity_count"
      ).asJava)

    // Test the analyzeJoinPartsForReuse method directly
    val reuseAnalysis = mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results
    println(s"Reuse table: ${reuseAnalysis.reuseTable.getOrElse("None")}")
    println(s"Columns to select from production: ${reuseAnalysis.columnsToReuse.mkString(", ")}")
    println(s"Join parts to compute: ${reuseAnalysis.joinPartsToCompute.map(_.groupBy.metaData.name).mkString(", ")}")

    // Both join parts should be reusable since production table contains all expected columns
    assertEquals("Should reuse both column sets from production table", 2, reuseAnalysis.columnsToReuse.length)
    assertTrue("Should include price columns", reuseAnalysis.columnsToReuse.contains("price_user_price_sum"))
    assertTrue("Should include quantity columns", reuseAnalysis.columnsToReuse.contains("user_quantity_count"))

    // No join parts should need to be computed
    assertEquals("No join parts should need computation", 0, reuseAnalysis.joinPartsToCompute.length)
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
        SparkStructField("user_price_sum", SparkDoubleType)
        // SparkStructField("user_ratings_avg", SparkDoubleType)
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

    // Set table properties with column hashes for the production table (only price)
    val gson2 = new Gson()
    val productionColumnHashes2 = Map(
      "user" -> "hash_user",
      "item" -> "hash_item",
      "ts" -> "hash_ts",
      "ds" -> "hash_ds",
      "user_price_sum" -> "hash_user_price_sum"
      // Note: rating column hash is missing, so it can't be reused
    )

    productionData.save(productionTable)

    // Add table properties with column hashes
    spark.sql(s"""
      ALTER TABLE $productionTable SET TBLPROPERTIES (
        'column_hashes' = '${gson2.toJson(productionColumnHashes2.asJava)}'
      )
    """)

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

    // Set column hashes in the new join's metadata
    newJoin.metaData.columnHashes = Map(
      "user" -> "hash_user",
      "item" -> "hash_item",
      "ts" -> "hash_ts",
      "ds" -> "hash_ds",
      "user_price_sum" -> "hash_user_price_sum",
      "user_rating_average" -> "hash_user_rating_average" // This won't match production table
    ).asJava

    // Test the analyzeJoinPartsForReuse method directly
    val reuseAnalysis = mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results
    println(s"Reuse table: ${reuseAnalysis.reuseTable.getOrElse("None")}")
    println(s"Columns to select from production: ${reuseAnalysis.columnsToReuse.mkString(", ")}")
    println(s"Join parts to compute: ${reuseAnalysis.joinPartsToCompute.map(_.groupBy.metaData.name).mkString(", ")}")

    // Only price should be reusable, rating should need computation
    assertEquals("Should reuse only price columns from production table", 1, reuseAnalysis.columnsToReuse.length)
    assertTrue("Should include price columns", reuseAnalysis.columnsToReuse.contains("user_price_sum"))
    assertFalse("Should not include rating columns", reuseAnalysis.columnsToReuse.contains("user_rating_avg"))

    // Rating join part should need to be computed
    assertEquals("Rating join part should need computation", 1, reuseAnalysis.joinPartsToCompute.length)
    assertEquals("Should compute rating join part",
                 s"$testName.rating",
                 reuseAnalysis.joinPartsToCompute.head.groupBy.metaData.name)
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

    // Set table properties with column hashes for the production table (v0)
    val gson3 = new Gson()
    val productionColumnHashes3 = Map(
      "user" -> "hash_user",
      "item" -> "hash_item",
      "ts" -> "hash_ts",
      "ds" -> "hash_ds",
      "user_price_sum" -> "hash_user_price_sum_v0" // v0 hash
    )

    productionData.save(productionTable)

    // Add table properties with column hashes
    spark.sql(s"""
      ALTER TABLE $productionTable SET TBLPROPERTIES (
        'column_hashes' = '${gson3.toJson(productionColumnHashes3.asJava)}'
      )
    """)

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

    // Set column hashes in the new join's metadata (v1) - different hash than production
    newJoin.metaData.columnHashes = Map(
      "user" -> "hash_user",
      "item" -> "hash_item",
      "ts" -> "hash_ts",
      "ds" -> "hash_ds",
      "user_price_sum" -> "hash_user_price_sum_v1" // v1 hash - different from production
    ).asJava

    // Test the analyzeJoinPartsForReuse method directly
    val reuseAnalysis = mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results
    println(s"Reuse table: ${reuseAnalysis.reuseTable.getOrElse("None")}")
    println(s"Columns to select from production: ${reuseAnalysis.columnsToReuse.mkString(", ")}")
    println(s"Join parts to compute: ${reuseAnalysis.joinPartsToCompute.map(_.groupBy.metaData.name).mkString(", ")}")

    assertEquals("Should NOT reuse price columns from production table due to major version difference",
                 0,
                 reuseAnalysis.columnsToReuse.length)
    assertEquals("Join part should need computation", 1, reuseAnalysis.joinPartsToCompute.length)
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
    val reuseAnalysis = mergeJob.analyzeJoinPartsForReuse(dateRange.toPartitionRange(tableUtils.partitionSpec), leftDf)

    // Verify results - should fall back to normal join computation
    println(s"Columns to select from production: ${reuseAnalysis.columnsToReuse.mkString(", ")}")
    println(s"Join parts to compute: ${reuseAnalysis.joinPartsToCompute.map(_.groupBy.metaData.name).mkString(", ")}")

    // Should not reuse anything and compute all join parts
    assertEquals("Should not reuse any columns when no production join", 0, reuseAnalysis.columnsToReuse.length)
    assertEquals("Should compute all join parts when no production join", 1, reuseAnalysis.joinPartsToCompute.length)
    assertEquals("Should compute the price join part",
                 s"$testName.price",
                 reuseAnalysis.joinPartsToCompute.head.groupBy.metaData.name)
  }
}
