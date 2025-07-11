package ai.chronon.spark.test.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.planner.{JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.{JoinPartJob, MergeJob, SourceJob}
import ai.chronon.spark.test.{DataFrameGen, TableTestUtils}
import ai.chronon.spark.{Join, JoinUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

class MergeJobVersioningTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  val spark: SparkSession = submission.SparkSessionBuilder.build("MergeJobVersioningTest", local = true)
  private implicit val tableUtils: TableTestUtils = TableTestUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  private val namespace = "test_namespace_merge_job_versioning"
  tableUtils.createDatabase(namespace)

  it should "reuse columns from production table when join part major versions match" in {
    val testName = "merge_job_versioning_test"

    // Create test data tables
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("item", StringType, 10)
    )

    val sharedSchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100)
    )

    val removedSchema = List(
      Column("user", StringType, 10),
      Column("quantity", LongType, 100)
    )

    val addedSchema = List(
      Column("user", StringType, 10),
      Column("rating", DoubleType, 5)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val sharedTable = s"$namespace.${testName}_shared"
    val removedTable = s"$namespace.${testName}_removed"
    val addedTable = s"$namespace.${testName}_added"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $sharedTable")
    spark.sql(s"DROP TABLE IF EXISTS $removedTable")
    spark.sql(s"DROP TABLE IF EXISTS $addedTable")

    DataFrameGen.events(spark, leftSchema, 1000, 60).save(leftTable)
    DataFrameGen.events(spark, sharedSchema, 100, 60).save(sharedTable)
    DataFrameGen.events(spark, removedSchema, 100, 60).save(removedTable)
    DataFrameGen.events(spark, addedSchema, 100, 60).save(addedTable)

    // Create GroupBys
    val sharedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = sharedTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.shared", namespace = namespace, team = "test_team")
    )

    val removedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = removedTable,
          query = Builders.Query(selects = Builders.Selects("quantity"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.COUNT, inputColumn = "quantity")),
      metaData = Builders.MetaData(name = s"$testName.removed", namespace = namespace, team = "test_team")
    )

    val addedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = addedTable,
          query = Builders.Query(selects = Builders.Selects("rating"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "rating")),
      metaData = Builders.MetaData(name = s"$testName.added", namespace = namespace, team = "test_team")
    )

    // Create join parts
    val sharedJoinPart = Builders.JoinPart(groupBy = sharedGroupBy, prefix = "shared").setUseLongNames(false)
    val removedJoinPart = Builders.JoinPart(groupBy = removedGroupBy, prefix = "removed").setUseLongNames(false)
    val addedJoinPart = Builders.JoinPart(groupBy = addedGroupBy, prefix = "added").setUseLongNames(false)

    // Create v0 join (with shared and removed)
    val joinV0 = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(sharedJoinPart, removedJoinPart),
        metaData = Builders.MetaData(name = testName + "__0", version = 0, namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    // Create v1 join (with shared and added)
    val joinV1 = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(sharedJoinPart, addedJoinPart),
        metaData = Builders.MetaData(name = testName + "__1", version = 1, namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(start).setEndDate(monthAgo)

    // Step 1: Run v0 join completely to create production table
    val v0Join = new Join(joinConf = joinV0, endPartition = monthAgo, tableUtils)
    v0Join.computeJoin(Some(100))

    // Step 2: Manually modify production table with literal values, maintaining partitioning
    val productionTable = joinV0.metaData.outputTable
    val existingProductionData = tableUtils.scanDf(null, productionTable, None)

    existingProductionData.show()
    print(existingProductionData.schema.pretty)

    val sharedColumnName = s"shared_user_price_sum"
    val removedColumnName = s"removed_user_quantity_count"

    // Replace with literal values for testing, maintaining original structure
    val productionDataWithLiterals =
      existingProductionData //.drop(sharedColumnName, removedColumnName) // Remove existing columns
        .withColumn(sharedColumnName, lit(999.0)) // Literal value we expect to be reused
        .withColumn(removedColumnName, lit(42L)) // This should not appear in v1 result

    productionDataWithLiterals.show()
    print(productionDataWithLiterals.schema.pretty)

    // Use proper partition overwrite to maintain partitioning
    productionDataWithLiterals.write
      .mode(SaveMode.Overwrite)
      .insertInto(productionTable)

    // Step 3: Run source job for v1
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(joinV1)
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    val sourceMetaData = new MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    tableUtils.sql(f"SELECT * from $leftTable").show()
    tableUtils.sql(f"SELECT distinct ds from $leftTable order by ds desc").show(100)

    val leftSourceWithFilter = new SourceWithFilterNode().setSource(joinV1.left)
    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceMetaData, dateRange)
    try {}
    sourceRunner.run()

    // Step 4: Run join part job for the added GroupBy only (shared will be reused from production)
    val addedPartTableName = RelevantLeftForJoinPart.partTableName(joinV1, addedJoinPart)
    val addedPartFullTableName = RelevantLeftForJoinPart.fullPartTableName(joinV1, addedJoinPart)

    val addedPartMetaData = new MetaData()
      .setName(addedPartTableName)
      .setOutputNamespace(joinV1.metaData.outputNamespace)

    val addedJoinPartNode = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinV1.getLeft.dataModel)
      .setJoinPart(addedJoinPart)

    val addedJoinPartJob = new JoinPartJob(addedJoinPartNode, addedPartMetaData, dateRange)
    addedJoinPartJob.run()

    // Step 5: Run MergeJob with production join reference
    val mergeNode = new JoinMergeNode()
      .setJoin(joinV1)
      .setProductionJoin(joinV0)

    val mergeMetaData = new MetaData()
      .setName(joinV1.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(sharedJoinPart, addedJoinPart))
    mergeJob.run()

    // Step 6: Verify results
    val resultTable = joinV1.metaData.outputTable
    val result = tableUtils.scanDf(null, resultTable, None)

    val resultRows = result.collect()
    assertTrue("Should have results", resultRows.length > 0)
    result.show()

    // Verify that shared column was reused from production (literal value 999.0)
    assertTrue(s"Result should contain reused column $sharedColumnName", result.columns.contains(sharedColumnName))

    val reusedValues = result.select(sharedColumnName).distinct().collect()
    assertEquals("Should have exactly one distinct value for reused column", 1, reusedValues.length)
    assertEquals("Reused column should have literal value from production",
                 999.0,
                 reusedValues(0).getAs[Double](sharedColumnName),
                 0.01)

    // Verify that added column was computed normally
    val addedColumnName = s"added_user_rating_average"
    assertTrue(s"Result should contain computed column $addedColumnName", result.columns.contains(addedColumnName))

    val addedValues = result.select(addedColumnName).filter(col(addedColumnName).isNotNull).collect()
    assertTrue("Added column should have non-null computed values", addedValues.length > 0)

    // Verify that removed column is not present in result
    assertFalse(s"Removed column should not be present", result.columns.contains(removedColumnName))

    // Verify essential columns are present
    val expectedEssentialColumns = Set("user", "item", "ds", "ts")
    val actualColumns = result.columns.toSet
    assertTrue("Essential columns should be present", expectedEssentialColumns.subsetOf(actualColumns))

    println(s"Test passed! Result schema: ${result.columns.mkString(", ")}")
    println(s"Reused column distinct values: ${reusedValues.length}")
    println(s"Computed column non-null values: ${addedValues.length}")
  }
}
