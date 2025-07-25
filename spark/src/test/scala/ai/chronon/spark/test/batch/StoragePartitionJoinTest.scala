package ai.chronon.spark.test.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.planner.{JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.batch._
import ai.chronon.spark.test.{DataFrameGen, TableTestUtils}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files

class StoragePartitionJoinTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  val correctConfigsForSPJ = Map(
    // V1 bucketing configurations
    "spark.sql.sources.bucketing.enabled" -> "false",
    "spark.sql.bucketing.coalesceBucketsInJoin.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1", // Disable broadcast joins to force bucketed joins
    "spark.sql.adaptive.enabled" -> "true",

    // V2 bucketing configurations for Iceberg
    "spark.sql.sources.v2.bucketing.enabled" -> "true",
    "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
    "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
    "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
    "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> "true",

    // Iceberg catalog configurations
    "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type" -> "hadoop",
    "spark.sql.catalog.spark_catalog.warehouse" -> Files
      .createTempDirectory("storage-partition-join-test")
      .toString,
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

    // Hive metastore configurations for Iceberg
    "javax.jdo.option.ConnectionURL" -> "jdbc:derby:memory:metastore_db;create=true",
    "javax.jdo.option.ConnectionDriverName" -> "org.apache.derby.jdbc.EmbeddedDriver",
    "datanucleus.schema.autoCreateAll" -> "true",
    "datanucleus.schema.autoCreateTables" -> "true",
    "datanucleus.schema.autoCreateColumns" -> "true",
    "datanucleus.schema.autoCreateConstraints" -> "true",
    "spark.sql.join.preferSortMergeJoin" -> "false",
    "hive.metastore.schema.verification" -> "false",
    "hive.metastore.schema.verification.record.version" -> "false",
    "hive.metastore.uris" -> "",
    "hive.metastore.warehouse.dir" -> "file:///tmp/hive-warehouse",
    "spark.chronon.table_write.format" -> "iceberg"
  )

  val spark: SparkSession = submission.SparkSessionBuilder.build(
    "StoragePartitionJoinTest",
    hiveSupport = true,
    local = true,
    additionalConfig = Some(correctConfigsForSPJ)
  )
  private implicit val tableUtils: TableTestUtils = TableTestUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  val start = tableUtils.partitionSpec.minus(today, new Window(35, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  private val namespace = "test_namespace_storage_partition_join"
  tableUtils.createDatabase(namespace)

  def setSPJConfigs(enable: Boolean): Unit = {
    val value = enable.toString

    val spjConfigs = Map(
      "spark.sql.sources.bucketing.enabled" -> value,
      "spark.sql.bucketing.coalesceBucketsInJoin.enabled" -> value,
      "spark.sql.adaptive.enabled" -> value,
      "spark.sql.sources.v2.bucketing.enabled" -> value,
      "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> value,
      "spark.sql.iceberg.planning.preserve-data-grouping" -> value,
      "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> value,
      "spark.sql.requireAllClusterKeysForCoPartition" -> (!enable).toString // this one is inverted
    )

    spjConfigs.foreach { case (key, v) =>
      spark.conf.set(key, v)
    }
  }

  def getPhysicalPlan(df: org.apache.spark.sql.DataFrame): String = {
    val physicalPlan = df.queryExecution.executedPlan.toString
    println(s"=== Physical plan ===")
    println(physicalPlan)
    println("=" * 50)
    physicalPlan
  }

  /** Verifies that a DataFrame's physical plan doesn't contain shuffles (Exchange operators)
    * and uses bucketed joins as expected.
    */
  def verifyNoShuffle(df: org.apache.spark.sql.DataFrame, testName: String): Unit = {
    val physicalPlan = getPhysicalPlan(df)

    // Assert no shuffles (Exchange operators)
    assertFalse(
      s"Physical plan should not contain Exchange (shuffle) for $testName",
      physicalPlan.contains("Exchange")
    )

    // Assert successful bucketed join - look for SortMergeJoin or similar
    val hasBucketedJoin = physicalPlan.contains("SortMergeJoin")

    assertTrue(
      s"Physical plan should contain a join operator for $testName",
      hasBucketedJoin
    )

    println(s"✓ $testName: No shuffles detected in physical plan")
  }

  it should "test toy example" in {
    val left =
      s"""
         |CREATE TABLE target (id INT, salary INT, dep STRING)
         |USING iceberg
         |PARTITIONED BY (dep, bucket(4, id))
       """.stripMargin
    val right =
      s"""
         |CREATE TABLE source (id INT, salary INT, dep STRING)
         |USING iceberg
         |PARTITIONED BY (dep, bucket(4, id))
         |""".stripMargin
    spark.sql(left)
    spark.sql(right)

    // Insert dummy data into target table
    spark.sql("""
      INSERT INTO target VALUES
      (1, 50000, 'engineering'),
      (2, 60000, 'marketing'),
      (3, 55000, 'engineering'),
      (4, 45000, 'sales'),
      (5, 70000, 'marketing')
    """)

    // Insert dummy data into source table
    spark.sql("""
      INSERT INTO source VALUES
      (1, 52000, 'engineering'),
      (2, 58000, 'marketing'),
      (6, 48000, 'sales'),
      (7, 65000, 'engineering'),
      (8, 40000, 'hr')
    """)

    val tgt = spark.sql("SELECT * FROM target")
    val src = spark.sql("SELECT * FROM source")

    assertNoExchange(tgt.join(src, tgt("dep") === src("dep") && tgt("id") === src("id"), "inner"))

  }

  private def countExchanges(plan: SparkPlan): Int = {
    plan.collect { case _: Exchange => 1 }.sum
  }

  private def assertNoExchange(df: DataFrame, message: String = ""): Unit = {
    val plan = df.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    assert(exchangeCount == 0,
           s"Expected no Exchange operators but found $exchangeCount. $message\nPlan:\n${plan.toString}")
  }

  private def setupAndGetMergeDF(tableUtils: TableTestUtils) = {
    implicit val tu: TableTestUtils = tableUtils
    // Step 1: Setup test data - similar to ModularJoinTest but simplified with single joinPart
    val userTransactions = List(
      Column("user", StringType, 20),
      Column("user_name", api.StringType, 20),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val userTransactionTable = s"$namespace.user_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $userTransactionTable")
    // Create larger dataset to ensure we don't hit broadcast threshold
    DataFrameGen.entities(spark, userTransactions, 10000, partitions = 100).save(userTransactionTable)

    // Create the GroupBy source for the right side
    val transactionSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = monthAgo
      ),
      snapshotTable = userTransactionTable
    )

    // Create a simple GroupBy with single aggregation
    val groupBy = Builders.GroupBy(
      sources = Seq(transactionSource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "amount_dollars",
          windows = Seq(new Window(30, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "unit_test.user_transaction_sum", namespace = namespace, team = "chronon")
    )

    // Create queries (left side) with row IDs for bucketing
    val queriesSchema = List(
      Column("user", api.StringType, 20)
    )

    val queryTable = s"$namespace.user_queries"
    DataFrameGen
      .events(spark, queriesSchema, 5000, partitions = 50, addRowID = true)
      .save(queryTable)

    // Create the join configuration with single joinPart
    val joinPart = Builders.JoinPart(groupBy = groupBy)

    val joinConf: ai.chronon.api.Join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(startPartition = start),
        table = queryTable
      ),
      joinParts = Seq(joinPart), // Single joinPart only
      metaData =
        Builders.MetaData(name = "test.storage_partition_join_features", namespace = namespace, team = "chronon")
    )

    // Step 2: Run SourceJob to create bucketed left source table
    val leftSourceWithFilter = new SourceWithFilterNode().setSource(joinConf.left)
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(joinConf)

    println(s"Source output table: $sourceOutputTable")

    // Split the output table to get namespace and name
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    // Create metadata for source job
    val sourceMetaData = new api.MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    val sourceJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(monthAgo)

    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceMetaData, sourceJobRange)
    sourceRunner.run()

    val sourceRowCount = tableUtils.sql(s"SELECT * FROM $sourceOutputTable").count()
    println(s"Source table row count: $sourceRowCount")
    assertTrue("Source table should have data", sourceRowCount > 0)

    // Step 3: Run JoinPartJob to create bucketed right table
    val joinPartTableName = RelevantLeftForJoinPart.partTableName(joinConf, joinPart)
    val joinPartFullTableName = RelevantLeftForJoinPart.fullPartTableName(joinConf, joinPart)
    val outputNamespace = joinConf.metaData.outputNamespace

    println(s"JoinPart output table: $joinPartFullTableName")

    val joinPartJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(monthAgo)

    // Create metadata for join part job
    val joinPartMetaData = new api.MetaData()
      .setName(joinPartTableName)
      .setOutputNamespace(outputNamespace)

    val joinPartNode = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinConf.getLeft.dataModel)
      .setJoinPart(joinPart)

    val joinPartJob = new JoinPartJob(joinPartNode, joinPartMetaData, joinPartJobRange)
    joinPartJob.run()

    val joinPartRowCount = tableUtils.sql(s"SELECT * FROM $joinPartFullTableName").count()
    println(s"JoinPart table row count: $joinPartRowCount")
    assertTrue("JoinPart table should have data", joinPartRowCount > 0)

    // Step 4: Call MergeJob.runDayStep to get joined DataFrame (instead of full MergeJob.run)
    val mergeNode = new JoinMergeNode()
      .setJoin(joinConf)

    val mergeMetaData = new api.MetaData()
      .setName(joinConf.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, joinPartJobRange, Seq(joinPart))(tableUtils)

    // Create a single day step for testing
    val dayStep = joinPartJobRange.toPartitionRange(tableUtils.partitionSpec).steps(days = 1).head
    println(s"Testing day step: ${dayStep.start} to ${dayStep.end}")

    // Call runDayStep directly to get the DataFrame
    val joinedDfTry = mergeJob.runDayStep(dayStep)
    assertTrue("MergeJob.runDayStep should succeed", joinedDfTry.isSuccess)

    joinedDfTry.get

  }

  it should "test storage partition bucketing with no shuffle in join" in {
    val joinedDf = setupAndGetMergeDF(tableUtils)

    joinedDf.show()
    joinedDf.explain(true)

    // Step 5: Analyze physical plan to verify no shuffles
    verifyNoShuffle(joinedDf, "Storage Partition Bucketed Join")

    println("✓ Storage partition bucketing join test completed successfully!")
    println("✓ No shuffles detected - bucketing optimization is working!")
  }

  it should "NOT storage partitioned join due to incorrect configuration" in {
    setSPJConfigs(false)
    val tableUtils: TableTestUtils = TableTestUtils(spark)

    val joinedDf = setupAndGetMergeDF(tableUtils)

    joinedDf.show()
    joinedDf.explain(true)

    // Step 5: Analyze physical plan to verify no shuffles
    assertThrows[AssertionError](verifyNoShuffle(joinedDf, "Storage Partition Bucketed Join"))

    println("✓ Shuffles detected as expected when incorrect configs are used")
    setSPJConfigs(true)
  }
}
