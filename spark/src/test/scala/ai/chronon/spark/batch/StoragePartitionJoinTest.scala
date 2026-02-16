package ai.chronon.spark.batch

import ai.chronon.api._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.planner.JoinMergeNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{BootstrapInfo, Join, JoinUtils}
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.sql.types.{
  IntegerType => SparkIntegerType,
  StringType => SparkStringType,
  StructField => SparkStructField,
  StructType => SparkStructType
}
import org.junit.Assert._

class StoragePartitionJoinTest extends StoragePartitionJoinTestBase {

  override protected def namespace = "test_namespace_spj"
  override protected def sparkConfs: Map[String, String] = baseSparkConfs

  createDatabase(namespace)

  private val toyTableA = s"$namespace.spj_table_a"
  private val toyTableB = s"$namespace.spj_table_b"

  it should "eliminate exchanges when joining identity-partitioned Iceberg tables on composite key" in {
    createToyTables(toyTableA, toyTableB)

    val dfA = spark.table(toyTableA)
    val dfB = spark.table(toyTableB)
    val joined = dfA.join(dfB, Seq("id", "ds"))

    verifyNoShuffle(joined)
    assertTrue("Join should produce results", joined.count() > 0)
  }

  it should "eliminate exchanges in MergeJob pipeline" in {
    val f = buildEntityJoinFixture("spj_merge")

    val leftTable = JoinUtils.computeFullLeftSourceTableName(f.joinConf)
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    val leftDf = tableUtils.scanDf(query = null, table = f.entityTable, range = Some(f.partitionRange))
    leftDf.save(leftTable)

    val partTable = RelevantLeftForJoinPart.fullPartTableName(f.joinConf, f.joinPart)
    spark.sql(s"DROP TABLE IF EXISTS $partTable")
    val rightDf = tableUtils.scanDf(query = null, table = f.entityTable, range = Some(f.partitionRange))
      .withColumnRenamed("amount", "amount_sum")
    rightDf.save(partTable)

    // SPJ requires multiple ds partitions to align — single-day steps
    // cause Iceberg to drop groupedBy metadata since there's only one group.
    val mergeNode = new JoinMergeNode().setJoin(f.joinConf)
    val mergeMetaData = new MetaData()
      .setName(f.joinConf.metaData.name)
      .setOutputNamespace(namespace)
    val mergeJob = new MergeJob(mergeNode, mergeMetaData, f.range, Seq(f.joinPart))

    val resultDf = mergeJob.computeMerge(f.partitionRange)

    val plan = resultDf.queryExecution.executedPlan
    println(s"MergeJob multi-day physical plan:\n$plan")
    verifyNoShuffle(resultDf)
    assertTrue("MergeJob result should have rows", resultDf.count() > 0)
  }

  it should "eliminate exchanges in MonolithJoin pipeline" in {
    val f = buildEntityJoinFixture("spj_monolith")

    // Runs the full MonolithJoin pipeline: JoinBootstrapJob (saves bootstrap table),
    // JoinPartJob (saves part table), then joins both Iceberg scans.
    val join = new Join(f.joinConf, threeDaysAgo, tableUtils)
    val leftDfOpt = JoinUtils.leftDf(f.joinConf, f.partitionRange, tableUtils)
    assertTrue("Left data should not be empty", leftDfOpt.isDefined)

    val bootstrapInfo = BootstrapInfo.from(f.joinConf, f.partitionRange, tableUtils, leftDfOpt.map(_.schema))
    val resultOpt = join.computeRange(leftDfOpt.get, f.partitionRange, bootstrapInfo)

    assertTrue("computeRange should return a result", resultOpt.isDefined)
    val resultDf = resultOpt.get

    val plan = resultDf.queryExecution.executedPlan
    println(s"MonolithJoin physical plan:\n$plan")
    verifyNoShuffle(resultDf)
    assertTrue("MonolithJoin result should have rows", resultDf.count() > 0)
  }

  it should "eliminate exchanges when joining GroupBy snapshotEntities output with source" in {
    val f = buildGroupByEntitySnapshotFixture("spj_gb_entity")
    val leftDf = tableUtils.scanDf(query = null, table = f.sourceTable, range = Some(f.range))
    val rightDf = tableUtils.scanDf(query = null, table = f.outputTable, range = Some(f.range))
    val joined = leftDf.join(rightDf, Seq("user", "ds"))

    val plan = joined.queryExecution.executedPlan
    println(s"GroupBy snapshotEntities SPJ plan:\n$plan")
    verifyNoShuffle(joined)
    assertTrue("Join should produce results", joined.count() > 0)
  }

  it should "eliminate exchanges when joining GroupBy snapshotEvents output with source" in {
    val f = buildGroupByEventSnapshotFixture("spj_gb_events")
    val leftDf = tableUtils.scanDf(query = null, table = f.sourceTable, range = Some(f.range))
    val rightDf = tableUtils.scanDf(query = null, table = f.outputTable, range = Some(f.range))
    val joined = leftDf.join(rightDf, Seq("user", "ds"))

    val plan = joined.queryExecution.executedPlan
    println(s"GroupBy snapshotEvents SPJ plan:\n$plan")
    verifyNoShuffle(joined)
    assertTrue("Join should produce results", joined.count() > 0)
  }

  it should "not require correctness shuffles when writing to Iceberg tables with write.distribution-mode=none" in {
    val sourceTable = s"$namespace.spj_write_source"
    val targetTable = s"$namespace.spj_write_target"
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
    spark.sql(s"DROP TABLE IF EXISTS $targetTable")

    val schema = SparkStructType(Array(
      SparkStructField("id", SparkStringType),
      SparkStructField("value", SparkIntegerType),
      SparkStructField("ds", SparkStringType)))
    val rows = Seq("2024-01-01", "2024-01-02", "2024-01-03").flatMap { day =>
      (1 to 50).map(i => SparkRow(s"user_$i", i * 10, day))
    }
    val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)

    // save() creates tables with Chronon's default Iceberg properties (write.distribution-mode=none)
    df.save(sourceTable)
    df.save(targetTable)

    // Verify the property is set on the target table
    val props = tableUtils.getTableProperties(targetTable).getOrElse(Map.empty)
    assertEquals("write.distribution-mode should be none",
      "none", props.getOrElse("write.distribution-mode", ""))

    // Check write plan: read from Iceberg source → write to Iceberg target
    spark.table(sourceTable).createOrReplaceTempView("spj_write_iceberg_source")
    val planStr = getWritePlan(targetTable, "spj_write_iceberg_source")
    println(s"Iceberg write plan (distribution-mode=none, SPJ enabled):\n$planStr")

    // REBALANCE_PARTITIONS_BY_COL is a file-layout optimization, not a correctness shuffle.
    // ENSURE_REQUIREMENTS exchanges indicate mandatory data redistribution.
    val requirementExchanges = planStr.split("\n").count(_.contains("ENSURE_REQUIREMENTS"))
    assertEquals(
      s"Expected no ENSURE_REQUIREMENTS exchanges in write plan.\nPlan:\n$planStr",
      0, requirementExchanges)
  }

  it should "eliminate exchanges in MonolithJoin pipeline without bootstrap" in {
    val f = buildEntityJoinFixture("spj_no_bootstrap")

    val join = new Join(f.joinConf, threeDaysAgo, tableUtils)
    val leftDfOpt = JoinUtils.leftDf(f.joinConf, f.partitionRange, tableUtils)
    assertTrue("Left data should not be empty", leftDfOpt.isDefined)

    val bootstrapInfo = BootstrapInfo.from(f.joinConf, f.partitionRange, tableUtils, leftDfOpt.map(_.schema))
    val resultOpt = join.computeRange(leftDfOpt.get, f.partitionRange, bootstrapInfo, usingBootstrappedLeft = true)

    assertTrue("computeRange should return a result", resultOpt.isDefined)
    val resultDf = resultOpt.get

    val plan = resultDf.queryExecution.executedPlan
    println(s"MonolithJoin no-bootstrap physical plan:\n$plan")
    verifyNoShuffle(resultDf)
    assertTrue("MonolithJoin no-bootstrap result should have rows", resultDf.count() > 0)
  }
}
