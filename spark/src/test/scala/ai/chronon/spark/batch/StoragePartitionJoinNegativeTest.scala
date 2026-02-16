package ai.chronon.spark.batch

import ai.chronon.spark.{BootstrapInfo, Join, JoinUtils}
import org.junit.Assert._

/**
 * Negative counterpart to StoragePartitionJoinTest.
 * SPJ is disabled via sparkConfs so Exchange operators ARE expected.
 * This proves the positive tests aren't vacuously passing.
 */
class StoragePartitionJoinNegativeTest extends StoragePartitionJoinTestBase {

  override protected def namespace = "test_namespace_spj_neg"
  override protected def sparkConfs: Map[String, String] = baseSparkConfs ++ Map(
    "spark.sql.sources.v2.bucketing.enabled" -> "false",
    "spark.sql.iceberg.planning.preserve-data-grouping" -> "false",
    "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "false",
    "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> "false"
  )

  createDatabase(namespace)

  it should "contain exchanges when joining Iceberg tables without SPJ" in {
    val tableA = s"$namespace.spj_neg_table_a"
    val tableB = s"$namespace.spj_neg_table_b"
    createToyTables(tableA, tableB)

    val dfA = spark.table(tableA)
    val dfB = spark.table(tableB)
    val joined = dfA.join(dfB, Seq("id", "ds"))

    val plan = joined.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    println(s"Negative toy join physical plan:\n$plan")
    assertTrue(
      s"Expected Exchange operators when SPJ disabled but found $exchangeCount.\nPlan:\n${plan.toString}",
      exchangeCount > 0)
  }

  it should "contain exchanges in MonolithJoin pipeline without SPJ" in {
    val f = buildEntityJoinFixture("spj_neg_monolith")

    val join = new Join(f.joinConf, threeDaysAgo, tableUtils)
    val leftDfOpt = JoinUtils.leftDf(f.joinConf, f.partitionRange, tableUtils)
    assertTrue("Left data should not be empty", leftDfOpt.isDefined)

    val bootstrapInfo = BootstrapInfo.from(f.joinConf, f.partitionRange, tableUtils, leftDfOpt.map(_.schema))
    val resultOpt = join.computeRange(leftDfOpt.get, f.partitionRange, bootstrapInfo)

    assertTrue("computeRange should return a result", resultOpt.isDefined)
    val resultDf = resultOpt.get

    val plan = resultDf.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    println(s"Negative MonolithJoin physical plan:\n$plan")
    assertTrue(
      s"Expected Exchange operators when SPJ disabled but found $exchangeCount.\nPlan:\n${plan.toString}",
      exchangeCount > 0)
  }

  it should "contain exchanges when joining GroupBy snapshotEntities output without SPJ" in {
    val f = buildGroupByEntitySnapshotFixture("spj_neg_gb_entity")
    val leftDf = tableUtils.scanDf(query = null, table = f.sourceTable, range = Some(f.range))
    val rightDf = tableUtils.scanDf(query = null, table = f.outputTable, range = Some(f.range))
    val joined = leftDf.join(rightDf, Seq("user", "ds"))

    val plan = joined.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    println(s"Negative GroupBy snapshotEntities plan:\n$plan")
    assertTrue(
      s"Expected Exchange operators when SPJ disabled but found $exchangeCount.\nPlan:\n${plan.toString}",
      exchangeCount > 0)
  }

  it should "contain exchanges when joining GroupBy snapshotEvents output without SPJ" in {
    val f = buildGroupByEventSnapshotFixture("spj_neg_gb_events")
    val leftDf = tableUtils.scanDf(query = null, table = f.sourceTable, range = Some(f.range))
    val rightDf = tableUtils.scanDf(query = null, table = f.outputTable, range = Some(f.range))
    val joined = leftDf.join(rightDf, Seq("user", "ds"))

    val plan = joined.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    println(s"Negative GroupBy snapshotEvents plan:\n$plan")
    assertTrue(
      s"Expected Exchange operators when SPJ disabled but found $exchangeCount.\nPlan:\n${plan.toString}",
      exchangeCount > 0)
  }

  it should "not set write.distribution-mode=none on manually created Iceberg tables" in {
    val tableName = s"$namespace.spj_neg_write_manual"
    spark.sql(s"DROP TABLE IF EXISTS $tableName")

    spark.sql(
      s"""CREATE TABLE $tableName (
         |  id STRING,
         |  value INT,
         |  ds STRING
         |) USING iceberg
         |PARTITIONED BY (ds)""".stripMargin)

    val props = tableUtils.getTableProperties(tableName).getOrElse(Map.empty)
    assertFalse(
      "Manually created table should not have write.distribution-mode=none",
      props.get("write.distribution-mode").contains("none"))
  }

  it should "contain exchanges in MonolithJoin pipeline without bootstrap and without SPJ" in {
    val f = buildEntityJoinFixture("spj_neg_no_bootstrap")

    val join = new Join(f.joinConf, threeDaysAgo, tableUtils)
    val leftDfOpt = JoinUtils.leftDf(f.joinConf, f.partitionRange, tableUtils)
    assertTrue("Left data should not be empty", leftDfOpt.isDefined)

    val bootstrapInfo = BootstrapInfo.from(f.joinConf, f.partitionRange, tableUtils, leftDfOpt.map(_.schema))
    val resultOpt = join.computeRange(leftDfOpt.get, f.partitionRange, bootstrapInfo, usingBootstrappedLeft = true)

    assertTrue("computeRange should return a result", resultOpt.isDefined)
    val resultDf = resultOpt.get

    val plan = resultDf.queryExecution.executedPlan
    val exchangeCount = countExchanges(plan)
    println(s"Negative MonolithJoin no-bootstrap physical plan:\n$plan")
    assertTrue(
      s"Expected Exchange operators when SPJ disabled but found $exchangeCount.\nPlan:\n${plan.toString}",
      exchangeCount > 0)
  }
}
