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

package ai.chronon.spark.bootstrap

import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.spark.Comparison
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TableBootstrapTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = SparkSessionBuilder.build("BootstrapTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  // Create bootstrap dataset with randomly overwritten data
  def buildBootstrapPart(queryTable: String,
                         namespace: String,
                         tableName: String,
                         columnName: String = "unit_test_user_transactions_amount_dollars_sum_30d",
                         samplePercent: Double = 0.8,
                         partitionCol: String = "ds"): (BootstrapPart, DataFrame) = {
    val bootstrapTable = s"$namespace.$tableName"
    val preSampleBootstrapDf = tableUtils
      .loadTable(queryTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as(columnName),
        col("ds")
      )
      .withColumnRenamed("ds", partitionCol)

    val rawBootstrapDf = if (samplePercent < 1.0) {
      preSampleBootstrapDf.sample(samplePercent)
    } else {
      preSampleBootstrapDf
    }

    rawBootstrapDf.save(bootstrapTable, partitionColumns = Seq(partitionCol))

    val bootstrapDf = tableUtils.loadTable(bootstrapTable)
    val bootstrapDfDefaultPartition = bootstrapDf.withColumnRenamed(partitionCol, "ds")
    val partitionRange = bootstrapDfDefaultPartition.partitionRange

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", columnName),
        startPartition = partitionRange.start,
        endPartition = partitionRange.end,
        partitionColumn = partitionCol
      ),
      table = bootstrapTable
    )

    (bootstrapPart, bootstrapDfDefaultPartition)
  }

  it should "bootstrap" in {

    val namespace = "test_table_bootstrap"
    tableUtils.createDatabase(namespace)

    // group by
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)

    // query
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    // Define base join which uses standard backfill
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    // Runs through standard backfill
    val runner1 = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val baseOutput = runner1.computeJoin()

    // Create two bootstrap parts to verify that bootstrap coalesce respects the ordering of the input bootstrap parts
    val (bootstrapTable1, bootstrapTable2) = ("user_transactions_bootstrap1", "user_transactions_bootstrap2")
    val (bootstrapPart1, bootstrapDf1) = buildBootstrapPart(queryTable, namespace, bootstrapTable1)
    val (bootstrapPart2, bootstrapDf2) =
      buildBootstrapPart(queryTable, namespace, bootstrapTable2, partitionCol = "date")

    // val bootstrapDf2 = bootstrapDf2Partition.withColumnRenamed("date", "ds")

    // Create bootstrap join using base join as template
    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.user_transaction_features.bootstrap_copy")
    bootstrapJoin.setBootstrapParts(Seq(bootstrapPart1, bootstrapPart2).toJava)

    // Runs through boostrap backfill which combines backfill and bootstrap
    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)
    val computed = runner2.computeJoin()

    // Comparison
    val expected = baseOutput
      .join(bootstrapDf1,
            baseOutput("request_id") <=> bootstrapDf1("request_id") and baseOutput("ds") <=> bootstrapDf1("ds"),
            "left")
      .join(bootstrapDf2,
            baseOutput("request_id") <=> bootstrapDf2("request_id") and baseOutput("ds") <=> bootstrapDf2("ds"),
            "left")
      .select(
        baseOutput("user"),
        baseOutput("request_id"),
        baseOutput("ts"),
        coalesce(
          coalesce(bootstrapDf1("unit_test_user_transactions_amount_dollars_sum_30d"),
                   bootstrapDf2("unit_test_user_transactions_amount_dollars_sum_30d")),
          baseOutput("unit_test_user_transactions_amount_dollars_sum_30d")
        ).as("unit_test_user_transactions_amount_dollars_sum_30d"),
        baseOutput("unit_test_user_transactions_amount_dollars_sum_15d"), // not covered by bootstrap
        baseOutput("ds")
      )

    val overlapBaseBootstrap1 = baseOutput.join(bootstrapDf1, Seq("request_id", "ds")).count()
    val overlapBaseBootstrap2 = baseOutput.join(bootstrapDf2, Seq("request_id", "ds")).count()
    val overlapBootstrap12 = bootstrapDf1.join(bootstrapDf2, Seq("request_id", "ds")).count()
    logger.info(s"""Debug information:
         |base count: ${baseOutput.count()}
         |overlap keys between base and bootstrap1 count: ${overlapBaseBootstrap1}
         |overlap keys between base and bootstrap2 count: ${overlapBaseBootstrap2}
         |overlap keys between bootstrap1 and bootstrap2 count: ${overlapBootstrap12}
         |""".stripMargin)

    val diff = Comparison.sideBySide(computed, expected, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }

  it should "bootstrap same join part multiple sources" in {

    val namespace = "test_bootstrap_multi_source"
    tableUtils.createDatabase(namespace)

    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val endDs = tableUtils.loadTable(queryTable).select(max(tableUtils.partitionColumn)).head().getString(0)

    val joinPart = Builders.JoinPart(groupBy = BootstrapUtils.buildGroupBy(namespace, spark))
    val derivations = Seq(
      Builders.Derivation(
        name = "amount_dollars_sum_15d",
        expression = "unit_test_user_transactions_amount_dollars_sum_15d"
      ),
      Builders.Derivation(
        name = "amount_dollars_sum_30d",
        expression = "unit_test_user_transactions_amount_dollars_sum_30d"
      )
    )

    val bootstrapPart1 = buildBootstrapPart(queryTable,
                                            namespace,
                                            tableName = "bootstrap_1",
                                            columnName = "unit_test_user_transactions_amount_dollars_sum_30d",
                                            samplePercent = 1.0)

    val bootstrapPart2 = buildBootstrapPart(queryTable,
                                            namespace,
                                            tableName = "bootstrap_2",
                                            columnName = "unit_test_user_transactions_amount_dollars_sum_15d",
                                            samplePercent = 1.0)
    val join = Builders.Join(
      left = Builders.Source.events(table = queryTable, query = Builders.Query()),
      joinParts = Seq(joinPart),
      rowIds = Seq("request_id"),
      bootstrapParts = Seq(
        bootstrapPart1._1,
        bootstrapPart2._1
      ),
      derivations = derivations,
      metaData = Builders.MetaData(name = "test.bootstrap_multi_source", namespace = namespace, team = "chronon")
    )

    val joinJob = new ai.chronon.spark.Join(join, endDs, tableUtils)
    joinJob.computeJoin()

    // assert that no computation happened for join part since all derivations have been bootstrapped
    assertFalse(tableUtils.tableReachable(join.partOutputTable(joinPart)))
  }
}
