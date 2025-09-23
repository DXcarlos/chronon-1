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

package ai.chronon.spark.analyzer

import ai.chronon.api.Builders.Derivation
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark._
import ai.chronon.spark.bootstrap.BootstrapUtils
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DerivationBootstrapTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = submission.SparkSessionBuilder.build("DerivationBootstrapTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  it should "bootstrap to derivations" in {
    val namespace = "test_derivations"
    tableUtils.createDatabase(namespace)
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)

    val derivation1 = Builders.Derivation(name = "user_amount_30d_avg", expression = "amount_dollars_sum_30d / 30")
    val derivation2 = Builders.Derivation(
      name = "*"
    )

    val groupByWithDerivation = groupBy
      .setDerivations(
        Seq(
          derivation1,
          derivation2
        ).toJava
      )
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupByWithDerivation)),
      rowIds = Seq("request_id"),
      externalParts = Seq(
        Builders.ExternalPart(
          Builders.ExternalSource(
            metadata = Builders.MetaData(name = "payments_service"),
            keySchema = StructType(name = "keys", fields = Array(StructField("user", StringType))),
            valueSchema = StructType(name = "values", fields = Array(StructField("user_txn_count_15d", LongType)))
          )
        ),
        Builders.ExternalPart(
          Builders.ContextualSource(
            fields = Array(StructField("user_txn_count_30d", LongType))
          )
        )
      ),
      derivations = Seq(
        Builders.Derivation(
          name = "*"
        ),
        // contextual feature rename
        Builders.Derivation(
          name = "user_txn_count_30d",
          expression = "ext_contextual_user_txn_count_30d"
        ),
        // derivation based on one external field (rename)
        Builders.Derivation(
          name = "user_txn_count_15d",
          expression = "ext_payments_service_user_txn_count_15d"
        ),
        // derivation based on one left field
        Builders.Derivation(
          name = "user_txn_count_15d_with_user_id",
          expression = "CONCAT(ext_payments_service_user_txn_count_15d, ' ', user)"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_30d",
          expression = "unit_test_user_transactions_amount_dollars_sum_30d"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_30d_avg",
          expression = "unit_test_user_transactions_user_amount_30d_avg"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_15d",
          expression = "unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        // derivation based on two group by fields
        Builders.Derivation(
          name = "user_amount_30d_minus_15d",
          expression =
            "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        // derivation based on one group by field and one contextual field
        Builders.Derivation(
          name = "user_amount_avg_30d",
          expression = "1.0 * unit_test_user_transactions_amount_dollars_sum_30d / ext_contextual_user_txn_count_30d"
        ),
        // derivation based on one group by field and one external field
        Builders.Derivation(
          name = "user_amount_avg_15d",
          expression =
            "1.0 * unit_test_user_transactions_amount_dollars_sum_15d / ext_payments_service_user_txn_count_15d"
        )
      ),
      metaData = Builders.MetaData(name = "test.derivations_join", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val outputDf = runner.computeJoin()

    assertTrue(
      outputDf.columns.toSet == Set(
        "user",
        "request_id",
        "ts",
        "user_txn_count_30d",
        "user_txn_count_15d",
        "user_txn_count_15d_with_user_id",
        "user_amount_30d",
        "user_amount_30d_avg",
        "user_amount_15d",
        "user_amount_30d_minus_15d",
        "user_amount_avg_30d",
        "user_amount_avg_15d",
        "ds"
      ))

    val leftTable = baseJoin.left.getEvents.table

    /* directly bootstrap a derived feature field */
    val rawDiffBootstrapDf = tableUtils
      .loadTable(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d_minus_15d"),
        col("ds")
      )
      .sample(0.8)

    val diffBootstrapTable = s"$namespace.bootstrap_diff"
    rawDiffBootstrapDf.save(diffBootstrapTable)
    val diffBootstrapDf = tableUtils.loadTable(diffBootstrapTable)
    val diffBootstrapRange = diffBootstrapDf.partitionRange
    val diffBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "user_amount_30d_minus_15d"),
        startPartition = diffBootstrapRange.start,
        endPartition = diffBootstrapRange.end
      ),
      table = diffBootstrapTable
    )

    /* bootstrap an external feature field such that it can be used in a downstream derivation */
    val rawExternalBootstrapDf = tableUtils
      .loadTable(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("ext_payments_service_user_txn_count_15d"),
        col("ds")
      )
      .sample(0.8)

    val externalBootstrapTable = s"$namespace.bootstrap_external"
    rawExternalBootstrapDf.save(externalBootstrapTable)
    val externalBootstrapDf = tableUtils.loadTable(externalBootstrapTable)
    val externalBootstrapRange = rawExternalBootstrapDf.partitionRange
    val externalBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "ext_payments_service_user_txn_count_15d"),
        startPartition = externalBootstrapRange.start,
        endPartition = externalBootstrapRange.end
      ),
      table = externalBootstrapTable
    )

    /* bootstrap an contextual feature field such that it can be used in a downstream derivation */
    val rawContextualBootstrapDf = tableUtils
      .loadTable(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_txn_count_30d"),
        col("ds")
      )
      .sample(0.8)

    val contextualBootstrapTable = s"$namespace.bootstrap_contextual"
    rawContextualBootstrapDf.save(contextualBootstrapTable)
    val contextualBootstrapDf = tableUtils.loadTable(contextualBootstrapTable)
    val contextualBootstrapRange = contextualBootstrapDf.partitionRange
    val contextualBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        // bootstrap of contextual fields will against the keys but it should be propagated to the values as well
        selects = Builders.Selects("request_id", "user_txn_count_30d"),
        startPartition = contextualBootstrapRange.start,
        endPartition = contextualBootstrapRange.end
      ),
      table = contextualBootstrapTable
    )

    /* construct final boostrap join with 3 bootstrap parts */
    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.derivations_join_w_bootstrap")
    bootstrapJoin
      .setBootstrapParts(
        Seq(
          diffBootstrapPart,
          externalBootstrapPart,
          contextualBootstrapPart
        ).toJava
      )

    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)
    val computed = runner2.computeJoin()

    // Comparison
    val expected = outputDf
      .join(diffBootstrapDf,
            outputDf("request_id") <=> diffBootstrapDf("request_id") and outputDf("ds") <=> diffBootstrapDf("ds"),
            "left")
      .join(
        externalBootstrapDf,
        outputDf("request_id") <=> externalBootstrapDf("request_id") and outputDf("ds") <=> externalBootstrapDf("ds"),
        "left")
      .join(contextualBootstrapDf,
            outputDf("request_id") <=> contextualBootstrapDf("request_id") and outputDf("ds") <=> contextualBootstrapDf(
              "ds"),
            "left")
      .select(
        outputDf("user"),
        outputDf("request_id"),
        outputDf("ts"),
        contextualBootstrapDf("user_txn_count_30d"),
        externalBootstrapDf("ext_payments_service_user_txn_count_15d").as("user_txn_count_15d"),
        (concat(externalBootstrapDf("ext_payments_service_user_txn_count_15d"), lit(' '), outputDf("user")))
          .as("user_txn_count_15d_with_user_id"),
        outputDf("user_amount_30d"),
        outputDf("user_amount_15d"),
        coalesce(diffBootstrapDf("user_amount_30d_minus_15d"), outputDf("user_amount_30d_minus_15d"))
          .as("user_amount_30d_minus_15d"),
        (outputDf("user_amount_30d") * lit(1.0) / contextualBootstrapDf("user_txn_count_30d"))
          .as("user_amount_avg_30d"),
        (outputDf("user_amount_15d") * lit(1.0) / externalBootstrapDf("ext_payments_service_user_txn_count_15d"))
          .as("user_amount_avg_15d"),
        (outputDf("user_amount_30d") * lit(1.0) / 30).as("user_amount_30d_avg"),
        outputDf("ds")
      )

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

  it should "bootstrap to derivations no star" in {
    val namespace = "test_derivations_no_star"
    tableUtils.createDatabase(namespace)

    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    val rawBootstrapDf = tableUtils
      .loadTable(queryTable)
      .select(
        col("request_id"),
        col("user"),
        col("ts"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d_minus_15d"),
        col("ds")
      )
    val bootstrapTable = s"$namespace.bootstrap_table"
    rawBootstrapDf.save(bootstrapTable)
    val bootstrapDf = tableUtils.loadTable(bootstrapTable)
    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "user_amount_30d", "user_amount_30d_minus_15d")
      ),
      table = bootstrapTable
    )

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(joinPart),
      derivations = Seq(
        Builders.Derivation(
          name = "user_amount_30d",
          expression = "unit_test_user_transactions_amount_dollars_sum_30d"
        ),
        Builders.Derivation(
          name = "user_amount_30d_minus_15d",
          expression =
            "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
        )
      ),
      bootstrapParts = Seq(
        bootstrapPart
      ),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.derivations_join_no_star", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(joinConf, today, tableUtils)
    val outputDf = runner.computeJoin()

    // assert that no computation happened for join part since all derivations have been bootstrapped
    assertFalse(tableUtils.tableReachable(joinConf.partOutputTable(joinPart)))

    val diff = Comparison.sideBySide(outputDf, bootstrapDf, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${outputDf.count()}")
      logger.info(s"Expected count: ${bootstrapDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }
}
