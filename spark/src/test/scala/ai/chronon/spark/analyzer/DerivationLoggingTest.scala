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
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark._
import ai.chronon.spark.bootstrap.BootstrapUtils
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SchemaEvolutionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DerivationLoggingTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = submission.SparkSessionBuilder.build("DerivationLoggingTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  it should "logging non star" in {
    runLoggingTest("test_derivations_logging_non_star", wildcardSelection = false)
  }

  it should "logging" in {
    runLoggingTest("test_derivations_logging", wildcardSelection = true)
  }

  private def runLoggingTest(namespace: String, wildcardSelection: Boolean): Unit = {
    tableUtils.createDatabase(namespace)

    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val endDs = tableUtils.loadTable(queryTable).select(max(tableUtils.partitionColumn)).head().getString(0)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query(
          startPartition = endDs
        )
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          Builders.ContextualSource(
            Array(StructField("request_id", StringType))
          )
        )),
      joinParts = Seq(joinPart),
      derivations = (if (wildcardSelection) {
                       Seq(Derivation("*", "*"))
                     } else {
                       Seq.empty
                     }) :+ Builders.Derivation(
        name = "user_amount_30d_minus_15d",
        expression =
          "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
      ),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.derivations_logging", namespace = namespace, team = "chronon")
    )

    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.derivations_logging.bootstrap_copy")
    bootstrapJoin.setBootstrapParts(
      Seq(
        Builders.BootstrapPart(
          table = bootstrapJoin.metaData.loggedTable
        )
      ).toJava
    )

    // Init artifacts to run online fetching and logging
    val kvStore = OnlineUtils.buildInMemoryKVStore(namespace)
    val mockApi = new MockApi(() => kvStore, namespace)
    OnlineUtils.serve(tableUtils, kvStore, () => kvStore, namespace, endDs, groupBy)
    val fetcher = mockApi.buildFetcher(debug = true)

    val metadataStore = fetcher.metadataStore
    kvStore.create(Constants.MetadataDataset)
    metadataStore.putJoinConf(bootstrapJoin)

    val requests = tableUtils
      .loadTable(queryTable)
      .select("user", "request_id", "ts")
      .collect()
      .map { row =>
        val (user, requestId, ts) = (row.getLong(0), row.getString(1), row.getLong(2))
        Request(bootstrapJoin.metaData.name,
                Map(
                  "user" -> user,
                  "request_id" -> requestId
                ).asInstanceOf[Map[String, AnyRef]],
                atMillis = Some(ts))
      }
    val future = fetcher.fetchJoin(requests)
    val responses = Await.result(future, Duration.Inf).toArray

    // Populate log table
    val logs = mockApi.flushLoggedValues
    assertEquals(requests.length, responses.length)
    assertEquals(1 + requests.length, logs.length)
    mockApi
      .loggedValuesToDf(logs, spark)
      .save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))
    SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, endDs)
    val flattenerJob = new LogFlattenerJob(spark, bootstrapJoin, endDs, mockApi.logTable, mockApi.schemaTable)
    flattenerJob.buildLogTable()
    val logDf = tableUtils.loadTable(bootstrapJoin.metaData.loggedTable)

    // Verifies that logging is full regardless of select star
    val baseColumns = Seq(
      "unit_test_user_transactions_amount_dollars_sum_15d",
      "unit_test_user_transactions_amount_dollars_sum_30d"
    )
    assertTrue(baseColumns.forall(logDf.columns.contains))

    val bootstrapJoinJob = new ai.chronon.spark.Join(bootstrapJoin, endDs, tableUtils)
    val computedDf = bootstrapJoinJob.computeJoin()
    if (!wildcardSelection) {
      assertTrue(baseColumns.forall(c => !computedDf.columns.contains(c)))
    }

    // assert that no computation happened for join part since all derivations have been bootstrapped
    assertFalse(tableUtils.tableReachable(bootstrapJoin.partOutputTable(joinPart)))

    val baseJoinJob = new ai.chronon.spark.Join(baseJoin, endDs, tableUtils)
    val baseDf = baseJoinJob.computeJoin()

    val expectedDf = JoinUtils
      .coalescedJoin(
        leftDf = logDf,
        rightDf = baseDf,
        keys = Seq("request_id", "user", "ts", "ds"),
        joinType = "right"
      )
      .drop("schema_hash")

    val diff = Comparison.sideBySide(computedDf, expectedDf, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computedDf.count()}")
      logger.info(s"Expected count: ${expectedDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }

  it should "contextual" in {
    val namespace = "test_contextual"
    tableUtils.createDatabase(namespace)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val bootstrapDf = tableUtils
      .loadTable(queryTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("context_1"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("context_2"),
        col("ds")
      )
      .withColumn("ext_contextual_context_1", col("context_1"))
      .withColumn("ext_contextual_context_2", col("context_2"))

    val bootstrapTable = s"$namespace.bootstrap_table"
    bootstrapDf.save(bootstrapTable)

    def buildJoinConf(derivations: Seq[Derivation], name: String): ai.chronon.api.Join = {
      Builders.Join(
        left = Builders.Source.events(
          table = queryTable,
          query = Builders.Query()
        ),
        joinParts = Seq(),
        externalParts = Seq(
          Builders.ExternalPart(
            Builders.ContextualSource(
              fields = Array(
                StructField("context_1", LongType),
                StructField("context_2", LongType)
              )
            )
          )),
        derivations = derivations,
        // to simulate log-based bootstrap, and assumption is that logs will contain all columns
        bootstrapParts = Seq(Builders.BootstrapPart(table = bootstrapTable)),
        rowIds = Seq("request_id"),
        metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon")
      )
    }
    def getSchema(joinConf: ai.chronon.api.Join): Seq[String] = {
      val runner = new ai.chronon.spark.Join(joinConf, today, tableUtils)
      val outputDf = runner.computeJoin()
      outputDf.columns
    }

    /* when no derivations are present, we keep the values and discard the keys */
    val schema1 = getSchema(buildJoinConf(Seq(), "test_1"))
    assertFalse(schema1.contains("context_1"))
    assertTrue(schema1.contains("ext_contextual_context_1"))
    assertFalse(schema1.contains("context_2"))
    assertTrue(schema1.contains("ext_contextual_context_2"))

    /*
     * In order to keep the `key` format, use explicit rename derivation
     * Otherwise, in a * derivation, we keep only the values and discard the keys
     */
    val schema2 = getSchema(
      buildJoinConf(
        Seq(
          Builders.Derivation(
            name = "context_1",
            expression = "ext_contextual_context_1"
          ),
          Builders.Derivation(
            name = "*",
            expression = "*"
          )
        ),
        "test_2"
      ))

    assertTrue(schema2.contains("context_1"))
    assertFalse(schema2.contains("ext_contextual_context_1"))
    assertFalse(schema2.contains("context_2"))
    assertTrue(schema2.contains("ext_contextual_context_2"))

    /*
     * In order to keep the `key` format, use explicit rename derivation
     * Without the * derivation, the other columns are all discarded
     */
    val schema3 = getSchema(
      buildJoinConf(
        Seq(
          Builders.Derivation(
            name = "context_1",
            expression = "ext_contextual_context_1"
          )
        ),
        "test_3"
      ))

    assertTrue(schema3.contains("context_1"))
    assertFalse(schema3.contains("ext_contextual_context_1"))
    assertFalse(schema3.contains("context_2"))
    assertFalse(schema3.contains("ext_contextual_context_2"))

    /*
     * If we want to keep both format, select both format explicitly
     */
    val schema4 = getSchema(
      buildJoinConf(
        Seq(
          Builders.Derivation(
            name = "context_1",
            expression = "ext_contextual_context_1"
          ),
          Builders.Derivation(
            name = "ext_contextual_context_1",
            expression = "ext_contextual_context_1"
          )
        ),
        "test_4"
      ))

    assertTrue(schema4.contains("context_1"))
    assertTrue(schema4.contains("ext_contextual_context_1"))
    assertFalse(schema4.contains("context_2"))
    assertFalse(schema4.contains("ext_contextual_context_2"))
  }

  it should "group by derivations" in {
    val namespace = "test_group_by_derivations"
    tableUtils.createDatabase(namespace)
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    groupBy.setBackfillStartDate(today)
    groupBy.setDerivations(
      Seq(Builders.Derivation(name = "*"),
          Builders.Derivation(
            name = "amount_dollars_avg_15d",
            expression = "amount_dollars_sum_15d / 15"
          )).toJava)
    ai.chronon.spark.GroupBy.computeBackfill(groupBy, today, tableUtils)
    val actualDf = tableUtils.sql(s"""
         |select * from $namespace.${groupBy.metaData.cleanName}
         |""".stripMargin)

    val expectedDf = tableUtils.sql(s"""
         |select
         |  user,
         |  amount_dollars_sum_30d,
         |  amount_dollars_sum_15d,
         |  amount_dollars_sum_15d / 15 as amount_dollars_avg_15d,
         |  ds
         |from $namespace.${groupBy.metaData.cleanName}
         |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${actualDf.count()}")
      logger.info(s"Expected count: ${expectedDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }
}
