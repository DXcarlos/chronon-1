package ai.chronon.spark.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.planner._
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.batch._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.apache.spark.sql.functions._
import org.junit.Assert._

class ModularJoinTest extends SparkTestBase {

  private implicit val tableUtils: TableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  // Use 3 days ago as the end date to ensure data is always generated
  private val threeDaysAgo = tableUtils.partitionSpec.minus(today, new Window(3, TimeUnit.DAYS))
  val start = tableUtils.partitionSpec.minus(threeDaysAgo, new Window(60, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(threeDaysAgo, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(threeDaysAgo, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  private val namespace = "test_namespace_jointest_modular"
  createDatabase(namespace)

  it should "test a join with bootstrap/derivation/external part/events/entity" in {
    val dollarTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    spark.sql(s"DROP TABLE IF EXISTS $rupeeTable")
    DataFrameGen.events(spark, dollarTransactions, 2000, partitions = 80).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 3000, partitions = 80).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    val dollarEventSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      table = dollarTable
    )

    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts",
                        "amount_dollars" -> "CAST(amount_rupees/70 as long)",
                        "user_name" -> "user_name",
                        "user" -> "user"),
          startPartition = monthAgo,
          setups = Seq(
            "create temporary function temp_replace_right_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user", "user_name"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val groupBy2 = Builders.GroupBy(
      sources = Seq(dollarEventSource),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount_dollars")),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val queriesSchema = List(
      Column("user_name", api.StringType, 10),
      Column("user", api.StringType, 10)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 4000, partitions = 100, partitionColumn = Some("date"))
      .save(queryTable, partitionColumns = Seq("date"))

    // Make bootstrap part and table
    val bootstrapSourceTable = s"$namespace.bootstrap"
    val bootstrapCol = "unit_test_user_transactions_amount_dollars_sum_10d"
    tableUtils
      .loadTable(queryTable)
      .select(
        col("user"),
        col("ts"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as(bootstrapCol),
        col("date").as("ds")
      )
      .save(bootstrapSourceTable)

    val bootstrapGroupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(10, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("user", "ts", "unit_test_user_transactions_amount_dollars_sum_10d"),
        startPartition = start,
        endPartition = threeDaysAgo
      ),
      table = s"$namespace.bootstrap",
      keyColumns = Seq("user", "ts")
    )

    val jp1 = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user", "user" -> "user_name"))

    val jp2 = Builders.JoinPart(groupBy = groupBy2)

    val returnOneSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "return_one"
      ),
      keySchema = StructType("key_one", Array(StructField("key_number", IntType))),
      valueSchema = StructType("value_one", Array(StructField("value_number", IntType)))
    )

    val joinConf: ai.chronon.api.Join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          startPartition = start,
          setups = Seq(
            "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          ),
          partitionColumn = "date"
        ),
        table = queryTable
      ),
      joinParts = Seq(jp1, jp2, Builders.JoinPart(groupBy = bootstrapGroupBy)),
      bootstrapParts = Seq(bootstrapPart), // ext_return_one_number
      derivations = Seq(
        Builders.Derivation(
          "ratio_derivation",
          "unit_test_user_transactions_amount_dollars_sum / (COALESCE(unit_test_user_transactions_amount_dollars_sum_30d, 0) + 1)"),
        Builders.Derivation("external_coalesce", "COALESCE(ext_return_one_value_number, 1)")
      ),
      externalParts = Seq(Builders.ExternalPart(returnOneSource)),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    // Run the entire pipeline using ModularMonolith
    val dateRange = new DateRange()
      .setStartDate(start)
      .setEndDate(threeDaysAgo)

    val modularMonolith = new ModularMonolith(joinConf, dateRange)
    modularMonolith.run()

    // Verify source output
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(joinConf)
    val sourceExpected = spark.sql(s"SELECT *, date as ds FROM $queryTable WHERE date >= '$start' AND date <= '$threeDaysAgo'")
    val sourceComputed = tableUtils.sql(s"SELECT * FROM $sourceOutputTable").drop("ts_ds")
    val diff = Comparison.sideBySide(sourceComputed, sourceExpected, List("user_name", "user", "ts"))
    if (diff.count() > 0) {
      println(s"Actual count: ${sourceComputed.count()}")
      println(s"Expected count: ${sourceExpected.count()}")
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // Verify bootstrap table
    val bootstrapOutputTable = joinConf.metaData.bootstrapTable
    val sourceCount = tableUtils.sql(s"SELECT * FROM $sourceOutputTable").count()
    val bootstrapCount = tableUtils.sql(s"SELECT * FROM $bootstrapOutputTable").count()
    assertEquals(sourceCount, bootstrapCount)
    val boostrapSchema = tableUtils.sql(s"SELECT * FROM $bootstrapOutputTable").schema.map(_.name)
    val expectedSchema =
      Seq(
        "user",
        "ts",
        "user_name",
        "ts_ds",
        "matched_hashes",
        "unit_test_user_transactions_amount_dollars_sum_10d",
        "key_number",
        "ext_return_one_value_number",
        "ds"
      )
    assertEquals(expectedSchema, boostrapSchema)

    // Verify join part tables were created
    val joinPart1FullTableName = planner.RelevantLeftForJoinPart.fullPartTableName(joinConf, jp1)
    val joinPart2FullTableName = planner.RelevantLeftForJoinPart.fullPartTableName(joinConf, jp2)
    assertTrue(tableUtils.tableReachable(joinPart1FullTableName))
    assertTrue(tableUtils.tableReachable(joinPart2FullTableName))

    // Verify merge job output
    val mergeJobOutputTable = joinConf.metaData.outputTable
    assertTrue(tableUtils.tableReachable(mergeJobOutputTable))

    // Verify derivation job output
    val derivationOutputTable = s"$namespace.test_user_transaction_features__derived"

    val expectedQuery = s"""
                |WITH
                |   queries AS (
                |     SELECT user_name,
                |         user,
                |         ts,
                |         date as ds
                |     from $queryTable
                |     where date >= '$start'
                |         AND date <= '$threeDaysAgo')
                |  SELECT
                |    queries.user,
                |    queries.ts,
                |    queries.ds,
                |    SUM(IF(dollar.ts < CAST(DATEDIFF(queries.ds, '1970-01-01') AS BIGINT) * 86400000, dollar.amount_dollars, null)) / 1 as ratio_derivation,
                |    1 as external_coalesce
                |  FROM queries
                |  LEFT OUTER JOIN $dollarTable as dollar
                |  on queries.user == dollar.user
                |    AND dollar.ds >= '$yearAgo'
                |    AND dollar.ds <= '$dayAndMonthBefore'
                |  GROUP BY queries.user, queries.ts, queries.ds
                |""".stripMargin
    val expected = spark.sql(expectedQuery)
    val computed = spark.sql(s"SELECT user, ts, ds, ratio_derivation, external_coalesce FROM $derivationOutputTable")

    val finalDiff = Comparison.sideBySide(computed, expected, List("user", "ts", "ds"))

    if (finalDiff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${finalDiff.count()}")
      println("diff result rows")
      finalDiff.show(50, truncate = false)
    }
    assertEquals(0, finalDiff.count())
  }
}
