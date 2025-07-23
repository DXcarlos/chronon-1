package ai.chronon.spark.test.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch._
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.test.{DataFrameGen, TableTestUtils}
import ai.chronon.spark.{GroupBy, Join, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, rand}
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import ai.chronon.planner.{JoinBootstrapNode, JoinDerivationNode, JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import org.slf4j.LoggerFactory

class ShortNamesTest extends AnyFlatSpec {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = SparkSessionBuilder.build("LabelJoinV2Test", local = true)

  private implicit val tableUtils = TableTestUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
  private val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  val namespace = "test_short_names"
  tableUtils.createDatabase(namespace)

  it should "test monolith join short names" in {

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 2000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders
      .Join(
        left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
        joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user").setUseLongNames(false)),
        metaData = Builders.MetaData(name = "test.item_snapshot_features", namespace = namespace, team = "chronon")
      )
      .setUseLongNames(false)

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    val outputColumns = computed.columns
    println(outputColumns.mkString("Output columns::[", ", ", "]"))

    // prefix + key + col_name
    assert(computed.columns.contains("user_item_time_spent_ms_average"))
  }

  it should "test modular with short names" in {
    val dollarTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
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
    DataFrameGen.entities(spark, dollarTransactions, 1000, partitions = 100).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 1000, partitions = 40).save(rupeeTable)

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

    // println("Rupee Source start partition $month")
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
      metaData = Builders.MetaData(name = "unit_test.user_transactions_1", namespace = namespace, team = "chronon")
    )

    val queriesSchema = List(
      Column("user_name", api.StringType, 10),
      Column("user", api.StringType, 10)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 2000, partitions = 90, partitionColumn = Some("date"))
      .save(queryTable, partitionColumns = Seq("date"))

    // Make bootstrap part and table
    val bootstrapSourceTable = s"$namespace.bootstrap"
    val bootstrapCol = "user_amount_dollars_sum_10d"
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
      metaData = Builders.MetaData(name = "unit_test.user_transactions2", namespace = namespace, team = "chronon")
    )

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("user", "ts", "user_amount_dollars_sum_10d"),
        startPartition = start,
        endPartition = today
      ),
      table = s"$namespace.bootstrap",
      keyColumns = Seq("user", "ts")
    )

    val jp1 = Builders
      .JoinPart(
        groupBy = groupBy,
        keyMapping = Map("user_name" -> "user", "user" -> "user_name")
      )
      .setUseLongNames(false)

    val jp2 = Builders.JoinPart(groupBy = groupBy2).setUseLongNames(false)

    val returnOneSource = Builders.ExternalSource(
      metadata = Builders.MetaData(
        name = "return_one"
      ),
      keySchema = StructType("key_one", Array(StructField("key_number", IntType))),
      valueSchema = StructType("value_one", Array(StructField("value_number", IntType)))
    )

    val joinConf: ai.chronon.api.Join = Builders
      .Join(
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
        joinParts = Seq(jp1, jp2, Builders.JoinPart(groupBy = bootstrapGroupBy).setUseLongNames(false)),
        bootstrapParts = Seq(bootstrapPart), // ext_return_one_number
        derivations = Seq(
          Builders.Derivation("ratio_derivation",
                              "user_amount_dollars_sum / (COALESCE(user_user_name_amount_dollars_sum_30d, 0) + 1)"),
          Builders.Derivation("external_coalesce", "COALESCE(ext_return_one_value_number, 1)")
        ),
        externalParts = Seq(Builders.ExternalPart(returnOneSource)),
        metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
      )
      .setUseLongNames(false)

    // test eval
    val eval = new Eval()(tableUtils)
    val evalResult = eval.evalJoin(joinConf)

    assertEquals(
      evalResult.rightPartsSchema.toScala,
      Map(
        "user_user_name_amount_dollars_sum_30d" -> "LongType",
        "user_amount_dollars_sum" -> "LongType",
        "user_amount_dollars_sum_10d" -> "LongType"
      )
    )

    assertEquals(evalResult.derivationsSchema.toScala,
                 Map(
                   "ratio_derivation" -> "DoubleType",
                   "external_coalesce" -> "IntType"
                 ))

    assertEquals(evalResult.externalPartsSchema.toScala,
                 Map(
                   "ext_return_one_value_number" -> "IntType"
                 ))

    val leftSourceWithFilter = new SourceWithFilterNode().setSource(joinConf.left)

    // First run the SourceJob associated with the left
    // Compute source table name using utility function
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
      .setEndDate(today)

    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceMetaData, sourceJobRange)
    sourceRunner.run()
    tableUtils.sql(s"SELECT * FROM $sourceOutputTable").show()
    val sourceExpected = spark.sql(s"SELECT *, date as ds FROM $queryTable WHERE date >= '$start' AND date <= '$today'")
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

    // Now run the bootstrap part to get the bootstrap table (one of the joinParts)
    val bootstrapOutputTable = joinConf.metaData.bootstrapTable
    val bootstrapJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Split bootstrap output table
    val bootstrapParts = bootstrapOutputTable.split("\\.", 2)
    val bootstrapNamespace = bootstrapParts(0)
    val bootstrapName = bootstrapParts(1)

    // Create metadata for bootstrap job
    val bootstrapMetaData = new api.MetaData()
      .setName(bootstrapName)
      .setOutputNamespace(bootstrapNamespace)

    val bootstrapNode = new JoinBootstrapNode()
      .setJoin(joinConf)

    val bsj = new JoinBootstrapJob(bootstrapNode, bootstrapMetaData, bootstrapJobRange)
    bsj.run()
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
        "user_amount_dollars_sum_10d",
        "key_number",
        "ext_return_one_value_number",
        "ds"
      )
    assertEquals(expectedSchema, boostrapSchema)
    tableUtils.sql(s"SELECT * FROM $bootstrapOutputTable").show()

    // Now run the join part job that *does not* have a bootstrap
    // Use RelevantLeftForJoinPart to get the full table name (including namespace)
    val joinPart1TableName = planner.RelevantLeftForJoinPart.partTableName(joinConf, jp1)
    val outputNamespace = joinConf.metaData.outputNamespace
    val joinPart1FullTableName = planner.RelevantLeftForJoinPart.fullPartTableName(joinConf, jp1)

    val joinPartJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Create metadata with name and namespace directly
    val metaData = new api.MetaData()
      .setName(joinPart1TableName)
      .setOutputNamespace(outputNamespace)

    val joinPartNode = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinConf.getLeft.dataModel)
      .setJoinPart(jp1)

    val joinPartJob = new JoinPartJob(joinPartNode, metaData, joinPartJobRange)
    joinPartJob.run()
    tableUtils.sql(s"SELECT * FROM $joinPart1FullTableName").show()

    // Now run the join part job that *does not* have a bootstrap
    // Use RelevantLeftForJoinPart to get the appropriate output table name
    val joinPart2TableName = planner.RelevantLeftForJoinPart.partTableName(joinConf, jp2)
    val joinPart2FullTableName = planner.RelevantLeftForJoinPart.fullPartTableName(joinConf, jp2)

    val metaData2 = new api.MetaData()
      .setName(joinPart2TableName)
      .setOutputNamespace(outputNamespace)

    val joinPartNode2 = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinConf.getLeft.dataModel)
      .setJoinPart(jp2)

    val joinPart2Job = new JoinPartJob(joinPartNode2, metaData2, joinPartJobRange)
    joinPart2Job.run()
    tableUtils.sql(s"SELECT * FROM $joinPart2FullTableName").show()

    // Skip the joinPart that does have a bootstrap, and go straight to merge job
    val mergeJobOutputTable = joinConf.metaData.outputTable

    val mergeJobRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Create metadata for merge job
    val mergeMetaData = new api.MetaData()
      .setName(joinConf.metaData.name)
      .setOutputNamespace(namespace)

    val mergeNode = new JoinMergeNode()
      .setJoin(joinConf)

    val finalJoinJob = new MergeJob(mergeNode, mergeMetaData, mergeJobRange, Seq(jp1, jp2))
    finalJoinJob.run()
    tableUtils.sql(s"SELECT * FROM $mergeJobOutputTable").show()

    // Now run the derivations job
    val derivationOutputTable = s"$namespace.test_user_transaction_features_v1_derived"

    val derivationRange = new DateRange()
      .setStartDate(start)
      .setEndDate(today)

    // Split derivation output table
    val derivationParts = derivationOutputTable.split("\\.", 2)
    val derivationNamespace = derivationParts(0)
    val derivationName = derivationParts(1)

    // Create metadata for derivation job
    val derivationMetaData = new api.MetaData()
      .setName(derivationName)
      .setOutputNamespace(derivationNamespace)

    val derivationNode = new JoinDerivationNode()
      .setJoin(joinConf)

    val joinDerivationJob = new JoinDerivationJob(derivationNode, derivationMetaData, derivationRange)
    joinDerivationJob.run()
    tableUtils.sql(s"SELECT * FROM $derivationOutputTable").show()

    val expectedQuery = s"""
                   |WITH
                   |   queries AS (
                   |     SELECT user_name,
                   |         user,
                   |         ts,
                   |         date as ds
                   |     from $queryTable
                   |     where user_name IS NOT null
                   |         AND user IS NOT NULL
                   |         AND ts IS NOT NULL
                   |         AND date IS NOT NULL
                   |         AND date >= '$start'
                   |         AND date <= '$today')
                   |  SELECT
                   |    queries.user,
                   |    queries.ts,
                   |    queries.ds,
                   |    SUM(IF(dollar.ts < queries.ts, dollar.amount_dollars, null)) / 1 as ratio_derivation,
                   |    1 as external_coalesce
                   |  FROM queries
                   |  LEFT OUTER JOIN $dollarTable as dollar
                   |  on queries.user == dollar.user
                   |  GROUP BY queries.user, queries.ts, queries.ds
                   |""".stripMargin

    spark.sql(expectedQuery).show()
    val expected = spark.sql(expectedQuery)
    val computed = spark.sql(s"SELECT user, ts, ds, ratio_derivation, external_coalesce FROM $derivationOutputTable")

    val finalDiff = Comparison.sideBySide(computed, expected, List("user", "ts", "ds"))

    if (finalDiff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

  }

}
