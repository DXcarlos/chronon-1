package ai.chronon.spark.test.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, Operation, TimeUnit, Window}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.utils.{DataFrameGen, TableTestUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.Assert._

class EventsEventsTemporalWithGBDerivation extends BaseJoinTest {

  val sparkSkewFree: SparkSession = submission.SparkSessionBuilder.build(
    "JoinTest",
    local = true,
    additionalConfig = Option(Map("spark.chronon.join.backfill.mode.skewFree" -> "true"))
  )
  protected implicit val tableUtilsSkewFree: TableTestUtils = TableTestUtils(sparkSkewFree)

  it should "test events events temporal with GroupBy derivations" in {
    val joinConf = getEventsEventsTemporalWithGBDerivations("temporal_with_derivations")
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_temporal_with_derivations"
    DataFrameGen
      .events(sparkSkewFree, viewsSchema, count = 100, partitions = 200)
      .save(viewsTable, Map("tblProp1" -> "1"))

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo)
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 10))
    val itemQueriesTable = s"$namespace.item_queries_with_derivations"
    val itemQueriesDf = DataFrameGen
      .events(sparkSkewFree, itemQueries, 100, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) // .union(itemQueriesDf)

    val start = tableUtilsSkewFree.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    (new Analyzer(tableUtilsSkewFree, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtilsSkewFree)
    val computed = join.computeJoin(Some(100))
    computed.show()

    val expected = tableUtilsSkewFree.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, 
                                     |        part.user_unit_test_item_views_with_derivations_ts_min, 
                                     |        part.user_unit_test_item_views_with_derivations_ts_max, 
                                     |        part.user_unit_test_item_views_with_derivations_time_spent_ms_average,
                                     |        part.user_unit_test_item_views_with_derivations_time_spent_mins,
                                     |        part.user_unit_test_item_views_with_derivations_session_duration,
                                     |        part.user_unit_test_item_views_with_derivations_session_hours
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_with_derivations_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_with_derivations_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_unit_test_item_views_with_derivations_time_spent_ms_average,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) / 60000 as user_unit_test_item_views_with_derivations_time_spent_mins,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) - MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_with_derivations_session_duration,
                                     |        (MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) - MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null))) / 3600000 as user_unit_test_item_views_with_derivations_session_hours
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtilsSkewFree.sql(
        s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts",
                                     "a_user_unit_test_item_views_with_derivations_ts_max",
                                     "b_user_unit_test_item_views_with_derivations_ts_max"),
                                 dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  // Helper method that creates a join with GroupBy derivations (similar to getEventsEventsTemporal but with derivations)
  protected def getEventsEventsTemporalWithGBDerivations(nameSuffix: String = "") = {
    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_with_derivations"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) // .union(itemQueriesDf)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val suffix = if (nameSuffix.isEmpty) "" else s"_$nameSuffix"
    Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = getViewsGroupByWithDerivations(nameSuffix), prefix = "user")),
      metaData = Builders.MetaData(name = s"test.item_temporal_features_with_derivations${suffix}",
                                   namespace = namespace,
                                   team = "item_team")
    )
  }

  // Helper method that creates a GroupBy with derivations (similar to getViewsGroupBy but with derivations)
  protected def getViewsGroupByWithDerivations(suffix: String) = {
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$suffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 100, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo)
    )

    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    df.save(viewsTable, Map("tblProp1" -> "1"))

    Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      derivations = Seq(
        Builders.Derivation(name = "*"), // Include all base aggregations
        Builders.Derivation(name = "time_spent_mins", expression = "time_spent_ms_average / 60000"),
        Builders.Derivation(name = "session_duration", expression = "ts_max - ts_min"),
        Builders.Derivation(name = "session_hours", expression = "(ts_max - ts_min) / 3600000")
      ),
      metaData =
        Builders.MetaData(name = "unit_test.item_views_with_derivations", namespace = namespace, team = "item_team"),
      accuracy = api.Accuracy.TEMPORAL
    )
  }
}
