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

package ai.chronon.spark.test.join

import ai.chronon.spark.test.utils.{DataFrameGen, TableTestUtils}
import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, Operation, TimeUnit, Window}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.SparkSession
import org.junit.Assert._

class EventsEventsTemporalTest extends BaseJoinTest {

  val sparkSkewFree: SparkSession = submission.SparkSessionBuilder.build(
    "JoinTest",
    local = true,
    additionalConfig = Option(Map("spark.chronon.join.backfill.mode.skewFree" -> "true"))
  )
  protected implicit val tableUtilsSkewFree: TableTestUtils = TableTestUtils(sparkSkewFree)

  it should "test events events temporal" in {
    val joinConf = getEventsEventsTemporal("temporal")
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_temporal"
    DataFrameGen
      .events(sparkSkewFree, viewsSchema, count = 100, partitions = 200)
      .save(viewsTable, Map("tblProp1" -> "1"))

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo)
    )
    Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace)
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 10))
    val itemQueriesTable = s"$namespace.item_queries"
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
                                     | SELECT queries.item, queries.ts, queries.ds, part.user_unit_test_item_views_ts_min, part.user_unit_test_item_views_ts_max, part.user_unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
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
        .replaceWithReadableTime(Seq("ts", "a_user_unit_test_item_views_ts_max", "b_user_unit_test_item_views_ts_max"),
                                 dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }
}
