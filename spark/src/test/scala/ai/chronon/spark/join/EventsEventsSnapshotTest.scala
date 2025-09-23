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

package ai.chronon.spark.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.DataFrameGen
import org.junit.Assert._

class EventsEventsSnapshotTest extends BaseJoinTest {

  it should "test events events snapshot" in {
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events"
    DataFrameGen.events(spark, viewsSchema, count = 100, partitions = 200).drop("ts").save(viewsTable)

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
      .events(spark, itemQueries, 100, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = monthAgo, tableUtils = tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$monthAgo')
                                     | SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        AVG(IF(queries.ds > $viewsTable.ds, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
                                     | FROM queries left outer join $viewsTable
                                     |  ON queries.item = $viewsTable.item
                                     | WHERE ($viewsTable.item IS NOT NULL) AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }
}
