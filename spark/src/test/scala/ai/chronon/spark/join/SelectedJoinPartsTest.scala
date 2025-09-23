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
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.api.{Accuracy, Builders, Operation}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.DataFrameGen
import org.apache.spark.sql.AnalysisException
import org.junit.Assert._

class SelectedJoinPartsTest extends BaseJoinTest {

  /** Create a event table as left side, 3 group bys as right side.
    * Generate data using DataFrameGen and save to the tables.
    * Create a join with only one join part selected.
    * Run computeJoin().
    * Check if the selected join part is computed and the other join parts are not computed.
    */
  it should "test selected join parts" in {
    // Left
    val itemQueries = List(
      Column("item", api.StringType, 10),
      Column("value", api.LongType, 10)
    )
    val itemQueriesTable = s"$namespace.item_queries_selected_join_parts"
    spark.sql(s"DROP TABLE IF EXISTS $itemQueriesTable")
    spark.sql(s"DROP TABLE IF EXISTS ${itemQueriesTable}_tmp")
    DataFrameGen.events(spark, itemQueries, 100, partitions = 30).save(s"${itemQueriesTable}_tmp")
    val leftDf = tableUtils.sql(s"SELECT item, value, ts, ds FROM ${itemQueriesTable}_tmp")
    leftDf.save(itemQueriesTable)
    val start = monthAgo

    // Right
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("value", api.LongType, 100)
    )
    val viewsTable = s"$namespace.view_selected_join_parts"
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    DataFrameGen.events(spark, viewsSchema, count = 100, partitions = 30).save(viewsTable)

    // Group By
    val gb1 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = viewsTable,
          query = Builders.Query(startPartition = start)
        )),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "10"), inputColumn = "user"),
        Builders.Aggregation(operation = Operation.MAX, argMap = Map("k" -> "2"), inputColumn = "value")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_selected_join_parts_1",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val gb2 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = viewsTable,
          query = Builders.Query(startPartition = start)
        )),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.MIN, argMap = Map("k" -> "1"), inputColumn = "value")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_selected_join_parts_2",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val gb3 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = viewsTable,
          query = Builders.Query(startPartition = start)
        )),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "value")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_selected_join_parts_3",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val jp1 = Builders.JoinPart(groupBy = gb1, prefix = "user1")
    val jp2 = Builders.JoinPart(groupBy = gb2, prefix = "user2")
    val jp3 = Builders.JoinPart(groupBy = gb3, prefix = "user3")

    // Join
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(jp1, jp2, jp3),
      metaData = Builders.MetaData(name = "unit_test.item_temporal_features.selected_join_parts",
                                   namespace = namespace,
                                   team = "item_team",
                                   online = true)
    )

    // Drop Join Part tables if any
    val partTable1 = RelevantLeftForJoinPart.fullPartTableName(joinConf, jp1)
    val partTable2 = RelevantLeftForJoinPart.fullPartTableName(joinConf, jp2)
    val partTable3 = RelevantLeftForJoinPart.fullPartTableName(joinConf, jp3)
    spark.sql(s"DROP TABLE IF EXISTS $partTable1")
    spark.sql(s"DROP TABLE IF EXISTS $partTable2")
    spark.sql(s"DROP TABLE IF EXISTS $partTable3")

    // Compute daily join.
    val joinJob = new ai.chronon.spark.Join(joinConf = joinConf,
                                            endPartition = today,
                                            tableUtils = tableUtils,
                                            selectedJoinParts =
                                              Some(List("user1_unit_test_item_views_selected_join_parts_1")))

    joinJob.computeJoinOpt()

    val part1 = tableUtils.sql(s"SELECT * FROM $partTable1")
    assertTrue(part1.count() > 0)

    val thrown2 = intercept[AnalysisException] {
      spark.sql(s"SELECT * FROM $partTable2")
    }
    val thrown3 = intercept[AnalysisException] {
      spark.sql(s"SELECT * FROM $partTable3")
    }
    val notFoundString35 = "TABLE_OR_VIEW_NOT_FOUND" // spark 3.5
    val notFoundStringOld = "Table or view not found" // spark old versions
    assert(
      thrown2.getMessage.contains(notFoundString35) && thrown3.getMessage.contains(notFoundString35) ||
        thrown2.getMessage.contains(notFoundStringOld) && thrown3.getMessage.contains(notFoundStringOld))
  }
}
