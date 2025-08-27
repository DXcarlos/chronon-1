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

import ai.chronon.spark.test.utils.DataFrameGen
import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.SparkSession
import ai.chronon.spark.catalog.TableUtils
import org.junit.Assert._

class SkipBloomFilterJoinBackfillTest extends BaseJoinTest {

  it should "test skip bloom filter join backfill" in {
    import ai.chronon.spark.submission
    val testSpark: SparkSession =
      submission.SparkSessionBuilder.build("JoinTest",
                                           local = true,
                                           additionalConfig =
                                             Some(Map("spark.chronon.backfill.bloomfilter.threshold" -> "100")))
    val testTableUtils = TableUtils(testSpark)
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_bloom_test"
    DataFrameGen.events(testSpark, viewsSchema, count = 100, partitions = 200).drop("ts").save(viewsTable)

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
      metaData = Builders.MetaData(name = "bloom_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_bloom_test"
    DataFrameGen
      .events(testSpark, itemQueries, 100, partitions = 100)
      .save(itemQueriesTable)

    val start = testTableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_bloom_test", namespace = namespace, team = "chronon")
    )
    val skipBloomComputed =
      new ai.chronon.spark.Join(joinConf = joinConf, endPartition = today, tableUtils = testTableUtils).computeJoin()
    val leftSideCount = testSpark.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start'").count()
    println("computed count: " + skipBloomComputed.count())
    assertEquals(leftSideCount, skipBloomComputed.count())
  }
}
