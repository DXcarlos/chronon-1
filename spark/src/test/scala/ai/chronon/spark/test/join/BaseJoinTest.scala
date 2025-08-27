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

import ai.chronon.spark.test.utils.TableTestUtils
import ai.chronon.spark.test.utils.DataFrameGen
import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Tag

object JoinTag extends Tag("join")

case class TestRow(ds: String, value: String) {}

object TestRow {
  implicit def ordering[A <: TestRow]: Ordering[A] =
    new Ordering[A] {
      override def compare(x: A, y: A): Int = {
        x.ds.compareTo(y.ds)
      }
    }
}

abstract class BaseJoinTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  val spark: SparkSession = submission.SparkSessionBuilder.build("JoinTest", local = true)
  protected implicit val tableUtils: TableTestUtils = TableTestUtils(spark)

  protected val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  protected val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  protected val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  protected val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  protected val namespace = "test_namespace_jointest"
  tableUtils.createDatabase(namespace)

  protected def getViewsGroupBy(suffix: String, makeCumulative: Boolean = false) = {
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$suffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 100, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      isCumulative = makeCumulative
    )

    val dfToWrite = if (makeCumulative) {
      // Move all events into latest partition and set isCumulative on thrift object
      df.drop("ds").withColumn("ds", lit(today))
    } else { df }

    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    dfToWrite.save(viewsTable, Map("tblProp1" -> "1"))

    Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )
  }

  protected def getEventsEventsTemporal(nameSuffix: String = "") = {
    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) // .union(itemQueriesDf)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val suffix = if (nameSuffix.isEmpty) "" else s"_$nameSuffix"
    Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = getViewsGroupBy(nameSuffix), prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features${suffix}", namespace = namespace, team = "item_team")
    )
  }
}
