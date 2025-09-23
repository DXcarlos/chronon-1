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

import ai.chronon.api.{Accuracy, Builders, Operation, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import org.junit.Assert._

class MigrationTest extends BaseJoinTest {

  it should "test migration" in {
    // Left
    val itemQueriesTable = s"$namespace.item_queries"
    val ds = "2023-01-01"
    val leftStart = tableUtils.partitionSpec.minus(ds, new Window(100, TimeUnit.DAYS))
    val leftSource = Builders.Source.events(Builders.Query(startPartition = leftStart), table = itemQueriesTable)

    // Right
    val viewsTable = s"$namespace.view_events"
    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"),
                             startPartition = tableUtils.partitionSpec.minus(ds, new Window(200, TimeUnit.DAYS)))
    )

    val groupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.TEMPORAL
    )

    // Join
    val join = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.join_migration", namespace = namespace, team = "chronon")
    )

    // test older versions before migration
    // older versions do not have the bootstrap hash, but should not trigger recompute if no bootstrap_parts
    val productionHashV1 = Map(
      "left_source" -> "0DVP4fhmG8",
      "test_namespace_jointest.test_join_migration_user_unit_test_item_views" -> "J/Lqxs8k4t"
    )
    assertEquals(0, join.tablesToDrop(productionHashV1).length)

    // test newer versions
    val productionHashV2 = productionHashV1 ++ Map(
      "test_namespace_jointest.test_join_migration_bootstrap" -> "1B2M2Y8Asg"
    )
    assertEquals(0, join.tablesToDrop(productionHashV2).length)
  }
}
