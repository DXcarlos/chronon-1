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
import ai.chronon.api.{Builders, TimeUnit, Window}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import org.junit.Assert._

class KeyMappingOverlappingFieldsTest extends BaseJoinTest {

  it should "testKeyMappingOverlappingFields" in {
    // test the scenario when a key_mapping is a -> b, (right key b is mapped to left key a) and
    // a happens to be another field in the same group by

    val namesSchema = List(
      Column("user", api.StringType, 10),
      Column("attribute", api.StringType, 10)
    )
    val namesTable = s"$namespace.key_overlap_names"
    DataFrameGen.entities(spark, namesSchema, 100, partitions = 400).save(namesTable)

    val namesSource = Builders.Source.entities(
      query =
        Builders.Query(selects =
                         Builders.Selects.exprs("user" -> "user", "user_id" -> "user", "attribute" -> "attribute"),
                       startPartition = yearAgo,
                       endPartition = dayAndMonthBefore),
      snapshotTable = namesTable
    )

    val namesGroupBy = Builders.GroupBy(
      sources = Seq(namesSource),
      keyColumns = Seq("user"),
      aggregations = null,
      metaData = Builders.MetaData(name = "unit_test.key_overlap.user_names", team = "chronon")
    )

    // left side
    val userSchema = List(Column("user_id", api.StringType, 10))
    val usersTable = s"$namespace.key_overlap_users"
    DataFrameGen.events(spark, userSchema, 100, partitions = 400).dropDuplicates().save(usersTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(selects = Map("user_id" -> "user_id"), startPartition = start),
                                      snapshotTable = usersTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = namesGroupBy,
                          keyMapping = Map(
                            "user_id" -> "user"
                          ))),
      metaData =
        Builders.MetaData(name = "unit_test.key_overlap.user_features", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = end, tableUtils = tableUtils)
    val computed = runner.computeJoin(Some(7))
    assertFalse(computed.isEmpty)
  }
}
