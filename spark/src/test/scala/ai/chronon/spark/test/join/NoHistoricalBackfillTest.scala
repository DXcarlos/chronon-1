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

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, Operation, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.DataFrameGen
import org.junit.Assert._

class NoHistoricalBackfillTest extends BaseJoinTest {

  it should "test entities entities no historical backfill" in {
    val rowCount = 10000
    // Only backfill latest partition if historical_backfill is turned off
    val weightSchema = List(
      Column("user", api.StringType, 10),
      Column("country", api.StringType, 10),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights_no_historical_backfill"
    DataFrameGen.entities(spark, weightSchema, rowCount, partitions = 400).save(weightTable)

    val weightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("weight"), startPartition = yearAgo, endPartition = today),
      snapshotTable = weightTable
    )

    val weightGroupBy = Builders.GroupBy(
      sources = Seq(weightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "weight")),
      metaData = Builders.MetaData(name = "test.country_weights_no_backfill", namespace = namespace)
    )

    // left side
    val countrySchema = List(Column("country", api.StringType, 10))
    val countryTable = s"$namespace.countries_no_historical_backfill"
    DataFrameGen.entities(spark, countrySchema, rowCount, partitions = 30).save(countryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(5, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_no_historical_backfill",
                                   namespace = namespace,
                                   team = "chronon",
                                   historicalBackfill = false)
    )

    val runner = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = end, tableUtils = tableUtils)
    val computed = runner.computeJoin(Some(7))
    println("showing join result")
    computed.show()

    val leftSideCount = spark.sql(s"SELECT country, ds from $countryTable where ds == '$end'").count()
    println(s"Left side expected count: $leftSideCount")
    println(s"Actual count: ${computed.count()}")
    assertEquals(leftSideCount, computed.count())
    // There should be only one partition in computed df which equals to end partition
    val allPartitions = computed.select("ds").rdd.map(row => row(0)).collect().toSet
    assert(allPartitions.size == 1)
    assertEquals(allPartitions.toList(0), end)
  }
}
