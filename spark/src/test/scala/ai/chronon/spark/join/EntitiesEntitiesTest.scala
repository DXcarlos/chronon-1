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
import ai.chronon.api.{Builders, Operation, TimeUnit, Window}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.DataFrameGen
import org.junit.Assert._

class EntitiesEntitiesTest extends BaseJoinTest {

  it should "test entities entities" in {
    // untimed/unwindowed entities on right
    // right side
    val weightSchema = List(
      Column("user", api.StringType, 10),
      Column("country", api.StringType, 10),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataFrameGen
      .entities(spark, weightSchema, 100, partitions = 400, partitionFormat = Some("yyyyMMdd"))
      .save(weightTable)

    val weightSource = Builders.Source.entities(
      query = Builders
        .Query(selects = Builders.Selects("weight"), startPartition = yearAgo, endPartition = dayAndMonthBefore)
        .setPartitionFormat("yyyyMMdd"),
      snapshotTable = weightTable
    )

    val weightGroupBy = Builders.GroupBy(
      sources = Seq(weightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "weight")),
      metaData = Builders.MetaData(name = "unit_test.country_weights", namespace = namespace)
    )

    val heightSchema = List(
      Column("user", api.StringType, 10),
      Column("country", api.StringType, 10),
      Column("height", api.LongType, 200)
    )
    val heightTable = s"$namespace.heights"
    DataFrameGen.entities(spark, heightSchema, 100, partitions = 400).save(heightTable)
    val heightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("height"), startPartition = monthAgo),
      snapshotTable = heightTable
    )

    val heightGroupBy = Builders.GroupBy(
      sources = Seq(heightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "height")),
      metaData = Builders.MetaData(name = "unit_test.country_heights", namespace = namespace)
    )

    // left side
    val countrySchema = List(Column("country", api.StringType, 100))
    val countryTable = s"$namespace.countries"
    DataFrameGen.entities(spark, countrySchema, 100, partitions = 400).save(countryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = end, tableUtils = tableUtils)
    val computed = runner.computeJoin(Some(7))

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
                                     |   grouped_weights AS (
                                     |      SELECT country,
                                     |             ds,
                                     |             avg(weight) as unit_test_country_weights_weight_average
                                     |      FROM $weightTable
                                     |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore'
                                     |      GROUP BY country, ds),
                                     |   grouped_heights AS (
                                     |      SELECT country,
                                     |             ds,
                                     |             avg(height) as unit_test_country_heights_height_average
                                     |      FROM $heightTable
                                     |      WHERE ds >= '$monthAgo'
                                     |      GROUP BY country, ds)
                                     |   SELECT countries.country,
                                     |        countries.ds,
                                     |        grouped_weights.unit_test_country_weights_weight_average,
                                     |        grouped_heights.unit_test_country_heights_height_average
                                     | FROM countries left outer join grouped_weights
                                     | ON countries.country = grouped_weights.country
                                     | AND countries.ds = grouped_weights.ds
                                     | left outer join grouped_heights
                                     | ON countries.ds = grouped_heights.ds
                                     | AND countries.country = grouped_heights.country
                                     |""".stripMargin)

    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    println(
      s"Left side count: ${spark.sql(s"SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'").count()}")
    println(s"Actual count: ${computed.count()}")
    println(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("country", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
    /* the below testing case to cover the scenario when input table and output table
     * have same partitions, in other words, the frontfill is done, the join job
     * should not trigger a backfill and exit the program properly
     * TODO: Revisit this in a logger world.
    // use console to redirect println message to Java IO
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      // rerun the same join job
      runner.computeJoin(Some(7))
    }
    val stdOutMsg = stream.toString()
    println(s"std out message =\n $stdOutMsg")
    // make sure that the program exits with target print statements
    assertTrue(stdOutMsg.contains(s"There is no data to compute based on end partition of $end."))
     */
  }
}
