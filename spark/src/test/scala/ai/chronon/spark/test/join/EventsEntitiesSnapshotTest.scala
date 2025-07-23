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
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Accuracy, Join, Window, TimeUnit, _}
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.DataFrameGen
import org.junit.Assert._

class EventsEntitiesSnapshotTest extends BaseJoinTest {

  it should "test events entities snapshot" in {
    val dollarTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      Column("user", StringType, 10),
      Column("user_name", api.StringType, 10),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    spark.sql(s"DROP TABLE IF EXISTS $rupeeTable")
    DataFrameGen.entities(spark, dollarTransactions, 3000, partitions = 200).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 5000, partitions = 80).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    // println("Rupee Source start partition $month")
    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts",
                        "amount_dollars" -> "CAST(amount_rupees/70 as long)",
                        "user_name" -> "user_name",
                        "user" -> "user"),
          startPartition = monthAgo,
          setups = Seq(
            "create temporary function temp_replace_right_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user", "user_name"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )
    val queriesSchema = List(
      Column("user_name", api.StringType, 10),
      Column("user", api.StringType, 10)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 3000, partitions = 90, partitionColumn = Some("date"))
      .save(queryTable, partitionColumns = Seq("date"))

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          startPartition = start,
          setups = Seq(
            "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          ),
          partitionColumn = "date"
        ),
        table = queryTable
      ),
      joinParts =
        Seq(Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user", "user" -> "user_name"))),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )
    val runner1 = new ai.chronon.spark.Join(joinConf = joinConf,
                                            endPartition =
                                              tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)),
                                            tableUtils = tableUtils)
    runner1.computeJoin()
    val dropStart = tableUtils.partitionSpec.minus(today, new Window(55, TimeUnit.DAYS))
    val dropEnd = tableUtils.partitionSpec.minus(today, new Window(45, TimeUnit.DAYS))
    tableUtils.dropPartitionRange(
      s"$namespace.test_user_transaction_features",
      dropStart,
      dropEnd
    )
    println(tableUtils.partitions(s"$namespace.test_user_transaction_features"))

    joinConf.joinParts.toScala
      .map(jp => joinConf.partOutputTable(jp))
      .foreach(tableUtils.dropPartitionRange(_, dropStart, dropEnd))

    def resetUDFs(): Unit = {
      Seq("temp_replace_left", "temp_replace_right_a", "temp_replace_right_b", "temp_replace_right_c")
        .foreach(function => tableUtils.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $function"))
    }

    resetUDFs()
    val runner2 = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = end, tableUtils = tableUtils)
    val computed = runner2.computeJoin(Some(3))
    println(s"join start = $start")

    val expectedQuery = s"""
                           |WITH
                           |   queries AS (
                           |     SELECT user_name,
                           |         user,
                           |         ts,
                           |         date as ds
                           |     from $queryTable
                           |     where user_name IS NOT null
                           |         AND user IS NOT NULL
                           |         AND ts IS NOT NULL
                           |         AND date IS NOT NULL
                           |         AND date >= '$start'
                           |         AND date <= '$end'),
                           |   grouped_transactions AS (
                           |      SELECT user,
                           |             user_name,
                           |             ds,
                           |             SUM(IF(transactions.ts  >= (unix_timestamp(transactions.ds, 'yyyy-MM-dd') - (86400*(30-1))) * 1000, amount_dollars, null)) AS unit_test_user_transactions_amount_dollars_sum_30d,
                           |             SUM(amount_dollars) AS amount_dollars_sum
                           |      FROM
                           |         (SELECT user, user_name, ts, ds, CAST(amount_rupees/70 as long) as amount_dollars from $rupeeTable
                           |          WHERE ds >= '$monthAgo'
                           |          UNION
                           |          SELECT user, user_name, ts, ds, amount_dollars from $dollarTable
                           |          WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore') as transactions
                           |      WHERE unix_timestamp(ds, 'yyyy-MM-dd')*1000 + 86400*1000> ts
                           |        AND user IS NOT NULL
                           |        AND user_name IS NOT NULL
                           |        AND ds IS NOT NULL
                           |      GROUP BY user, user_name, ds)
                           | SELECT queries.user_name,
                           |        queries.user,
                           |        queries.ts,
                           |        queries.ds,
                           |        grouped_transactions.unit_test_user_transactions_amount_dollars_sum_30d
                           | FROM queries left outer join grouped_transactions
                           | ON queries.user_name = grouped_transactions.user
                           | AND queries.user = grouped_transactions.user_name
                           | AND from_unixtime(queries.ts/1000, 'yyyy-MM-dd') = date_add(grouped_transactions.ds, 1)
                           | WHERE queries.user_name IS NOT NULL AND queries.user IS NOT NULL
                           |""".stripMargin

    val expected = spark.sql(expectedQuery)
    val queries = tableUtils.sql(
      s"SELECT user_name, user, ts, date as ds from $queryTable where user IS NOT NULL AND user_name IS NOT null AND ts IS NOT NULL AND date IS NOT NULL AND date >= '$start' AND date <= '$end'")
    val diff = Comparison.sideBySide(computed, expected, List("user_name", "user", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"Queries count: ${queries.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // test join part table hole detection:
    // in events<>entities<>snapshot case, join part table partitions and left partitions are offset by 1
    //
    // drop only end-1 from join output table, and only end-2 from join part table. trying to trick the job into
    // thinking that end-1 is already computed for the join part, since end-1 already exists in join part table
    val endMinus1 = tableUtils.partitionSpec.minus(end, new Window(1, TimeUnit.DAYS))
    val endMinus2 = tableUtils.partitionSpec.minus(end, new Window(2, TimeUnit.DAYS))

    tableUtils.dropPartitionRange(s"$namespace.test_user_transaction_features", endMinus1, endMinus1)
    println(tableUtils.partitions(s"$namespace.test_user_transaction_features"))

    joinConf.joinParts.toScala
      .map(jp => joinConf.partOutputTable(jp))
      .foreach(tableUtils.dropPartitionRange(_, endMinus2, endMinus2))

    resetUDFs()
    val runner3 = new ai.chronon.spark.Join(joinConf = joinConf, endPartition = end, tableUtils = tableUtils)

    val expected2 = spark.sql(expectedQuery)
    val computed2 = runner3.computeJoin(Some(3))
    val diff2 = Comparison.sideBySide(computed2, expected2, List("user_name", "user", "ts", "ds"))

    if (diff2.count() > 0) {
      println(s"Actual count: ${computed2.count()}")
      println(s"Expected count: ${expected2.count()}")
      println(s"Diff count: ${diff2.count()}")
      println(s"Queries count: ${queries.count()}")
      println("diff result rows")
      diff2.show()
    }
    assertEquals(0, diff2.count())
  }
}
