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

import org.apache.spark.sql.SaveMode
import ai.chronon.api.Builders
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import org.junit.Assert._

class DynamicPartitionOverwriteTest extends BaseJoinTest {

  it should "testing basic spark dynamic partition overwrite" in {
    import spark.implicits._

    val rows = List(
      TestRow("1", "a"),
      TestRow("2", "b"),
      TestRow("3", "c"),
      TestRow("4", "d"),
      TestRow("5", "e")
    )
    val data = spark.createDataFrame(rows) toDF ("ds", "value")
    data.write.mode(SaveMode.Overwrite).format("hive").partitionBy("ds").saveAsTable(f"${namespace}.table")
    assertEquals(tableUtils.loadTable(f"${namespace}.table").as[TestRow].collect().toList.sorted, rows.sorted)

    tableUtils.loadTable(f"${namespace}.table").show(truncate = false)

    val dynamicPartitions = List(
      TestRow("4", "y"),
      TestRow("5", "z")
    )
    val dynamicPartitionsDF = spark.createDataset(dynamicPartitions).select("value", "ds")

    dynamicPartitionsDF.write
      .format("hive")
      .mode(SaveMode.Overwrite)
      .insertInto(f"${namespace}.table")

    tableUtils.loadTable(f"${namespace}.table").show(truncate = false)

    val updatedExpected =
      (rows.map((r) => r.ds -> r.value).toMap ++ dynamicPartitions.map((r) => r.ds -> r.value).toMap).map {
        case (k, v) => TestRow(k, v)
      }.toList

    assertEquals(updatedExpected.sorted,
                 tableUtils.loadTable(f"${namespace}.table").as[TestRow].collect().toList.sorted)
  }
}
