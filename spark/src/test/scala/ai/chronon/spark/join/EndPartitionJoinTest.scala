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

import ai.chronon.api.Builders
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import org.junit.Assert._

class EndPartitionJoinTest extends BaseJoinTest {

  it should "test end partition join" in {
    val join = getEventsEventsTemporal("end_partition_test")
    val start = join.getLeft.query.startPartition
    val end = tableUtils.partitionSpec.after(start)
    val limitedJoin = Builders.Join(
      left =
        Builders.Source.events(Builders.Query(startPartition = start, endPartition = end), table = join.getLeft.table),
      joinParts = join.getJoinParts.toScala,
      metaData = join.metaData
    )
    assertTrue(end < today)
    val toCompute = new ai.chronon.spark.Join(joinConf = limitedJoin, endPartition = today, tableUtils = tableUtils)
    toCompute.computeJoin()
    val ds = tableUtils.sql(s"SELECT MAX(ds) FROM ${limitedJoin.metaData.outputTable}")
    assertTrue(ds.first().getString(0) < today)
  }
}
