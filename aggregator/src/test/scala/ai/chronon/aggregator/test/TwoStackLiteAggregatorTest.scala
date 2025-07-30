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

package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.TopK
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.FiveMinuteResolution
import ai.chronon.aggregator.windowing.SawtoothAggregator
import ai.chronon.aggregator.windowing.TwoStackLiteAggregationBuffer
import ai.chronon.aggregator.windowing.TwoStackLiteAggregator
import ai.chronon.api.Aggregation
import ai.chronon.api.Builders
import ai.chronon.api.IntType
import ai.chronon.api.LongType
import ai.chronon.api.Operation
import ai.chronon.api.StructField
import ai.chronon.api.StructType
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.Seq

class TwoStackLiteAggregatorTest extends AnyFlatSpec {
  it should "buffer with top k" in {
    val topK = new TopK[Integer](IntType, 2)
    val bankersBuffer = new TwoStackLiteAggregationBuffer(topK, 5)
    assertEquals(null, bankersBuffer.query) // null
    Seq(7, 8, 9).map(x => Integer.valueOf(x)).foreach(i => bankersBuffer.push(i))
    def assertBufferEquals(a: Seq[Int], b: java.util.ArrayList[Integer]): Unit = {
      if (a == null || b == null) {
        assertEquals(a, b)
      } else {
        assertArrayEquals(Option(a).map(_.map(x => Integer.valueOf(x).asInstanceOf[AnyRef]).toArray).orNull,
                          Option(b).map(_.toArray).orNull)
      }
    }
    assertBufferEquals(Seq(8, 9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(Seq(8, 9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(Seq(9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(null, bankersBuffer.query)
    bankersBuffer.push(Integer.valueOf(10))
    assertBufferEquals(Seq(10), bankersBuffer.query)
  }

  it should "against sawtooth" in {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), 100000, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "num"),
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)),
        argMap = Map("k" -> "300")),
      Builders.Aggregation(Operation.TOP_K, "num", argMap = Map("k" -> "300"))
    )

    timer.publish("setup")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

//    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
//    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
//    val naiveAggregator = new NaiveAggregator(
//      sawtoothAggregator.windowedAggregator,
//      windows,
//      tailHops
//    )
//    val naiveIrs = naiveAggregator.aggregate(events, queries).map(sawtoothAggregator.windowedAggregator.finalize)
//    timer.publish("naive")
    val bankersAggregator =
      new TwoStackLiteAggregator(StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray), aggregations)

    // will finalize by default
    val bankersIrs = bankersAggregator
      .slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length)
      .toArray
    timer.publish("sorting + banker")

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
      .map(sawtoothAggregator.windowedAggregator.finalize)
    timer.publish("sawtooth")

    // rough timings below will vary by processor - but at 100k
    // naive                     256011 ms
    // sorting + banker          1597 ms
    // sawtooth                  914 ms

    val gson = new Gson()
    bankersIrs.zip(sawtoothIrs).foreach { case (bankers, sawtooth) =>
      assertEquals(gson.toJson(sawtooth), gson.toJson(bankers))
    }
  }

}
