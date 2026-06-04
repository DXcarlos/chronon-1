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

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps, WindowUtils}
import ai.chronon.api._
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._

class ResolutionTest extends AnyFlatSpec {

  private object LegacyFiveMinuteResolution extends Resolution {
    def calculateTailHop(window: Window): Long =
      window.millis match {
        case x if x >= new Window(12, TimeUnit.DAYS).millis  => WindowUtils.Day.millis
        case x if x >= new Window(12, TimeUnit.HOURS).millis => WindowUtils.Hour.millis
        case _                                               => WindowUtils.FiveMinutes
      }

    val hopSizes: Array[Long] =
      Array(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes)
  }

  private val schema = Seq("ts" -> LongType, "value" -> LongType)
  private val baseTs = 1704067200000L
  private val gson = new Gson

  private def groupBy(aggregations: Seq[Aggregation]): GroupBy = {
    val groupBy = new GroupBy()
    groupBy.setAggregations(aggregations.asJava)
    groupBy
  }

  it should "preserve legacy resolution for hour and day windows" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM,
                           "value",
                           Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS)))
    )

    val resolution = ResolutionUtils.effectiveResolution(aggregations, FiveMinuteResolution)

    assertSame(FiveMinuteResolution, resolution)
    assertEquals(LegacyFiveMinuteResolution.hopSizes.toSeq, resolution.hopSizes.toSeq)
    assertEquals(WindowUtils.FiveMinutes, resolution.calculateTailHop(new Window(1, TimeUnit.HOURS)))
    assertEquals(WindowUtils.Hour.millis, resolution.calculateTailHop(new Window(1, TimeUnit.DAYS)))
    assertEquals(WindowUtils.FiveMinutes, ResolutionUtils.getSmallestTailHopMillis(groupBy(aggregations)))
  }

  it should "ignore non-positive and unbounded windows when selecting minute resolution" in {
    val zeroWindow = new Window(0, TimeUnit.MINUTES)
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(zeroWindow, WindowUtils.Unbounded))
    )

    val resolution = ResolutionUtils.effectiveResolution(aggregations, FiveMinuteResolution)

    assertSame(FiveMinuteResolution, resolution)
    assertEquals(WindowUtils.FiveMinutes, resolution.calculateTailHop(zeroWindow))
    assertEquals(WindowUtils.Day.millis, resolution.calculateTailHop(WindowUtils.Unbounded))
  }

  it should "use minute hops only for windows below one hour" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM,
                           "value",
                           Seq(new Window(30, TimeUnit.MINUTES), new Window(1, TimeUnit.HOURS)))
    )

    val resolution = ResolutionUtils.effectiveResolution(aggregations, FiveMinuteResolution)

    assertNotSame(FiveMinuteResolution, resolution)
    assertEquals(Seq(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes, WindowUtils.Minute),
                 resolution.hopSizes.toSeq)
    assertEquals(WindowUtils.Minute, resolution.calculateTailHop(new Window(30, TimeUnit.MINUTES)))
    assertEquals(WindowUtils.Minute, resolution.calculateTailHop(new Window(59, TimeUnit.MINUTES)))
    assertEquals(WindowUtils.FiveMinutes, resolution.calculateTailHop(new Window(60, TimeUnit.MINUTES)))
    assertEquals(WindowUtils.FiveMinutes, resolution.calculateTailHop(new Window(1, TimeUnit.HOURS)))
    assertEquals(WindowUtils.Hour.millis, resolution.calculateTailHop(new Window(12, TimeUnit.HOURS)))
    assertEquals(WindowUtils.Day.millis, resolution.calculateTailHop(new Window(12, TimeUnit.DAYS)))
    assertEquals(WindowUtils.Minute, ResolutionUtils.getSmallestTailHopMillis(groupBy(aggregations)))
  }

  it should "preserve existing offline values for non-minute windows" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT,
                           "value",
                           Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM,
                           "value",
                           Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS)))
    )
    val events = Array(
      new TestRow(baseTs + 5 * WindowUtils.Minute, 10L)(0),
      new TestRow(baseTs + 35 * WindowUtils.Minute, 20L)(0),
      new TestRow(baseTs + 90 * WindowUtils.Minute, 30L)(0),
      new TestRow(baseTs + 3 * WindowUtils.Hour.millis, 40L)(0)
    )
    val queries = Array(baseTs + 2 * WindowUtils.Hour.millis, baseTs + 4 * WindowUtils.Hour.millis)

    val current = sawtoothAggregate(events, queries, aggregations, schema, FiveMinuteResolution)
    val legacy = sawtoothAggregate(events, queries, aggregations, schema, LegacyFiveMinuteResolution)

    assertEquals(gson.toJson(legacy), gson.toJson(current))
  }

  it should "serve existing online batch data unchanged for non-minute windows" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT,
                           "value",
                           Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM,
                           "value",
                           Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS)))
    )
    val batchEndTs = baseTs + WindowUtils.Day.millis
    val events = Array(
      new TestRow(baseTs + 5 * WindowUtils.Minute, 10L)(0),
      new TestRow(baseTs + 35 * WindowUtils.Minute, 20L)(0),
      new TestRow(baseTs + 23 * WindowUtils.Hour.millis, 30L)(0),
      new TestRow(batchEndTs + 10 * WindowUtils.Minute, 40L)(0),
      new TestRow(batchEndTs + 70 * WindowUtils.Minute, 50L)(0)
    )
    val legacyUpload = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, LegacyFiveMinuteResolution)
    val oldFinalBatchIr = legacyUpload.normalizeBatchIr(
      events.filter(_.ts < batchEndTs).foldLeft(legacyUpload.init)(legacyUpload.update)
    )
    val oldDenormalizedBatchIr = legacyUpload.denormalizeBatchIr(oldFinalBatchIr)
    val currentServing = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, FiveMinuteResolution)
    val legacyServing = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, LegacyFiveMinuteResolution)
    val queries = Array(batchEndTs + WindowUtils.Hour.millis, batchEndTs + 2 * WindowUtils.Hour.millis)
    val streamingRows = events.filter(_.ts >= batchEndTs)

    assertEquals(LegacyFiveMinuteResolution.hopSizes.length, oldFinalBatchIr.tailHops.length)
    assertEquals(LegacyFiveMinuteResolution.hopSizes.length, currentServing.init.tailHops.length)

    queries.foreach { queryTs =>
      val current = currentServing.windowedAggregator.finalize(
        currentServing.lambdaAggregateIr(oldDenormalizedBatchIr, streamingRows.iterator, queryTs))
      val legacy = legacyServing.windowedAggregator.finalize(
        legacyServing.lambdaAggregateIr(oldDenormalizedBatchIr, streamingRows.iterator, queryTs))
      assertEquals(gson.toJson(legacy), gson.toJson(current))
    }
  }

  it should "serve existing online batch data for minute windows from the legacy hop layout" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT,
                           "value",
                           Seq(new Window(30, TimeUnit.MINUTES), new Window(1, TimeUnit.HOURS))),
      Builders.Aggregation(Operation.SUM,
                           "value",
                           Seq(new Window(30, TimeUnit.MINUTES), new Window(1, TimeUnit.HOURS)))
    )
    val batchEndTs = baseTs + WindowUtils.Day.millis
    val events = Array(
      new TestRow(batchEndTs - 27 * WindowUtils.Minute, 10L)(0),
      new TestRow(batchEndTs - 11 * WindowUtils.Minute, 20L)(0),
      new TestRow(batchEndTs - 2 * WindowUtils.Minute, 30L)(0)
    )
    val legacyUpload = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, LegacyFiveMinuteResolution)
    val oldFinalBatchIr = legacyUpload.normalizeBatchIr(events.foldLeft(legacyUpload.init)(legacyUpload.update))
    val oldDenormalizedBatchIr = legacyUpload.denormalizeBatchIr(oldFinalBatchIr)
    val currentServing = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, FiveMinuteResolution)
    val legacyServing = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, LegacyFiveMinuteResolution)
    val queryTs = batchEndTs + 3 * WindowUtils.Minute

    assertEquals(LegacyFiveMinuteResolution.hopSizes.length, oldFinalBatchIr.tailHops.length)
    assertEquals(4, currentServing.init.tailHops.length)

    val current = currentServing.windowedAggregator.finalize(
      currentServing.lambdaAggregateIr(oldDenormalizedBatchIr, Iterator.empty, queryTs))
    val legacy = legacyServing.windowedAggregator.finalize(
      legacyServing.lambdaAggregateIr(oldDenormalizedBatchIr, Iterator.empty, queryTs))

    assertEquals(gson.toJson(legacy), gson.toJson(current))
  }

  it should "aggregate minute windows with minute hop accuracy" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT,
                           "value",
                           Seq(new Window(30, TimeUnit.MINUTES), new Window(1, TimeUnit.HOURS))),
      Builders.Aggregation(Operation.SUM,
                           "value",
                           Seq(new Window(30, TimeUnit.MINUTES), new Window(1, TimeUnit.HOURS)))
    )
    val events = Array(
      new TestRow(baseTs + 1 * WindowUtils.Minute, 10L)(0),
      new TestRow(baseTs + 3 * WindowUtils.Minute, 20L)(0),
      new TestRow(baseTs + 32 * WindowUtils.Minute, 30L)(0),
      new TestRow(baseTs + 58 * WindowUtils.Minute, 40L)(0)
    )
    val queries = Array(baseTs + 35 * WindowUtils.Minute, baseTs + 59 * WindowUtils.Minute)
    val resolution = ResolutionUtils.effectiveResolution(aggregations, FiveMinuteResolution)
    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(resolution.calculateTailHop)
    val rowAggregator = new RowAggregator(schema, aggregations.flatMap(_.unpack))
    val sawtooth = sawtoothAggregate(events, queries, aggregations, schema, FiveMinuteResolution)
    val naive = new NaiveAggregator(rowAggregator, windows, tailHops).aggregate(events, queries)

    assertEquals(4, new HopsAggregator(queries.min, aggregations, schema, FiveMinuteResolution).hopSizes.length)
    assertEquals(4, new SawtoothOnlineAggregator(queries.min, aggregations, schema, FiveMinuteResolution).init.tailHops.length)

    queries.indices.foreach { idx =>
      assertEquals(gson.toJson(rowAggregator.finalize(naive(idx))),
                   gson.toJson(rowAggregator.finalize(sawtooth(idx))))
    }
  }
}
