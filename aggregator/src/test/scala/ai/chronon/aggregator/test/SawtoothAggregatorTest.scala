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
import ai.chronon.aggregator.windowing.SawtoothAggregator.WindowComputerKind
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api._
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

class Timer {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  var ts: Long = System.currentTimeMillis()

  def start(): Unit = { ts = System.currentTimeMillis() }

  // TODO: Write this out into a file checked into git
  // or incorporate proper benchmarks
  def publish(name: String, reset: Boolean = true): Unit = {
    logger.info(s"${name.padTo(25, ' ')} ${System.currentTimeMillis() - ts} ms")
    if (reset) ts = System.currentTimeMillis()
  }
}

class SawtoothAggregatorTest extends AnyFlatSpec {

  private val NumericTolerance = 0.00001

  private def assertApproximatelyEqual(expected: Any, actual: Any, path: String = "value"): Unit = {
    (expected, actual) match {
      case (null, null) =>
      case (null, _) | (_, null) =>
        fail(s"$path expected <$expected> but was <$actual>")
      case (expectedArray: Array[_], actualArray: Array[_]) =>
        assertEquals(s"$path length", expectedArray.length, actualArray.length)
        expectedArray.indices.foreach { i =>
          assertApproximatelyEqual(expectedArray(i), actualArray(i), s"$path[$i]")
        }
      case (expectedMap: java.util.Map[_, _], actualMap: java.util.Map[_, _]) =>
        assertMapsApproximatelyEqual(
          expectedMap.asScala.toMap.asInstanceOf[Map[Any, Any]],
          actualMap.asScala.toMap.asInstanceOf[Map[Any, Any]],
          path
        )
      case (expectedMap: scala.collection.Map[_, _], actualMap: scala.collection.Map[_, _]) =>
        assertMapsApproximatelyEqual(
          expectedMap.asInstanceOf[scala.collection.Map[Any, Any]],
          actualMap.asInstanceOf[scala.collection.Map[Any, Any]],
          path
        )
      case (expectedList: java.util.List[_], actualList: java.util.List[_]) =>
        assertSequencesApproximatelyEqual(expectedList.asScala.toIndexedSeq, actualList.asScala.toIndexedSeq, path)
      case (expectedSeq: Seq[_], actualSeq: Seq[_]) =>
        assertSequencesApproximatelyEqual(expectedSeq, actualSeq, path)
      case _ =>
        (toApproximateDouble(expected), toApproximateDouble(actual)) match {
          case (Some(expectedDouble), Some(actualDouble)) =>
            assertDoublesApproximatelyEqual(expectedDouble, actualDouble, path)
          case _ =>
            assertEquals(path, expected, actual)
        }
    }
  }

  private def assertMapsApproximatelyEqual(expected: scala.collection.Map[Any, Any],
                                           actual: scala.collection.Map[Any, Any],
                                           path: String): Unit = {
    assertEquals(s"$path keys", expected.keySet.toSet, actual.keySet.toSet)
    expected.keys.foreach { key =>
      assertApproximatelyEqual(expected(key), actual(key), s"$path.$key")
    }
  }

  private def assertSequencesApproximatelyEqual(expected: Seq[_], actual: Seq[_], path: String): Unit = {
    assertEquals(s"$path length", expected.length, actual.length)
    expected.indices.foreach { i =>
      assertApproximatelyEqual(expected(i), actual(i), s"$path[$i]")
    }
  }

  private def toApproximateDouble(value: Any): Option[Double] =
    value match {
      case double: java.lang.Double     => Some(double.doubleValue())
      case float: java.lang.Float       => Some(float.doubleValue())
      case decimal: java.math.BigDecimal => Some(decimal.doubleValue())
      case decimal: BigDecimal          => Some(decimal.doubleValue)
      case _                            => None
    }

  private def assertDoublesApproximatelyEqual(expected: Double, actual: Double, path: String): Unit = {
    if (expected.isNaN || actual.isNaN) {
      assertTrue(s"$path expected <$expected> but was <$actual>", expected.isNaN && actual.isNaN)
    } else if (expected.isInfinity || actual.isInfinity) {
      assertEquals(path, expected, actual, 0.0)
    } else {
      assertTrue(
        s"$path expected <$expected> but was <$actual>",
        math.abs(expected - actual) <= NumericTolerance
      )
    }
  }

  it should "tail accuracy" in {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), 10000, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))))
    timer.publish("setup")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val hopsAggregator = new HopsAggregator(
      queries.min,
      aggregations,
      schema,
      FiveMinuteResolution
    )
    var hops1 = hopsAggregator.init()
    var hops2 = hopsAggregator.init()
    var hopsAll = hopsAggregator.init()

    for (i <- 0 until events.length / 2) {
      hops1 = hopsAggregator.update(hops1, events(i))
      hopsAll = hopsAggregator.update(hopsAll, events(i))
    }

    for (i <- events.length / 2 until events.length) {
      hops2 = hopsAggregator.update(hops2, events(i))
      hopsAll = hopsAggregator.update(hopsAll, events(i))
    }
    timer.publish("hops/Update")

    val hopsMerged = hopsAggregator.merge(hops1, hops2)
    val mergedHops = hopsAggregator.toTimeSortedArray(hopsMerged)
    val rawHops = hopsAggregator.toTimeSortedArray(hopsAll)
    timer.publish("hops/Sort")

    val gson = new Gson
    val mergedStr = gson.toJson(mergedHops)
    val rawStr = gson.toJson(rawHops)
    assertEquals(mergedStr, rawStr)

    timer.start()
    val sawtoothIrs = sawtoothAggregator.computeWindows(rawHops, queries)
    timer.publish("sawtooth/ComputeWindows")

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      sawtoothAggregator.windowedAggregator,
      windows,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naive/Aggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    for (i <- queries.indices) {
      assertApproximatelyEqual(naiveIrs(i), sawtoothIrs(i), s"query $i")
    }
  }

  it should "real time accuracy" in {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), 1000)
    val columns = Seq(Column("ts", LongType, 180),
                      Column("num", LongType, 1000),
                      Column("age", LongType, 100),
                      Column("bucket1", StringType, 3),
                      Column("bucket2", StringType, 2))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE,
                           "num",
                           Seq(
                             new Window(1, TimeUnit.DAYS),
                             new Window(1, TimeUnit.HOURS),
                             new Window(30, TimeUnit.DAYS)
                           ),
                           buckets = Seq("bucket1", "bucket2")),
      Builders.Aggregation(Operation.AVERAGE, "age", buckets = Seq("bucket1")),
      Builders.Aggregation(Operation.AVERAGE,
                           "age",
                           Seq(
                             new Window(1, TimeUnit.DAYS),
                             new Window(1, TimeUnit.HOURS),
                             new Window(30, TimeUnit.DAYS)
                           )),
      Builders.Aggregation(Operation.SUM, "age")
    )
    timer.publish("setup")
    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    timer.publish("sawtoothAggregate")

    val unpacked = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = unpacked.map(FiveMinuteResolution.calculateTailHop)
    val rowAgg = new RowAggregator(schema, aggregations.flatMap(_.unpack))
    val naiveAggregator = new NaiveAggregator(
      rowAgg,
      unpacked,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naiveAggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    for (i <- queries.indices) {
      assertApproximatelyEqual(rowAgg.finalize(naiveIrs(i)), rowAgg.finalize(sawtoothIrs(i)), s"query $i")
    }
    timer.publish("comparison")
  }

  it should "exclude events with same timestamp as query by default" in {
    val baseTimestamp = 1700000000000L

    val columns = Seq(Column("ts", LongType, 180), Column("value", LongType, 100))
    val schema = columns.map(_.schema)

    val events = Array(
      new TestRow(baseTimestamp + 500, 10L)(0),
      new TestRow(baseTimestamp + 2000, 100L)(0),
      new TestRow(baseTimestamp + 2000, 200L)(0),
      new TestRow(baseTimestamp + 2500, 50L)(0),
      new TestRow(baseTimestamp + 4000, 300L)(0)
    )

    val queries = Array(
      baseTimestamp + 1000,
      baseTimestamp + 2000,
      baseTimestamp + 2000,
      baseTimestamp + 3000
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "value", Seq(new Window(1, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(1, TimeUnit.DAYS)))
    )

    val result = sawtoothAggregate(events, queries, aggregations, schema)
    val rowAgg = new RowAggregator(schema, aggregations.flatMap(_.unpack))

    assertEquals(queries.length, result.length)

    val result0 = rowAgg.finalize(result(0))
    assertEquals(1L, result0(0))
    assertEquals(10L, result0(1))

    val result1 = rowAgg.finalize(result(1))
    assertEquals(1L, result1(0))
    assertEquals(10L, result1(1))

    val result2 = rowAgg.finalize(result(2))
    assertEquals(1L, result2(0))
    assertEquals(10L, result2(1))

    val result3 = rowAgg.finalize(result(3))
    assertEquals(4L, result3(0))
    assertEquals(360L, result3(1))
  }

  it should "exclude events with same timestamp as query in sorted cumulation" in {
    val baseTimestamp = 1700000000000L
    val columns = Seq(Column("ts", LongType, 180), Column("value", LongType, 100))
    val schema = columns.map(_.schema)
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "value", Seq(new Window(1, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(1, TimeUnit.DAYS)))
    )

    val aggregator = new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val events = mutable.Buffer[Row](
      new TestRow(baseTimestamp + 500, 10L)(0),
      new TestRow(baseTimestamp + 2000, 100L)(0),
      new TestRow(baseTimestamp + 2000, 200L)(0)
    )
    val queries = mutable.Buffer[Row](
      new TestRow(baseTimestamp + 2000, 0L)(0),
      new TestRow(baseTimestamp + 3000, 0L)(0)
    )
    val results = mutable.ArrayBuffer.empty[Array[Any]]
    aggregator.cumulateAndFinalizeSorted(events, queries, null, (_, finalized) => results += finalized)

    assertEquals(1L, results(0)(0))
    assertEquals(10L, results(0)(1))
    assertEquals(3L, results(1)(0))
    assertEquals(310L, results(1)(1))

  }

  it should "match window computer implementations across hop tiers" in {
    val dayMillis = 24L * 60 * 60 * 1000
    val baseTs = TsUtils.datetimeToTs("2026-01-01 00:00:00")
    val eventCount = 5000
    val queryCount = 2000
    val events = Array.tabulate(eventCount) { i =>
      val ts = baseTs + ((45L * dayMillis * i) / eventCount)
      new TestRow(ts, (i % 1000).toLong)(0)
    }
    val queries = Array.tabulate(queryCount) { i =>
      baseTs + 20L * dayMillis + ((20L * dayMillis * i) / queryCount)
    }
    val columns = Seq(Column("ts", LongType, 45), Column("num", LongType, 1000))
    val schema = columns.map(_.schema)
    val windows = Seq(new Window(1, TimeUnit.HOURS),
                      new Window(1, TimeUnit.DAYS),
                      new Window(7, TimeUnit.DAYS),
                      new Window(30, TimeUnit.DAYS))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", windows),
      Builders.Aggregation(Operation.COUNT, "num", windows),
      Builders.Aggregation(Operation.AVERAGE, "num", windows),
      Builders.Aggregation(Operation.VARIANCE, "num", windows),
      Builders.Aggregation(Operation.LAST_K, "num", windows, argMap = Map("k" -> "20"))
    )

    val hopsAggregator = new HopsAggregator(queries.min, aggregations, schema, FiveMinuteResolution)
    val sawtoothAggregator = new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    var hopMaps = hopsAggregator.init()
    events.foreach { event => hopMaps = hopsAggregator.update(hopMaps, event) }
    val hops = hopsAggregator.toTimeSortedArray(hopMaps)

    val expectedIrs = sawtoothAggregator.computeWindows(hops, queries, WindowComputerKind.Cached)
    val defaultIrs = sawtoothAggregator.computeWindows(hops, queries)
    val strategyIrs = WindowComputerKind.all.map { kind =>
      kind -> sawtoothAggregator.computeWindows(hops, queries, kind)
    }

    assertEquals(expectedIrs.length, defaultIrs.length)
    strategyIrs.foreach { case (kind, irs) =>
      assertEquals(s"${kind.name} length", expectedIrs.length, irs.length)
    }

    for (i <- queries.indices) {
      val expected = sawtoothAggregator.windowedAggregator.finalize(expectedIrs(i))
      val defaultActual = sawtoothAggregator.windowedAggregator.finalize(defaultIrs(i))
      assertApproximatelyEqual(expected, defaultActual, s"query $i default")
      strategyIrs.foreach { case (kind, irs) =>
        val actual = sawtoothAggregator.windowedAggregator.finalize(irs(i))
        assertApproximatelyEqual(expected, actual, s"query $i ${kind.name}")
      }
    }

    val unsortedQueries = Array(queries(20), queries(2), queries(200))
    val unsortedExpectedIrs = sawtoothAggregator.computeWindows(hops, unsortedQueries, WindowComputerKind.Cached)
    val unsortedDefaultIrs = sawtoothAggregator.computeWindows(hops, unsortedQueries)
    val unsortedSlidingIrs = sawtoothAggregator.computeWindows(hops, unsortedQueries, WindowComputerKind.Sliding)
    for (i <- unsortedQueries.indices) {
      val expected = sawtoothAggregator.windowedAggregator.finalize(unsortedExpectedIrs(i))
      val defaultActual = sawtoothAggregator.windowedAggregator.finalize(unsortedDefaultIrs(i))
      assertApproximatelyEqual(expected, defaultActual, s"unsorted query $i default")
      val slidingActual = sawtoothAggregator.windowedAggregator.finalize(unsortedSlidingIrs(i))
      assertApproximatelyEqual(expected, slidingActual, s"unsorted query $i sliding fallback")
    }
    assertThrows[IllegalArgumentException] {
      sawtoothAggregator.windowComputer(WindowComputerKind.Sliding).computeWindowsIterator(hops, unsortedQueries).toArray
    }
  }

  it should "benchmark window computer implementations" in {
    val dayMillis = 24L * 60 * 60 * 1000
    val hourMillis = 60L * 60 * 1000
    val baseTs = TsUtils.datetimeToTs("2026-01-01 00:00:00")
    val columns = Seq(Column("ts", LongType, 900), Column("num", LongType, 1000))
    val schema = columns.map(_.schema)
    val windows = Seq(new Window(1, TimeUnit.HOURS),
                      new Window(1, TimeUnit.DAYS),
                      new Window(7, TimeUnit.DAYS),
                      new Window(30, TimeUnit.DAYS))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", windows),
      Builders.Aggregation(Operation.COUNT, "num", windows),
      Builders.Aggregation(Operation.LAST_K, "num", windows, argMap = Map("k" -> "20"))
    )

    def timed[A](name: String, runs: Int = 3, warmups: Int = 1)(f: => A): A = {
      (0 until warmups).foreach(_ => f)
      val start = System.nanoTime()
      var result: A = null.asInstanceOf[A]
      (0 until runs).foreach(_ => result = f)
      val elapsedMillis = (System.nanoTime() - start).toDouble / 1000000
      println(f"$name: ${elapsedMillis / runs}%.2f ms avg over $runs runs")
      result
    }

    def runScenario(label: String, queryCount: Int, queryStepMillis: Long): Unit = {
      val eventCount = 200000
      val queryStartOffset = 40L * dayMillis
      val querySpan = queryCount.toLong * queryStepMillis
      val eventSpan = queryStartOffset + querySpan
      val events = Array.tabulate(eventCount) { i =>
        val ts = baseTs + ((eventSpan * i) / eventCount)
        new TestRow(ts, (i % 1000).toLong)(0)
      }
      val queries = Array.tabulate(queryCount) { i =>
        baseTs + queryStartOffset + i.toLong * queryStepMillis
      }

      val hopsAggregator = new HopsAggregator(queries.min, aggregations, schema, FiveMinuteResolution)
      val sawtoothAggregator = new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
      var hopMaps = hopsAggregator.init()
      events.foreach { event => hopMaps = hopsAggregator.update(hopMaps, event) }
      val hops = hopsAggregator.toTimeSortedArray(hopMaps)

      val results = WindowComputerKind.all.map { kind =>
        val computer = sawtoothAggregator.windowComputer(kind)
        val irs = timed(s"sawtooth/$label/${kind.name}") {
          computer.computeWindowsIterator(hops, queries).toArray
        }
        println(s"sawtooth/$label/${kind.name}/maxRetainedQueueEntries: ${computer.maxRetainedQueueEntries}")
        kind -> irs
      }

      val resultsByKind = results.toMap
      val expectedIrs = resultsByKind(WindowComputerKind.Cached)
      results.foreach { case (kind, irs) =>
        assertEquals(s"$label ${kind.name} length", expectedIrs.length, irs.length)
      }
      for (i <- queries.indices) {
        val expected = sawtoothAggregator.windowedAggregator.finalize(expectedIrs(i))
        results.foreach { case (kind, irs) =>
          val actual = sawtoothAggregator.windowedAggregator.finalize(irs(i))
          assertApproximatelyEqual(expected, actual, s"$label query $i ${kind.name}")
        }
      }
    }

    runScenario("hourly", queryCount = 20000, queryStepMillis = hourMillis)
    runScenario("daily", queryCount = 2000, queryStepMillis = dayMillis)
  }

}

object SawtoothAggregatorTest {
  // the result is irs in sorted order of queries
  // with head real-time accuracy and tail hop accuracy
  // NOTE: This provides a sketch for a distributed topology
  def sawtoothAggregate(
      events: Array[TestRow],
      queries: Array[Long],
      specs: Seq[Aggregation],
      schema: Seq[(String, DataType)],
      resolution: Resolution = FiveMinuteResolution
  ): Array[Array[Any]] = {

    // STEP-1. build hops
    val hopsAggregator =
      new HopsAggregator(queries.min, specs, schema, resolution)
    val sawtoothAggregator =
      new SawtoothAggregator(specs, schema, resolution)
    var hopMaps = hopsAggregator.init()
    for (i <- events.indices)
      hopMaps = hopsAggregator.update(hopMaps, events(i))
    val hops = hopsAggregator.toTimeSortedArray(hopMaps)

    val minResolution = resolution.hopSizes.min
    // STEP-2. group events and queries by headStart - 5minute round down of ts
    val groupedQueries: Map[Long, Array[Long]] =
      queries.groupBy(TsUtils.round(_, minResolution))
    val groupedEvents: Map[Long, Array[TestRow]] = events.groupBy { row =>
      TsUtils.round(row.ts, minResolution)
    }

    // STEP-3. compute windows based on headStart
    // aggregates will have at-most 5 minute staleness
    val headStartTimes = groupedQueries.keys.toArray
    util.Arrays.sort(headStartTimes)
    // compute windows up-to 5min accuracy for the queries
    val nonRealtimeIrs = sawtoothAggregator.computeWindows(hops, headStartTimes)

    val result = mutable.ArrayBuffer.empty[Array[Any]]
    // STEP-4. join tailAccurate - Irs with headTimeStamps and headEvents
    // to achieve realtime accuracy
    for (i <- headStartTimes.indices) {
      val headStart = headStartTimes(i)
      val tailIr = nonRealtimeIrs(i)

      // join events and queries on tailEndTimes
      val endTimes: Array[Long] = groupedQueries.getOrElse(headStart, null)
      val headEvents = groupedEvents.getOrElse(headStart, null)
      if (endTimes != null && headEvents != null) {
        util.Arrays.sort(endTimes)
      }

      result ++= sawtoothAggregator.cumulate(
        Option(headEvents).map(_.iterator).orNull,
        endTimes,
        tailIr
      )
    }
    result.toArray
  }
}
