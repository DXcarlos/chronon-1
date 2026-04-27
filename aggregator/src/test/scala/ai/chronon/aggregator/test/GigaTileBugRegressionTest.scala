package ai.chronon.aggregator.test

import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

/** Failing tests demonstrating bugs in GigaTile (PR #1645) that Mick already fixed in MegaTile (PRs #1680/#1776).
  *
  * Each test reproduces a specific bug at the processor / aggregator level. When a fix is ported
  * from MegaTile, the corresponding test should turn green.
  */
class GigaTileBugRegressionTest extends AnyFlatSpec {
  val gson = new Gson
  val DayMillis: Long = new Window(1, TimeUnit.DAYS).millis
  val HourMillis: Long = 3600 * 1000L
  val MinuteMillis: Long = 60 * 1000L
  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)

  // -----------------------------------------------------------------
  // Bug A — Late retained event re-inflates the cached 1h window.
  //
  // Mirrors MegaTile fix `cedf6f4 — Fix MegaTile small-window as-of ingestion`.
  // GigaTile's onEvent updates cachedSmallWindowIr for any column whose tier-aligned
  // tile is within retention, regardless of whether the event lies inside the column's
  // own as-of horizon. A late event valid for the 1d window therefore also bumps 1h.
  //
  // Both events share the same as-of (the current serving horizon). The retained late
  // event must update the base tile (so a future eviction can rebuild correctly) but
  // must NOT bump the live cached IR for columns whose horizon excludes it.
  // -----------------------------------------------------------------
  it should "FAIL: late retained event must not re-inflate 1h cached small-window IR" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(1, TimeUnit.HOURS)))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val baseDay = TsUtils.round(1700000000000L, DayMillis)
    val tNow = baseDay + 12 * HourMillis
    val asOfTs = tNow + 5 * MinuteMillis // current serving horizon, just past tNow
    val tLate = tNow - 2 * HourMillis    // outside 1h horizon, inside 2d retention

    processor.advanceWatermark(tNow)
    val r1 = processor.onEvent(new TestRow(tNow, 1L)(0), tNow, asOfTs)
    assertNotNull("first event should emit", r1.finalizedVector)
    assertEquals("baseline: 1h count after first event must be 1", 1L, r1.finalizedVector(0))

    val r2 = processor.onEvent(new TestRow(tLate, 1L)(0), tLate, asOfTs)
    assertEquals(
      "1h count must remain 1 — the late event is outside the 1h horizon at the current as-of",
      1L,
      Option(r2.finalizedVector).map(_(0)).getOrElse(r1.finalizedVector(0))
    )
  }

  // -----------------------------------------------------------------
  // Bug B — cachedSmallWindowIr is carried across day rollover unchanged.
  //
  // Mirrors MegaTile fix `eedd34a — Fix MegaTile small-window cache on day rollover`.
  // advanceWatermark rotates large-IR slots but does not rebuild cachedSmallWindowIr,
  // so events older than the new as-of horizon survive into the next day's serves
  // until the next eviction timer fires.
  // -----------------------------------------------------------------
  it should "FAIL: cachedSmallWindowIr must not include yesterday's events after rollover" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.COUNT, "num", Seq(new Window(1, TimeUnit.HOURS)))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val baseDay = TsUtils.round(1700000000000L, DayMillis)
    val day0_22h = baseDay + 22 * HourMillis
    Seq(day0_22h, day0_22h + 5 * MinuteMillis, day0_22h + 10 * MinuteMillis).foreach { ts =>
      processor.advanceWatermark(ts)
      val r = processor.onEvent(new TestRow(ts, 1L)(0), ts)
      assertNotNull(s"event at $ts should emit", r.finalizedVector)
    }

    // Roll over — wall clock has moved to day+1, three hours past midnight.
    val day1_3h = baseDay + DayMillis + 3 * HourMillis
    processor.advanceWatermark(day1_3h)

    // Single fresh event 55 minutes later. Only this event lies inside the 1h horizon now.
    val day1_3h_55m = day1_3h + 55 * MinuteMillis
    processor.advanceWatermark(day1_3h_55m)
    val r4 = processor.onEvent(new TestRow(day1_3h_55m, 1L)(0), day1_3h_55m)
    assertNotNull("post-rollover event should emit", r4.finalizedVector)
    assertEquals(
      "1h count after rollover must reflect only the new event — yesterday's events fell out of the window",
      1L,
      r4.finalizedVector(0)
    )
  }

  // -----------------------------------------------------------------
  // Bug C — bulkMerge mutates the cached batch tail-hop IR.
  //
  // Mirrors MegaTile fix `c311a0a — Port MegaTile PT semantics and tail-hop fix`.
  // mergeTailHopsForBatchColumns passes a tail-hop slot directly into bulkMerge as the
  // (initially null) accumulator's first non-null entry. bulkMerge mutates that entry
  // for non-trivial aggregators (e.g. AVERAGE → (sum,count) Array[Any]). Repeated serves
  // therefore re-merge against a corrupted tail-hop and drift.
  //
  // This test goes through MegaTileAggregator.serveMegaTile — the same path GigaTile's
  // GigaTileStreamProcessor.recomputeRunningLargeIr takes via mergeTailHopsForBatchColumns.
  // -----------------------------------------------------------------
  it should "FAIL: serveMegaTile must not mutate batch tail hops across repeated calls" in {
    val avgSchema: Seq[(String, DataType)] = Seq("ts" -> LongType, "amount" -> DoubleType)
    val batchEnd = TsUtils.round(1700000000000L, DayMillis)
    val queryTs = batchEnd + HourMillis
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE, "amount", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, avgSchema, tailBufferMillis = TailBufferMillis)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, avgSchema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 26 * HourMillis, 2.0)(0))
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 25 * HourMillis, 4.0)(0))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val emptyMegaTileIr = megaTileAgg.buildMegaTileIr(
      Map.empty[Long, collection.Map[Long, Array[Any]]],
      now = queryTs,
      batchEnd = batchEnd
    )
    val first = megaTileAgg.serveMegaTile(finalBatchIr, emptyMegaTileIr, queryTs, batchEnd)
    val second = megaTileAgg.serveMegaTile(finalBatchIr, emptyMegaTileIr, queryTs, batchEnd)

    assertEquals(
      "first serve should report the true 3-day average over (2.0, 4.0)",
      3.0,
      first(0).asInstanceOf[Double],
      1e-6
    )
    assertEquals(
      "second serve must equal first — tail hops in batchIr must be untouched between calls",
      first(0).asInstanceOf[Double],
      second(0).asInstanceOf[Double],
      1e-6
    )
  }

  // -----------------------------------------------------------------
  // Bug D — Delayed batch (>1d stale) loses streaming events from intermediate days.
  //
  // Analogous to MegaTile fix `ced0185` (the multi-day fetch part), but the architectural
  // shape is different: GigaTile keeps only `largeTodayIr` and `largeYesterdayIr` per
  // entity. When the batch upload is more than 1 day stale and the watermark advances
  // through sequential day rollovers, each rollover overwrites yesterday with today and
  // the previous yesterday is silently dropped. recomputeRunningLargeIr (run on every
  // eviction and batch update) then merges only batch + today + yesterday and the
  // intermediate streaming day disappears from the finalized vector.
  //
  // Setup: 7d SUM, batch covers up to day0 with no events. Stream 5 events × 1 each on
  // days 0, 1, 2. After eviction on day 2, the running large IR should contain all 15
  // events; with the bug it contains 10 (day 0 lost between rollovers).
  // -----------------------------------------------------------------
  it should "FAIL: delayed batch (>1d) must not lose streaming events from intermediate days" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", Seq(window))
    )
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val day1 = day0 + DayMillis
    val day2 = day1 + DayMillis

    // Empty batch covering up to day0 — keeps the assertion clean (all 15 events come from streaming).
    val onlineAgg = new SawtoothOnlineAggregator(day0, aggregations, schema, tailBufferMillis = TailBufferMillis)
    val emptyFinalized = onlineAgg.finalizeSnapshot(onlineAgg.init)
    val batchIr = onlineAgg.denormalizeBatchIr(emptyFinalized)
    processor.onBatchUpdate(batchIr, day0, day0)

    def streamDay(dayStart: Long): Unit = {
      (1 to 5).foreach { i =>
        val ts = dayStart + i * HourMillis
        processor.advanceWatermark(ts)
        processor.onEvent(new TestRow(ts, 1L)(0), ts)
      }
    }

    // Sequential adjacent rollovers — this is what surfaces the bug. A single multi-day
    // jump would zero yesterday out via the `wmDay == currentDayStart + DayMillis` branch,
    // so the only way to lose day0 is to walk through day1 in between.
    streamDay(day0)
    processor.advanceWatermark(day1)
    streamDay(day1)
    processor.advanceWatermark(day2)
    streamDay(day2)

    val queryTs = day2 + 12 * HourMillis
    processor.advanceWatermark(queryTs)
    val r = processor.onEviction(queryTs)
    assertNotNull("eviction at day2 should emit", r.finalizedVector)
    val sum = r.finalizedVector(0).asInstanceOf[Long]
    assertEquals(
      "7d sum after delayed batch must include all 15 streaming events — batch is 2d stale, day0 must not be lost",
      15L,
      sum
    )
  }
}
