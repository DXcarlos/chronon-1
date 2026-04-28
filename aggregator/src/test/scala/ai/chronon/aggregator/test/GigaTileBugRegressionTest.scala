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

  // -----------------------------------------------------------------
  // Gap E — Bootstrap underreport: emits before batch IR is loaded report a 7d window as
  // if it equalled the streaming-since-startup partial sum. The fetcher's PUSH branch
  // already returns null for "no streaming data", but as soon as Flink emits one vector
  // (with batchIr still null), that under-counted vector overwrites the KV row and is
  // served indefinitely.
  //
  // Correct behavior: don't emit a finalized vector for any column whose contract requires
  // batch (large/unwindowed). Either suppress the emit entirely, or null out the batch-
  // dependent columns until onBatchUpdate has run.
  // -----------------------------------------------------------------
  it should "FAIL: must not emit batch-dependent columns before batch IR has loaded" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    // Deliberately skip onBatchUpdate — Iceberg connected stream hasn't fired yet.
    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val event = new TestRow(day0 + HourMillis, 5L)(0)
    processor.advanceWatermark(event.ts)
    val r = processor.onEvent(event, event.ts)

    assertNotNull("event must produce some emit", r)
    val sumIr = r.finalizedVector
    val sumValue = if (sumIr == null) null else sumIr(0)
    assertNull(
      "7d SUM must not be emitted as the streaming-only partial sum while batchIr is unloaded — " +
        "fetcher will serve this under-counted value indefinitely from the PUSH KV row",
      sumValue
    )
  }

  // -----------------------------------------------------------------
  // Gap F — Silent drop of events older than the staleness bound. With
  // maxBatchStalenessDays=2 and an event 5 days old, the event is dropped from BOTH the
  // small-window path (out of retention) and the daily-slot path (older than oldestAcceptedDay).
  // Nothing surfaces — no exception, no metric, no return signal. A consumer downstream
  // cannot tell that data was silently lost.
  //
  // Correct behavior at minimum: signal the drop on the GigaEmitResult so the Flink wiring
  // can bump a counter / log. The simplest API surface is a `droppedStaleEvent: Boolean`
  // field on GigaEmitResult. This test asserts that signal exists; it currently doesn't.
  // -----------------------------------------------------------------
  it should "FAIL: events older than maxBatchStalenessDays must surface a drop signal" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store, maxBatchStalenessDays = 2)

    val today = TsUtils.round(1700000000000L, DayMillis) + 12 * HourMillis
    processor.advanceWatermark(today)
    processor.onEvent(new TestRow(today, 1L)(0), today)

    val staleEvent = new TestRow(today - 5 * DayMillis, 100L)(0)
    val r = processor.onEvent(staleEvent, staleEvent.ts)

    // GigaEmitResult does not currently expose a drop indicator. Ask reflection for a field
    // named droppedStaleEvent (or similar) — the test fails until such a signal exists.
    val signalField = r.getClass.getDeclaredFields
      .find(f => f.getName.toLowerCase.contains("drop") || f.getName.toLowerCase.contains("stale"))
    assertTrue(
      "GigaEmitResult must expose a drop signal so callers can bump a metric / log a stale event — " +
        s"none of the fields ${r.getClass.getDeclaredFields.map(_.getName).mkString(",")} indicate this",
      signalField.isDefined
    )
  }

  // -----------------------------------------------------------------
  // Gap G — Idle-entity stale finalized vector. After events arrive, the small-window
  // cache reflects the (eventTs - window, eventTs] interval. As wall-clock advances with
  // no further events, the cache should age out: events whose ts falls outside the
  // [now - window, now] interval should no longer contribute. GigaTile's PUSH KV row
  // is whatever was last emitted, and `onTimer` deliberately doesn't re-register a
  // processing-time eviction timer ("Don't re-register from onTimer" comment), so the
  // KV value stays at the last-emit forever — even after the events should have aged
  // out of the small window.
  //
  // The processor itself decays correctly when onEviction is called with a later ts —
  // the bug is at the Flink wiring level (no PT timer for idle entities). Here we
  // assert the desired contract at the processor level: a serving emit issued at
  // (last_event + 2h) for a 1h SUM must show 0/null because the events are outside
  // the 1h horizon. The test simulates "idle Flink wiring" by NOT calling onEviction
  // and asks the processor for the as-of value via a no-op trigger.
  //
  // Without a "serve at time T" API on the processor (which the Flink wiring would
  // need too), we exercise the proxy: the last in-state cached IR is the value the
  // PUSH KV would serve. The test fails until either (a) the processor exposes a way
  // to compute the as-of vector without an event, or (b) the Flink wiring keeps the
  // cache fresh via PT timers.
  // -----------------------------------------------------------------
  it should "FAIL: idle entity must serve a vector that decays with wall-clock, not the last emit" in {
    val window = new Window(1, TimeUnit.HOURS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val tEvent = day0 + 6 * HourMillis
    processor.advanceWatermark(tEvent)
    val r1 = processor.onEvent(new TestRow(tEvent, 1L)(0), tEvent)
    assertEquals("baseline: 1h SUM after the event is 1", 1L, r1.finalizedVector(0))

    // Wall clock advances 2 hours past the event with no further events — the event is
    // now well outside the 1h serving horizon. A read at this point must reflect that.
    val tServe = tEvent + 2 * HourMillis
    processor.advanceWatermark(tServe)

    // The processor exposes no "serve as of T without an event" API. The PUSH KV row
    // therefore still holds r1.finalizedVector (sum=1), even though the correct
    // as-of-tServe answer is 0/null. Assert the contract we actually want.
    val servedAsOfTServe: Any = {
      val method = processor.getClass.getDeclaredMethods.find(_.getName == "serveAsOf")
      method.map { m => m.setAccessible(true); m.invoke(processor, java.lang.Long.valueOf(tServe)) }.orNull
    }
    assertNotNull(
      "GigaTileStreamProcessor must expose a serveAsOf(asOfTs) helper so Flink can " +
        "refresh idle KV rows without waiting for a new event. Without it, idle entities " +
        "serve stale finalized vectors indefinitely.",
      servedAsOfTServe
    )
  }

  // -----------------------------------------------------------------
  // Gap I — Tombstone signal when the finalized vector is fully empty.
  //
  // After a 1h-windowed entity goes idle for 2h, the rebuilt cache is all-null. The
  // resulting finalized vector encodes to a row of nulls. GigaTile writes that row to the
  // PUSH KV table with no signal that it is semantically "empty". Two consequences:
  //   1. Idle keys accumulate as zero-rows in KV indefinitely (no TTL on the values).
  //   2. The fetcher cannot distinguish "Flink emitted an empty row for this key" from
  //      "Flink never emitted for this key" — both serve nulls — so a freshly-decayed key
  //      cannot be tombstoned via DELETE.
  //
  // The contract: GigaEmitResult should expose `isEmpty` (or equivalent) when the packed
  // IR has every column null, so the Flink wiring can choose to issue a KV DELETE instead
  // of a PUT-with-nulls. Currently no such field exists.
  // -----------------------------------------------------------------
  it should "FAIL: emit must signal a tombstone when the finalized vector is fully empty" in {
    val window = new Window(1, TimeUnit.HOURS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val tEvent = day0 + 6 * HourMillis
    processor.advanceWatermark(tEvent)
    processor.onEvent(new TestRow(tEvent, 1L)(0), tEvent)

    // Eviction well past the window — every retained tile is stale, cache rebuilt to empty.
    val tDecay = tEvent + 2 * HourMillis
    processor.advanceWatermark(tDecay)
    val emit = processor.onEviction(tDecay)
    assertNotNull("eviction past window must still produce an emit", emit.finalizedVector)
    val allNull = emit.finalizedVector.forall(_ == null)
    assertTrue("baseline: every column should be null after the window decays", allNull)

    // Once that's true, a tombstone signal is what lets the writer DELETE the KV row.
    val tombstoneField = emit.getClass.getDeclaredFields
      .find(f => Set("isEmpty", "tombstone", "isTombstone", "shouldDelete").contains(f.getName))
    assertTrue(
      "GigaEmitResult must expose a tombstone signal so Flink can DELETE the PUSH KV row " +
        "for fully-decayed entities — instead of writing a row of nulls that lives forever. " +
        s"Available fields: ${emit.getClass.getDeclaredFields.map(_.getName).mkString(",")}",
      tombstoneField.isDefined
    )
  }

  // -----------------------------------------------------------------
  // Gap J — Idle entity post-batch-advance: stale row served indefinitely.
  //
  // Architectural gap, not a processor-internal bug. The Iceberg connected stream emits a
  // BatchIrRow only for entities present in the new batch. An entity X that had events
  // weeks ago and went silent has no row in the new batch, so processElement2 is never
  // called for X. processElement1 also never fires (no Kafka events). Result:
  //   - X's `batchEndTs` in Flink state stays at the OLD batch end.
  //   - X's daily-slot map keeps the old streaming events.
  //   - recomputeRunningLargeIr (when an eviction does fire) merges the OLD batch + OLD
  //     slots, reproducing the stale value.
  //
  // The fetcher then serves X's stale row — even though every event for X is now outside
  // every retained window. Worst case: a 7d window keeps reporting X's events from a year
  // ago because the entity never received a per-key signal that the world moved on.
  //
  // Correct behavior requires a *broadcast* batch-advance signal that reaches every key
  // (or some equivalent: scheduled per-key TTL, periodic null heartbeat from a side
  // input). This test asserts a `onGlobalBatchAdvance` API exists on the processor; it
  // currently does not.
  // -----------------------------------------------------------------
  it should "FAIL: idle entity must receive a global batch-advance signal so its KV row decays after weeks of silence" in {
    val window = new Window(7, TimeUnit.DAYS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store)

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val day20 = day0 + 20 * DayMillis

    // Batch covers up to day0; this entity has events on day 0 and then goes silent.
    val onlineAgg = new SawtoothOnlineAggregator(day0, aggregations, schema, tailBufferMillis = TailBufferMillis)
    val emptyBatch = onlineAgg.denormalizeBatchIr(onlineAgg.finalizeSnapshot(onlineAgg.init))
    processor.onBatchUpdate(emptyBatch, day0, day0)

    val event = new TestRow(day0 + HourMillis, 5L)(0)
    processor.advanceWatermark(event.ts)
    processor.onEvent(event, event.ts)

    // 20 days pass globally. Other entities trigger batch advances; this entity never does.
    // No per-key signal reaches the processor — exactly the production gap.
    processor.advanceWatermark(day20 + 12 * HourMillis)

    // The processor should expose a broadcast / heartbeat hook that lets Flink wiring tell
    // every key's state that the global batch boundary has moved, even without an entity-
    // specific BatchIrRow. Without it, the serving value for this idle key remains stale.
    val advanceMethod = processor.getClass.getDeclaredMethods
      .find(m =>
        Set("onGlobalBatchAdvance", "advanceBatchBoundary", "heartbeatBatchEnd").contains(m.getName))
    assertTrue(
      "GigaTileStreamProcessor must expose a global batch-advance hook (e.g. onGlobalBatchAdvance(newBatchEnd)) " +
        "so idle keys not present in the new batch can still decay their state. Currently the only path to update " +
        "batchEndTs is processElement2, which never fires for entities not in the batch — their KV rows stay " +
        s"stale indefinitely. Available methods: ${processor.getClass.getDeclaredMethods.map(_.getName).filterNot(_.startsWith("$")).distinct.mkString(",")}",
      advanceMethod.isDefined
    )
  }

  // -----------------------------------------------------------------
  // Gap K — Decay produces a stable empty-emit only when irEqual is configured.
  //
  // With the default `irEqual = (_, _) => false`, every eviction past window decay emits
  // the same all-null vector — burning O(idle_entities × eviction_interval) KV writes
  // per day. With a value-aware irEqual, the second decay emit is suppressed correctly.
  //
  // This is more efficiency than correctness, but it ALSO masks a write-amplification bug
  // that's user-visible (KV write traffic, write quota exhaustion). The processor should
  // suppress consecutive all-empty emits even under the default no-op irEqual — those
  // are guaranteed equivalent by construction.
  // -----------------------------------------------------------------
  it should "FAIL: consecutive all-empty eviction emits must be suppressed even under default irEqual" in {
    val window = new Window(1, TimeUnit.HOURS)
    val aggregations: Seq[Aggregation] = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(window)))
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryGigaTileStore(megaTileAgg.windowedAggregator)
    val processor = new GigaTileStreamProcessor(megaTileAgg, store) // default irEqual

    val day0 = TsUtils.round(1700000000000L, DayMillis)
    val tEvent = day0 + 6 * HourMillis
    processor.advanceWatermark(tEvent)
    processor.onEvent(new TestRow(tEvent, 1L)(0), tEvent)

    // Two evictions past the window — both rebuild to all-null.
    val firstDecay = processor.onEviction(tEvent + 2 * HourMillis)
    val secondDecay = processor.onEviction(tEvent + 3 * HourMillis)

    assertNotNull("first decay emit must exist (transitions from non-null to all-null)", firstDecay.finalizedVector)
    assertTrue("first decay emit should be all-null", firstDecay.finalizedVector.forall(_ == null))
    assertNull(
      "second decay emit must be suppressed — both rebuilds produce identical all-null vectors and " +
        "burning a KV write per eviction past idle adds up to traffic quota exhaustion at scale",
      secondDecay.finalizedVector
    )
  }
}
