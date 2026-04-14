package ai.chronon.flink.test.window

import ai.chronon.api._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.flink.FlinkJob
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.flink.window.MegaTileProcessFunction
import ai.chronon.online.MegaTileCodec
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util
import scala.collection.JavaConverters._

class MegaTileProcessFunctionTest extends AnyFlatSpec with Matchers {
  import MegaTileProcessFunctionTest._

  "MegaTileProcessFunction" should "keep watermark constants aligned with MegaTile eviction lifecycle" in {
    FlinkJob.AllowedOutOfOrderness.toMillis shouldEqual 5 * 60 * 1000L
    FlinkJob.IdlenessTimeout.toMillis shouldEqual 30 * 1000L
    FlinkJob.CatchupWatermarkLagSlackMillis shouldEqual 30 * 1000L
  }

  it should "use processing-time eviction when watermark lag is inside one hop plus slack" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      seedOldTileBeforeLiveCatchupBoundary(driver, "gen_live_slack")

      driver.processWatermark("2025-07-21T11:29:45Z")
      driver.setProcessingTime("2025-07-21T11:34:00Z")
      driver.processEvent("gen_live_slack", "2025-07-21T11:34:00Z", "user_current")
      driver.drainNewOutputs().last.values shouldEqual windowValues(3L, 3L, 3L)

      driver.setProcessingTime("2025-07-21T11:35:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_live_slack",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 3L, 3L))
    }
  }

  it should "use watermark-time eviction when active catchup is beyond slack" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      seedOldTileBeforeLiveCatchupBoundary(driver, "gen_catchup")

      driver.processWatermark("2025-07-21T11:29:29Z")
      driver.setProcessingTime("2025-07-21T11:34:00Z")
      driver.processEvent("gen_catchup", "2025-07-21T11:34:00Z", "user_current")
      driver.drainNewOutputs().last.values shouldEqual windowValues(3L, 3L, 3L)

      driver.setProcessingTime("2025-07-21T11:35:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_catchup",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(2L, 3L, 3L))
    }
  }

  it should "roll day state at watermark midnight without losing the previous dirty row" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-21T23:59:00Z")
      driver.processEvent("gen_steady", "2025-07-21T23:59:00Z", "user_before_roll")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_steady",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      driver.setProcessingTime("2025-07-22T00:00:01Z")
      driver.drainNewOutputs() should not be empty

      driver.processWatermark("2025-07-22T00:00:00Z")
      driver.processEvent("gen_steady", "2025-07-22T00:00:00Z", "user_after_roll")
      val rolloverOutputs = driver.drainNewOutputs()
      assertSingleOutput(
        rolloverOutputs,
        expectedKey = "gen_steady",
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = windowValues(2L, 2L, 1L))

      driver.setProcessingTime("2025-07-22T00:05:01Z")
      driver.drainNewOutputs() should not be empty

      driver.processEvent("gen_steady", "2025-07-22T00:05:00Z", "user_exact_hop")
      val todayOutputs = driver.drainNewOutputs()
      todayOutputs.last.dayStartMillis shouldEqual dayStart("2025-07-22T00:00:00Z")
      todayOutputs.last.values shouldEqual windowValues(3L, 3L, 2L)
    }
  }

  it should "route late previous-day events to current small windows and yesterday large windows after PT midnight roll" in {
    withDriver(bufferingOutputTimeMillis = 1000L) { driver =>
      driver.processWatermark("2025-07-21T23:54:55.999Z")
      driver.setProcessingTime("2025-07-21T23:59:56.900Z")
      driver.processEvent("gen_midnight_live", "2025-07-21T23:59:56Z", "user_before_midnight")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2025-07-21T23:59:57.900Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_midnight_live",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      driver.setProcessingTime("2025-07-22T00:00:00.001Z")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2025-07-22T00:00:00.006Z")
      driver.processEvent("gen_midnight_live", "2025-07-21T23:59:58Z", "user_previous_day_after_roll")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2025-07-22T00:00:01.001Z")
      val boundaryOutputs = driver.drainNewOutputs()
      boundaryOutputs should have size 2
      boundaryOutputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(2L, 2L, null))
      boundaryOutputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 2L))
    }
  }

  it should "update retained yesterday-start for 1d but not stale 1h" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-22T00:10:00Z")
      driver.processEvent("gen_boundary", "2025-07-22T00:00:00Z", "user_today_start")
      driver.drainNewOutputs() should have size 1

      driver.processWatermark("2025-07-22T00:05:00Z")
      driver.processEvent("gen_boundary", "2025-07-21T00:00:00Z", "user_yesterday_start")
      val outputs = driver.drainNewOutputs()
      outputs should have size 2
      outputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 2L, 1L))
      outputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 1L))
    }
  }

  it should "decay during a short upstream stop and recover on live-time resume" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_stop_short", "2025-07-21T10:30:00Z", "user_initial")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)

      driver.setProcessingTime("2025-07-21T11:30:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)

      driver.setProcessingTime("2025-07-21T11:35:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, 1L, 1L)

      driver.processEvent("gen_stop_short", "2025-07-21T11:36:00Z", "user_resume")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 2L, 2L)
    }
  }

  it should "not re-inflate 1h for a retained late tile outside current small-window as-of" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_late_same_day", "2025-07-21T10:30:00Z", "user_on_time")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)

      driver.setProcessingTime("2025-07-21T11:35:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, 1L, 1L)

      driver.setProcessingTime("2025-07-21T11:36:00Z")
      driver.processWatermark("2025-07-21T11:30:00Z")
      driver.processEvent("gen_late_same_day", "2025-07-21T10:32:00Z", "user_late_expired_hop")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, 2L, 2L)
    }
  }

  it should "expire 1d after a long stop but preserve fresh resume behavior" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_stop_long", "2025-07-21T10:30:00Z", "user_initial")
      driver.drainNewOutputs() should have size 1

      driver.setProcessingTime("2025-07-22T11:35:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, null, null)

      driver.processEvent("gen_stop_long", "2025-07-22T11:36:00Z", "user_resume")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)
    }
  }

  it should "update retained yesterday-start for 1d but not stale 1h after resume" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-22T00:10:00Z")
      driver.processEvent("gen_stop_boundary", "2025-07-22T00:10:00Z", "user_anchor")
      driver.drainNewOutputs() should have size 1

      driver.setProcessingTime("2025-07-22T00:15:00Z")
      driver.drainNewOutputs() should not be empty

      driver.processWatermark("2025-07-22T00:10:00Z")
      driver.processEvent("gen_stop_boundary", "2025-07-21T00:00:00Z", "user_yesterday_start")
      val outputs = driver.drainNewOutputs()
      outputs should have size 2
      outputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 2L, 1L))
      outputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 1L))

      driver.processEvent("gen_stop_boundary", "2025-07-20T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "keep active replay on watermark time before sparse-key PT fallback" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-23T10:30:00Z")
      driver.processWatermark("2025-07-21T10:30:00Z")
      driver.processEvent("gen_replay", "2025-07-21T10:30:00Z", "user_replay_1")
      driver.drainNewOutputs().last.dayStartMillis shouldEqual dayStart("2025-07-21T00:00:00Z")

      driver.setProcessingTime("2025-07-23T10:35:00Z")
      driver.processEvent("gen_replay", "2025-07-21T10:31:00Z", "user_replay_2")
      driver.drainNewOutputs().last.values shouldEqual windowValues(2L, 2L, 2L)

      driver.setProcessingTime("2025-07-23T10:50:00Z")
      val idleFallback = driver.drainNewOutputs().last
      idleFallback.dayStartMillis shouldEqual dayStart("2025-07-23T00:00:00Z")
      idleFallback.values shouldEqual windowValues(null, null, null)
    }
  }

  it should "retain an event ahead of stale watermark until eviction catches up" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.processWatermark("2025-07-21T22:31:23.999Z")
      driver.setProcessingTime("2025-07-21T23:40:01Z")
      driver.processEvent("gen_resume_stale_watermark", "2025-07-21T23:40:00Z", "user_resume")
      driver.drainNewOutputs().last.values shouldEqual windowValues(null, null, 1L)

      driver.processWatermark("2025-07-21T23:40:00Z")
      driver.setProcessingTime("2025-07-21T23:45:00Z")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)
    }
  }

  it should "handle catchup UTC day-boundary scenarios" in {
    val scenarios = Seq(
      activeKeyResumesAfterUtcMidnightWithStaleWatermarkThenCatchesUp _,
      sparseKeyResumesAfterUtcMidnightWithStaleWatermark _,
      yesterdayBoundaryEventAfterResumeIsAdmitted _,
      olderThanYesterdayEventAfterResumeIsDropped _
    )

    scenarios.foreach { scenario =>
      withDriver(bufferingOutputTimeMillis = 0L)(scenario)
    }
  }

  it should "not emit an old dirty day as an adjacent rollover after multi-day downtime" in {
    withDriver(bufferingOutputTimeMillis = 3L * DayMillis) { driver =>
      driver.setProcessingTime("2026-04-11T23:50:00Z")
      driver.processEvent("axis_multi_day_jump", "2026-04-11T23:50:00Z", "user_seed")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2026-04-13T00:10:00Z")
      driver.drainNewOutputs() shouldBe empty

      driver.processEvent("axis_multi_day_jump", "2026-04-11T23:59:00Z", "user_backlog_too_old_after_jump")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "update replayed yesterday-start for 1d but not stale 1h" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-24T10:00:00Z")
      driver.processWatermark("2025-07-22T00:00:00Z")
      driver.processEvent("gen_replay_boundary", "2025-07-22T00:00:00Z", "user_today_start")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_replay_boundary",
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      driver.processWatermark("2025-07-23T00:00:00Z")
      driver.processEvent("gen_replay_boundary", "2025-07-22T00:00:00Z", "user_yesterday_start")
      val outputs = driver.drainNewOutputs()
      outputs should have size 2
      outputs.find(_.dayStartMillis == dayStart("2025-07-23T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(1L, 2L, null))
      outputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 2L))

      driver.processEvent("gen_replay_boundary", "2025-07-21T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "drop sparse-key backlog older than yesterday after PT fallback day roll" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-23T10:30:00Z")
      driver.processWatermark("2025-07-21T10:30:00Z")
      driver.processEvent("gen_sparse_replay_drop", "2025-07-21T10:30:00Z", "user_seed")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_sparse_replay_drop",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      driver.setProcessingTime("2025-07-23T10:40:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_sparse_replay_drop",
        expectedDayStart = dayStart("2025-07-23T00:00:00Z"),
        expectedValues = windowValues(null, null, null))

      driver.setProcessingTime("2025-07-23T10:41:00Z")
      driver.processEvent("gen_sparse_replay_drop", "2025-07-21T23:59:00Z", "user_backlog_too_late")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "roll cold-start backlog rows without dropping valid events" in {
    withDriver(bufferingOutputTimeMillis = 120000L) { driver =>
      driver.setProcessingTime("2025-07-23T10:30:00Z")
      driver.processWatermark("2025-07-21T23:59:00Z")
      driver.processEvent("gen_cold_start", "2025-07-21T23:59:00Z", "user_day_1")
      driver.drainNewOutputs() shouldBe empty

      driver.processWatermark("2025-07-22T00:00:01Z")
      driver.processEvent("gen_cold_start", "2025-07-22T00:00:00Z", "user_day_2")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_cold_start",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))

      driver.processEvent("gen_cold_start", "2025-07-22T00:05:00Z", "user_exact_hop")
      driver.processEvent("gen_cold_start", "2025-07-22T00:05:01Z", "user_after_hop")
      driver.processEvent("gen_cold_start", "2025-07-21T00:00:00Z", "user_prev_day")
      driver.processEvent("gen_cold_start", "2025-07-20T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2025-07-23T10:32:00Z")
      val preEvictionOutputs = driver.drainNewOutputs()
      preEvictionOutputs should have size 2
      preEvictionOutputs.find(_.dayStartMillis == dayStart("2025-07-21T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(null, null, 2L))
      preEvictionOutputs.find(_.dayStartMillis == dayStart("2025-07-22T00:00:00Z")).map(_.values) shouldEqual
        Some(windowValues(2L, 5L, 3L))

      driver.setProcessingTime("2025-07-23T10:35:00Z")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2025-07-23T10:37:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_cold_start",
        expectedDayStart = dayStart("2025-07-22T00:00:00Z"),
        expectedValues = windowValues(2L, 5L, 3L))
    }
  }

  it should "drop events older than yesterday before mutating state" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.processWatermark("2026-04-12T00:05:00Z")
      driver.setProcessingTime("2026-04-12T00:10:00Z")
      driver.processEvent("gen_stale", "2026-04-12T00:10:00Z", "user_anchor")
      driver.drainNewOutputs() should have size 1

      driver.processEvent("gen_stale", "2026-04-10T23:59:59.999Z", "user_too_old")
      driver.drainNewOutputs() shouldBe empty
    }
  }

  it should "restore pending timers, dirty bits, and key isolation from checkpoint" in {
    val originalHarness = harness(bufferingOutputTimeMillis = 10 * 60 * 1000L)
    originalHarness.open()
    val baseProcessingTs = toMillis("2025-07-21T10:30:00Z")
    originalHarness.setProcessingTime(baseProcessingTs)
    originalHarness.processElement(event("gen_restore_a", "2025-07-21T10:30:00Z", "user_a_1", baseProcessingTs), 0L)
    originalHarness.processElement(event("gen_restore_b", "2025-07-21T10:31:00Z", "user_b_1", baseProcessingTs), 0L)
    originalHarness.extractOutputValues().asScala.toList shouldBe empty

    val snapshot = originalHarness.snapshot(7L, baseProcessingTs + 1000L)
    originalHarness.close()

    val restoredHarness = harness(bufferingOutputTimeMillis = 10 * 60 * 1000L)
    restoredHarness.setup()
    restoredHarness.initializeState(snapshot)
    restoredHarness.open()

    restoredHarness.setProcessingTime(toMillis("2025-07-21T10:35:00Z"))
    restoredHarness.extractOutputValues().asScala.toList shouldBe empty

    restoredHarness.setProcessingTime(toMillis("2025-07-21T10:40:00Z"))
    val outputs = restoredHarness.extractOutputValues().asScala.toList.map(decodeOutput)
    outputs should have size 2
    outputs.find(_.keys == List("gen_restore_a")).map(_.values) shouldEqual Some(windowValues(1L, 1L, 1L))
    outputs.find(_.keys == List("gen_restore_b")).map(_.values) shouldEqual Some(windowValues(1L, 1L, 1L))

    restoredHarness.processElement(
      event("gen_restore_a", "2025-07-21T10:41:00Z", "user_a_2", toMillis("2025-07-21T10:40:00Z")),
      0L)
    restoredHarness.setProcessingTime(toMillis("2025-07-21T10:51:00Z"))
    val postRestore = restoredHarness.extractOutputValues().asScala.toList.map(decodeOutput)
    postRestore.filter(_.keys == List("gen_restore_a")).last.values shouldEqual windowValues(2L, 2L, 2L)
    postRestore.filter(_.keys == List("gen_restore_b")).last.values shouldEqual windowValues(1L, 1L, 1L)
    restoredHarness.close()
  }

  it should "skip malformed timestamp rows without corrupting later valid rows" in {
    withDriver(bufferingOutputTimeMillis = 0L) { driver =>
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processMalformedTimestamp("gen_bad", "bad_ts", "user_bad")
      driver.drainNewOutputs() shouldBe empty

      driver.processEvent("gen_bad", "2025-07-21T10:31:00Z", "user_good")
      driver.drainNewOutputs().last.values shouldEqual windowValues(1L, 1L, 1L)
    }
  }

  it should "emit once when buffered emit and eviction timers share the same timestamp" in {
    withDriver(bufferingOutputTimeMillis = 5 * 60 * 1000L) { driver =>
      driver.setProcessingTime("2025-07-21T10:30:00Z")
      driver.processEvent("gen_collision", "2025-07-21T10:30:00Z", "user_1")
      driver.drainNewOutputs() shouldBe empty

      driver.setProcessingTime("2025-07-21T10:35:00Z")
      assertSingleOutput(
        driver.drainNewOutputs(),
        expectedKey = "gen_collision",
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))
    }
  }

  it should "add stable per-key jitter to buffered emissions" in {
    val key = "gen_jitter"
    val baseProcessingTs = toMillis("2025-07-21T10:30:00Z")
    val bufferMillis = 1000L
    val maxJitterMillis = 1000L
    val expectedJitter = Math.floorMod(keyList(key).hashCode().toLong, maxJitterMillis + 1L)
    val expectedEmitTs = baseProcessingTs + bufferMillis + expectedJitter

    withDriver(bufferingOutputTimeMillis = bufferMillis, bufferingOutputJitterMillis = maxJitterMillis) { driver =>
      driver.setProcessingTimeMillis(baseProcessingTs)
      driver.processEvent(key, "2025-07-21T10:30:00Z", "user_1")
      driver.drainNewOutputs() shouldBe empty

      if (expectedEmitTs > baseProcessingTs) {
        driver.setProcessingTimeMillis(expectedEmitTs - 1L)
        driver.drainNewOutputs() shouldBe empty
      }

      driver.setProcessingTimeMillis(expectedEmitTs)
      val outputs = driver.drainNewOutputs()
      assertSingleOutput(
        outputs,
        expectedKey = key,
        expectedDayStart = dayStart("2025-07-21T00:00:00Z"),
        expectedValues = windowValues(1L, 1L, 1L))
      outputs.head.processingTsMillis shouldEqual expectedEmitTs
    }
  }
}

object MegaTileProcessFunctionTest extends Matchers {
  private val DayMillis = new Window(1, TimeUnit.DAYS).millis

  private val inputSchema: Seq[(String, DataType)] =
    Seq("view_by" -> StringType, Constants.TimeColumn -> LongType)

  private val groupBy: GroupBy = {
    val gb = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.test_stream",
          topic = "events.test_stream",
          query = Builders.Query(
            selects = Map("id" -> "id", "view_by" -> "view_by"),
            timeColumn = Constants.TimeColumn,
            startPartition = "20250101"
          )
        )
      ),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "view_by",
          windows = Seq(new Window(1, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS), new Window(3, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "mega_tile_process_function_test"),
      accuracy = Accuracy.TEMPORAL
    )
    gb.setOnlineStrategy(OnlineStrategy.STREAMING_MEGATILES)
    gb
  }

  private val megaTileCodec = new MegaTileCodec(groupBy, inputSchema)

  private def windowValues(oneHour: Any, oneDay: Any, threeDay: Any): Seq[Any] =
    Seq(oneHour, oneDay, threeDay)

  private def seedOldTileBeforeLiveCatchupBoundary(driver: Driver, key: String): Unit = {
    driver.setProcessingTime("2025-07-21T10:30:00Z")
    driver.processEvent(key, "2025-07-21T10:30:00Z", "user_old_1")
    driver.processEvent(key, "2025-07-21T10:31:00Z", "user_old_2")
    driver.drainNewOutputs().last.values shouldEqual windowValues(2L, 2L, 2L)

    driver.setProcessingTime("2025-07-21T11:30:00Z")
    driver.drainNewOutputs().last.values shouldEqual windowValues(2L, 2L, 2L)
  }

  private def activeKeyResumesAfterUtcMidnightWithStaleWatermarkThenCatchesUp(driver: Driver): Unit = {
    val key = "axis_active_after_midnight"
    driver.processWatermark("2026-04-11T23:54:00Z")
    driver.setProcessingTime("2026-04-12T00:01:00Z")

    driver.processEvent(key, "2026-04-12T00:00:30Z", "user_after_midnight")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(null, null, 1L))

    driver.processWatermark("2026-04-12T00:00:30Z")
    driver.setProcessingTime("2026-04-12T00:05:00Z")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))
  }

  private def sparseKeyResumesAfterUtcMidnightWithStaleWatermark(driver: Driver): Unit = {
    val key = "axis_sparse_after_midnight"
    driver.processWatermark("2026-04-11T23:49:00Z")
    driver.setProcessingTime("2026-04-11T23:50:00Z")
    driver.processEvent(key, "2026-04-11T23:50:00Z", "user_seed")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-11T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))

    driver.processWatermark("2026-04-11T23:54:00Z")
    driver.setProcessingTime("2026-04-12T00:10:00Z")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, null))
  }

  private def yesterdayBoundaryEventAfterResumeIsAdmitted(driver: Driver): Unit = {
    val key = "axis_yesterday_boundary"
    driver.processWatermark("2026-04-12T00:05:00Z")
    driver.setProcessingTime("2026-04-12T00:10:00Z")
    driver.processEvent(key, "2026-04-12T00:10:00Z", "user_anchor")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))

    driver.processEvent(key, "2026-04-11T00:00:00Z", "user_yesterday_start")
    val outputs = driver.drainNewOutputs()
    outputs should have size 2
    outputs.find(_.dayStartMillis == dayStart("2026-04-12T00:00:00Z")).map(_.values) shouldEqual
      Some(windowValues(1L, 2L, 1L))
    outputs.find(_.dayStartMillis == dayStart("2026-04-11T00:00:00Z")).map(_.values) shouldEqual
      Some(windowValues(null, null, 1L))
  }

  private def olderThanYesterdayEventAfterResumeIsDropped(driver: Driver): Unit = {
    val key = "axis_older_than_yesterday"
    driver.processWatermark("2026-04-12T00:05:00Z")
    driver.setProcessingTime("2026-04-12T00:10:00Z")
    driver.processEvent(key, "2026-04-12T00:10:00Z", "user_anchor")
    assertSingleOutput(
      driver.drainNewOutputs(),
      expectedKey = key,
      expectedDayStart = dayStart("2026-04-12T00:00:00Z"),
      expectedValues = windowValues(1L, 1L, 1L))

    driver.processEvent(key, "2026-04-10T23:59:59.999Z", "user_too_old")
    driver.drainNewOutputs() shouldBe empty
  }

  private case class DecodedOutput(
      keys: List[Any],
      dayStartMillis: Long,
      processingTsMillis: Long,
      values: Seq[Any])

  final private class Driver(bufferingOutputTimeMillis: Long, bufferingOutputJitterMillis: Long) {
    private val testHarness = harness(bufferingOutputTimeMillis, bufferingOutputJitterMillis)
    private var emittedCount = 0
    private var currentProcessingTs = 0L

    testHarness.open()

    def setProcessingTime(iso: String): Unit =
      setProcessingTimeMillis(toMillis(iso))

    def setProcessingTimeMillis(ts: Long): Unit = {
      currentProcessingTs = ts
      testHarness.setProcessingTime(ts)
    }

    def processWatermark(iso: String): Unit =
      testHarness.processWatermark(toMillis(iso))

    def processEvent(id: String, eventIso: String, viewBy: String): Unit =
      testHarness.processElement(event(id, eventIso, viewBy, currentProcessingTs), 0L)

    def processMalformedTimestamp(id: String, malformedTs: String, viewBy: String): Unit =
      testHarness.processElement(
        ProjectedEvent(Map("id" -> id, "view_by" -> viewBy, Constants.TimeColumn -> malformedTs), currentProcessingTs),
        0L)

    def drainNewOutputs(): List[DecodedOutput] = {
      val allOutputs = testHarness.extractOutputValues().asScala.toList.map(decodeOutput)
      val newOutputs = allOutputs.drop(emittedCount)
      emittedCount = allOutputs.size
      newOutputs
    }

    def close(): Unit =
      testHarness.close()
  }

  private def withDriver(bufferingOutputTimeMillis: Long, bufferingOutputJitterMillis: Long = 0L)(
      fn: Driver => Unit): Unit = {
    val driver = new Driver(bufferingOutputTimeMillis, bufferingOutputJitterMillis)
    try {
      fn(driver)
    } finally {
      driver.close()
    }
  }

  private def harness(bufferingOutputTimeMillis: Long,
                      bufferingOutputJitterMillis: Long = 0L)
      : KeyedOneInputStreamOperatorTestHarness[java.util.List[Any], ProjectedEvent, TimestampedTile] = {
    new KeyedOneInputStreamOperatorTestHarness[java.util.List[Any], ProjectedEvent, TimestampedTile](
      new KeyedProcessOperator[java.util.List[Any], ProjectedEvent, TimestampedTile](
        new MegaTileProcessFunction(
          groupBy,
          inputSchema,
          bufferingOutputTimeMillis = bufferingOutputTimeMillis,
          bufferingOutputJitterMillis = bufferingOutputJitterMillis
        )),
      new KeySelector[ProjectedEvent, java.util.List[Any]] {
        override def getKey(value: ProjectedEvent): java.util.List[Any] =
          keyList(value.fields("id"))
      },
      TypeInformation.of(classOf[java.util.List[_]]).asInstanceOf[TypeInformation[java.util.List[Any]]]
    )
  }

  private def event(id: String, eventIso: String, viewBy: String, processingTs: Long): ProjectedEvent =
    ProjectedEvent(
      Map("id" -> id, "view_by" -> viewBy, Constants.TimeColumn -> toMillis(eventIso)),
      processingTs
    )

  private def keyList(key: Any): java.util.List[Any] = {
    val result = new util.ArrayList[Any](1)
    result.add(key)
    result
  }

  private def assertSingleOutput(
      outputs: List[DecodedOutput],
      expectedKey: String,
      expectedDayStart: Long,
      expectedValues: Seq[Any]): Unit = {
    outputs should have size 1
    outputs.head.keys shouldEqual List(expectedKey)
    outputs.head.dayStartMillis shouldEqual expectedDayStart
    outputs.head.values shouldEqual expectedValues
  }

  private def decodeOutput(tile: TimestampedTile): DecodedOutput =
    DecodedOutput(
      keys = tile.keys.asScala.toList,
      dayStartMillis = tile.latestTsMillis,
      processingTsMillis = tile.startProcessingTime,
      values = megaTileCodec.rowAggregator
        .finalize(megaTileCodec.decode(tile.tileBytes))
        .toSeq
    )

  private def dayStart(iso: String): Long =
    TsUtils.round(toMillis(iso), DayMillis)

  private def toMillis(iso: String): Long =
    Instant.parse(iso).toEpochMilli
}
