package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{MegaTileAggregator, MegaTileStreamProcessor, TileStore}
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.FlinkJob
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.MegaTileCodec
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.{TimeDomain, TimerService}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/** Flink KeyedProcessFunction that maintains per-entity mega tile state.
  * Delegates all aggregation logic to MegaTileStreamProcessor.
  * State access goes through FlinkTileStore — only touched entries are serialized/deserialized.
  *
  * Timer strategy:
  * - During catchup (watermark far behind wall clock): event-time timers only.
  *   Eviction fires as watermark advances through the backlog at the correct cadence.
  * - During live operation (watermark near wall clock): processing-time timers aligned
  *   to hop boundaries. Fires at wall-clock intervals so idle entities get timely
  *   sawtooth correction without waiting for the next event to advance the watermark.
  * - Late event-time timers can coexist with processing-time timers in live mode to correct
  *   sawtooth state after out-of-order arrivals.
  */
class MegaTileProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false
) extends KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var processor: MegaTileStreamProcessor = _
  @transient private var megaTileCodec: MegaTileCodec = _
  @transient private var flinkStore: FlinkTileStore = _

  @transient private var eventProcessingErrorCounter: Counter = _
  @transient private var lastKey: java.util.List[Any] = _

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray
  private val timeColumnAlias: String = Constants.TimeColumn

  // Flink managed state
  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _

  private var nextEventTimerState: ValueState[java.lang.Long] = _
  private var nextProcessingTimerState: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")

    tileState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Byte]]("mega-tile-tiles", classOf[String], classOf[Array[Byte]]))
    megaTileIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("mega-tile-ir", classOf[Array[Byte]]))
    largeTodayIrState =
      getRuntimeContext.getState(new ValueStateDescriptor[Array[Byte]]("mega-tile-large-today", classOf[Array[Byte]]))
    largeYesterdayIrState = getRuntimeContext.getState(
      new ValueStateDescriptor[Array[Byte]]("mega-tile-large-yesterday", classOf[Array[Byte]]))
    currentDayStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-day-start", classOf[java.lang.Long]))
    earliestTileStartState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-earliest-tile", classOf[java.lang.Long]))
    nextEventTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-next-event-timer", classOf[java.lang.Long]))
    nextProcessingTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[java.lang.Long]("mega-tile-next-processing-timer", classOf[java.lang.Long]))

    initializeTransients()
  }

  private def initializeTransients(): Unit = {
    val inputCols = inputSchema.map { case (name, dt) => (name, dt) }
    val megaTileAgg = new MegaTileAggregator(groupBy.getAggregations.iterator().toScala.toSeq, inputCols)
    megaTileCodec = new MegaTileCodec(groupBy, inputCols)
    flinkStore = new FlinkTileStore(megaTileAgg, megaTileCodec)
    processor = new MegaTileStreamProcessor(megaTileAgg, flinkStore)
  }

  private def ensureStateBound(currentKey: java.util.List[Any]): Unit = {
    if (lastKey == null || !lastKey.equals(currentKey)) {
      lastKey = currentKey
      flinkStore.bindFlinkState(tileState,
                                megaTileIrState,
                                largeTodayIrState,
                                largeYesterdayIrState,
                                currentDayStartState,
                                earliestTileStartState)
    }
  }

  override def processElement(
      event: ProjectedEvent,
      ctx: KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile]#Context,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      val element = event.fields
      val tsMills = Try(element(timeColumnAlias).asInstanceOf[Long])
        .getOrElse(element(timeColumnAlias).asInstanceOf[Double].toLong)
      val values: Array[Any] = valueColumns.map(element(_))
      val row = new ArrayRow(values, tsMills)

      ensureStateBound(ctx.getCurrentKey)
      val timerService = ctx.timerService()
      val watermark = timerService.currentWatermark()
      val processingTs = timerService.currentProcessingTime()

      processor.advanceWatermark(watermark)
      val result = processor.onEvent(row, tsMills)

      // Emit results
      val keys = ctx.getCurrentKey
      if (result.todayEntry != null) {
        out.collect(
          new TimestampedTile(keys,
                              megaTileCodec.encode(result.todayEntry),
                              result.todayStart,
                              event.startProcessingTimeMillis))
      }
      if (result.yesterdayEntry != null) {
        out.collect(
          new TimestampedTile(keys,
                              megaTileCodec.encode(result.yesterdayEntry),
                              result.yesterdayStart,
                              event.startProcessingTimeMillis))
      }

      scheduleSmallWindowTimers(computeSmallWindowTimerMode(tsMills, watermark, processingTs),
                                tsMills,
                                processingTs,
                                timerService)
    } catch {
      case e: Exception =>
        logger.error(s"Error processing mega tile event for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile]#OnTimerContext,
      out: Collector[TimestampedTile]
  ): Unit = {
    try {
      if (processor == null) initializeTransients()

      ensureStateBound(ctx.getCurrentKey)
      val timerService = ctx.timerService()
      val watermark = timerService.currentWatermark()
      val processingTs = timerService.currentProcessingTime()
      val caughtUp = isCaughtUp(watermark, processingTs)
      clearFiredTimerState(ctx.timeDomain(), timestamp)

      if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME && !caughtUp) {
        if (watermark == Long.MinValue) {
          // Restored processing-time timers can fire before the source emits its first watermark.
          if (hasActiveSmallWindowState) {
            scheduleProcessingTimeTimerIfNeeded(
              timerService,
              TsUtils.round(processingTs, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize)
          }
          return
        }

        cancelProcessingTimeTimerIfPresent(timerService)
        if (hasActiveSmallWindowState) {
          scheduleEventTimeTimerIfNeeded(
            timerService,
            TsUtils.round(watermark, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize)
        }
        return
      }

      val evictionTime =
        if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
          processor.advanceWatermark(timestamp)
          timestamp
        } else if (caughtUp) {
          // Late event-time timers in live mode should rebuild at wall clock, not stale event time.
          processor.advanceWatermark(processingTs)
          processingTs
        } else {
          processor.advanceWatermark(watermark)
          timestamp
        }

      val result = processor.onEviction(evictionTime)

      if (result.todayEntry != null) {
        out.collect(
          new TimestampedTile(ctx.getCurrentKey,
                              megaTileCodec.encode(result.todayEntry),
                              result.todayStart,
                              System.currentTimeMillis()))
      }

      scheduleSmallWindowTimers(computeSmallWindowTimerMode(evictionTime, watermark, processingTs),
                                evictionTime,
                                processingTs,
                                timerService)
    } catch {
      case e: Exception =>
        logger.error(s"Error in mega tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  private def hasActiveSmallWindowState: Boolean =
    processor.hasSmallWindows &&
      Option(earliestTileStartState.value()).exists(_.longValue() != Long.MaxValue)

  private def catchupThresholdMillis: Long =
    FlinkJob.AllowedOutOfOrderness.toMillis + processor.minSmallWindowTileSize

  private def isCaughtUp(watermark: Long, processingTs: Long): Boolean =
    watermark > 0 && (processingTs - watermark) <= catchupThresholdMillis

  private def computeSmallWindowTimerMode(
      eventTs: Long,
      watermark: Long,
      processingTs: Long): SmallWindowTimerMode = {
    if (!hasActiveSmallWindowState) {
      SmallWindowTimerMode.NoSmallState
    } else if (!isCaughtUp(watermark, processingTs)) {
      SmallWindowTimerMode.CatchupEventTimeOnly
    } else if (eventTs < processingTs - processor.minSmallWindowTileSize) {
      SmallWindowTimerMode.LiveProcessingTimeAndLateEventTime
    } else {
      SmallWindowTimerMode.LiveProcessingTimeOnly
    }
  }

  private def scheduleSmallWindowTimers(mode: SmallWindowTimerMode,
                                        eventTs: Long,
                                        processingTs: Long,
                                        timerService: TimerService): Unit = {
    mode match {
      case SmallWindowTimerMode.NoSmallState =>
        cancelEventTimeTimerIfPresent(timerService)
        cancelProcessingTimeTimerIfPresent(timerService)

      case SmallWindowTimerMode.CatchupEventTimeOnly =>
        cancelProcessingTimeTimerIfPresent(timerService)
        scheduleEventTimeTimerIfNeeded(
          timerService,
          TsUtils.round(eventTs, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize)

      case SmallWindowTimerMode.LiveProcessingTimeOnly =>
        cancelEventTimeTimerIfPresent(timerService)
        scheduleProcessingTimeTimerIfNeeded(
          timerService,
          TsUtils.round(processingTs, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize)

      case SmallWindowTimerMode.LiveProcessingTimeAndLateEventTime =>
        scheduleProcessingTimeTimerIfNeeded(
          timerService,
          TsUtils.round(processingTs, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize)
        scheduleEventTimeTimerIfNeeded(
          timerService,
          TsUtils.round(eventTs, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize)
    }
  }

  private def scheduleEventTimeTimerIfNeeded(timerService: TimerService, timestamp: Long): Unit = {
    val current = nextEventTimerState.value()
    if (current == null || timestamp < current.longValue()) {
      if (current != null) timerService.deleteEventTimeTimer(current.longValue())
      timerService.registerEventTimeTimer(timestamp)
      nextEventTimerState.update(timestamp)
    }
  }

  private def scheduleProcessingTimeTimerIfNeeded(timerService: TimerService, timestamp: Long): Unit = {
    val current = nextProcessingTimerState.value()
    if (current == null || timestamp < current.longValue()) {
      if (current != null) timerService.deleteProcessingTimeTimer(current.longValue())
      timerService.registerProcessingTimeTimer(timestamp)
      nextProcessingTimerState.update(timestamp)
    }
  }

  private def cancelEventTimeTimerIfPresent(timerService: TimerService): Unit = {
    val current = nextEventTimerState.value()
    if (current != null) {
      timerService.deleteEventTimeTimer(current.longValue())
      nextEventTimerState.clear()
    }
  }

  private def cancelProcessingTimeTimerIfPresent(timerService: TimerService): Unit = {
    val current = nextProcessingTimerState.value()
    if (current != null) {
      timerService.deleteProcessingTimeTimer(current.longValue())
      nextProcessingTimerState.clear()
    }
  }

  private def clearFiredTimerState(timeDomain: TimeDomain, timestamp: Long): Unit = {
    timeDomain match {
      case TimeDomain.EVENT_TIME =>
        val current = nextEventTimerState.value()
        if (current != null && current.longValue() == timestamp) {
          nextEventTimerState.clear()
        }
      case TimeDomain.PROCESSING_TIME =>
        val current = nextProcessingTimerState.value()
        if (current != null && current.longValue() == timestamp) {
          nextProcessingTimerState.clear()
        }
    }
  }
}

sealed private trait SmallWindowTimerMode

private object SmallWindowTimerMode {
  case object NoSmallState extends SmallWindowTimerMode
  case object CatchupEventTimeOnly extends SmallWindowTimerMode
  case object LiveProcessingTimeOnly extends SmallWindowTimerMode
  case object LiveProcessingTimeAndLateEventTime extends SmallWindowTimerMode
}

/** TileStore backed by Flink's keyed MapState/ValueState.
  * Encodes/decodes only the entries actually accessed — no bulk restore/persist.
  * Windowed IR get/put is memoized to avoid redundant decodes within the same event
  * (e.g., onEvent reads cachedSmallWindowIr, then packTodayEntry reads it again).
  * Cache is invalidated on key switch via bindFlinkState.
  */
class FlinkTileStore(megaTileAgg: MegaTileAggregator, codec: MegaTileCodec) extends TileStore {
  private val windowedAgg = megaTileAgg.windowedAggregator

  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _

  // Per-access decode cache for windowed IRs. Avoids redundant Avro decodes
  // when the same IR is read multiple times within one event (get → update → pack).
  // Invalidated on key switch (bindFlinkState) and updated on put.
  private var cachedSmallDecoded: Array[Any] = _
  private var cachedSmallValid: Boolean = false
  private var largeTodayDecoded: Array[Any] = _
  private var largeTodayValid: Boolean = false
  private var largeYesterdayDecoded: Array[Any] = _
  private var largeYesterdayValid: Boolean = false

  def bindFlinkState(tiles: MapState[String, Array[Byte]],
                     megaTileIr: ValueState[Array[Byte]],
                     largeToday: ValueState[Array[Byte]],
                     largeYesterday: ValueState[Array[Byte]],
                     dayStart: ValueState[java.lang.Long],
                     earliest: ValueState[java.lang.Long]): Unit = {
    tileState = tiles
    megaTileIrState = megaTileIr
    largeTodayIrState = largeToday
    largeYesterdayIrState = largeYesterday
    currentDayStartState = dayStart
    earliestTileStartState = earliest
    // Invalidate decode cache on key switch
    cachedSmallValid = false
    largeTodayValid = false
    largeYesterdayValid = false
  }

  private def tileKey(hopSize: Long, tileStart: Long): String = s"$hopSize:$tileStart"

  override def getTile(hopSize: Long, tileStart: Long): Array[Any] = {
    val bytes = tileState.get(tileKey(hopSize, tileStart))
    if (bytes != null) codec.decodeBaseIr(bytes) else null
  }

  override def putTile(hopSize: Long, tileStart: Long, ir: Array[Any]): Unit =
    tileState.put(tileKey(hopSize, tileStart), codec.encodeBaseIr(ir))

  override def removeTile(hopSize: Long, tileStart: Long): Unit =
    tileState.remove(tileKey(hopSize, tileStart))

  override def tileIterator: Iterator[(Long, Long, Array[Any])] = {
    val iter = tileState.iterator()
    new Iterator[(Long, Long, Array[Any])] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): (Long, Long, Array[Any]) = {
        val entry = iter.next()
        val parts = entry.getKey.split(":")
        (parts(0).toLong, parts(1).toLong, codec.decodeBaseIr(entry.getValue))
      }
    }
  }

  private def decodeWindowedIr(state: ValueState[Array[Byte]]): Array[Any] = {
    val bytes = state.value()
    if (bytes != null) codec.decode(bytes) else windowedAgg.init
  }

  override def getCachedSmallWindowIr: Array[Any] = {
    if (!cachedSmallValid) { cachedSmallDecoded = decodeWindowedIr(megaTileIrState); cachedSmallValid = true }
    cachedSmallDecoded
  }
  override def putCachedSmallWindowIr(ir: Array[Any]): Unit = {
    megaTileIrState.update(codec.encode(ir))
    cachedSmallDecoded = ir; cachedSmallValid = true
  }

  override def getLargeTodayIr: Array[Any] = {
    if (!largeTodayValid) { largeTodayDecoded = decodeWindowedIr(largeTodayIrState); largeTodayValid = true }
    largeTodayDecoded
  }
  override def putLargeTodayIr(ir: Array[Any]): Unit = {
    largeTodayIrState.update(codec.encode(ir))
    largeTodayDecoded = ir; largeTodayValid = true
  }

  override def getLargeYesterdayIr: Array[Any] = {
    if (!largeYesterdayValid) {
      largeYesterdayDecoded = decodeWindowedIr(largeYesterdayIrState); largeYesterdayValid = true
    }
    largeYesterdayDecoded
  }
  override def putLargeYesterdayIr(ir: Array[Any]): Unit = {
    largeYesterdayIrState.update(codec.encode(ir))
    largeYesterdayDecoded = ir; largeYesterdayValid = true
  }

  override def getCurrentDayStart: Long = Option(currentDayStartState.value()).map(_.longValue()).getOrElse(-1L)
  override def putCurrentDayStart(ts: Long): Unit = currentDayStartState.update(ts)

  override def getEarliestTileStart: Long =
    Option(earliestTileStartState.value()).map(_.longValue()).getOrElse(Long.MaxValue)
  override def putEarliestTileStart(ts: Long): Unit = earliestTileStartState.update(ts)
}
