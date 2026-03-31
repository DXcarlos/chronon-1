package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{MegaTileAggregator, MegaTileStreamProcessor, TileStore}
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.MegaTileCodec
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.TimeDomain
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
  * - Transition is one-way: catchup → live. Once caught up, stays in live mode.
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

  // Tracks whether this entity has a processing-time timer registered.
  // Not persisted — after checkpoint restore, first event re-registers.
  @transient private var hasProcessingTimeTimer: Boolean = false

  // Once true, stays true for the lifetime of this subtask.
  @transient private var isLive: Boolean = false

  // Threshold: watermark within 2× the allowed lateness of wall clock → caught up.
  private val CatchupThresholdMillis: Long = 2 * 60 * 1000L // 2 minutes

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

  private def isCaughtUp(watermark: Long, procNow: Long): Boolean =
    watermark > 0 && (procNow - watermark) <= CatchupThresholdMillis

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
      processor.advanceWatermark(ctx.timerService().currentWatermark())
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

      // Schedule eviction timer if small windows exist
      if (processor.hasSmallWindows) {
        val hopSize = processor.minSmallWindowTileSize
        val watermark = ctx.timerService().currentWatermark()
        val procNow = ctx.timerService().currentProcessingTime()

        if (!isLive && isCaughtUp(watermark, procNow)) {
          isLive = true
          if (enableDebug) logger.info(s"Transitioning to live mode: watermark=$watermark procNow=$procNow")
        }

        if (isLive) {
          // Live: processing-time timer aligned to hop boundary from wall clock.
          // Fires at wall-clock intervals so idle entities get timely sawtooth correction.
          if (!hasProcessingTimeTimer) {
            val nextEviction = TsUtils.round(procNow, hopSize) + hopSize
            ctx.timerService().registerProcessingTimeTimer(nextEviction)
            hasProcessingTimeTimer = true
          }
        } else {
          // Catchup: event-time timer. Fires as watermark advances through the backlog.
          val nextEviction = TsUtils.round(tsMills, hopSize) + hopSize
          ctx.timerService().registerEventTimeTimer(nextEviction)
        }
      }
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
      processor.advanceWatermark(ctx.timerService().currentWatermark())

      // Both event-time and processing-time timers run the same eviction logic.
      // The timerTs is the eviction point — either the event-time boundary (catchup)
      // or the wall-clock boundary (live).
      val result = processor.onEviction(timestamp)

      if (result.todayEntry != null) {
        out.collect(
          new TimestampedTile(ctx.getCurrentKey,
                              megaTileCodec.encode(result.todayEntry),
                              result.todayStart,
                              System.currentTimeMillis()))
      }

      // Re-register the next timer
      if (processor.hasSmallWindows) {
        val hopSize = processor.minSmallWindowTileSize
        if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
          // Live mode: re-register next processing-time timer at next hop boundary
          val nextEviction = TsUtils.round(timestamp, hopSize) + hopSize
          ctx.timerService().registerProcessingTimeTimer(nextEviction)
        } else {
          // Catchup mode: re-register next event-time timer
          ctx.timerService().registerEventTimeTimer(timestamp + hopSize)
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error in mega tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }
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
