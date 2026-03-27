package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.{MegaTileAggregator, MegaTileStreamProcessor}
import ai.chronon.api.{Constants, DataType, GroupBy, TsUtils}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.MegaTileCodec
import ai.chronon.online.TileCodec
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
  * Flink KeyedProcessFunction that maintains per-entity mega tile state.
  * Delegates all aggregation logic to MegaTileStreamProcessor (pure Scala, no Flink deps).
  * Handles Flink-specific concerns: state serde, timer registration, output collection.
  */
class MegaTileProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false
) extends KeyedProcessFunction[java.util.List[Any], ProjectedEvent, TimestampedTile] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Transient: rebuilt on checkpoint restore
  @transient private var processor: MegaTileStreamProcessor = _
  @transient private var megaTileCodec: MegaTileCodec = _
  @transient private var tileCodec: TileCodec = _

  @transient private var eventProcessingErrorCounter: Counter = _

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray
  private val timeColumnAlias: String = Constants.TimeColumn

  // Flink managed state — survives checkpoints
  // Tiles are stored as encoded bytes to avoid custom TypeSerializer.
  // Key format: "hopSize:tileStart"
  private var tileState: MapState[String, Array[Byte]] = _
  private var megaTileIrState: ValueState[Array[Byte]] = _
  private var largeTodayIrState: ValueState[Array[Byte]] = _
  private var largeYesterdayIrState: ValueState[Array[Byte]] = _
  private var currentDayStartState: ValueState[java.lang.Long] = _
  private var earliestTileStartState: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")

    tileState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Byte]]("mega-tile-tiles", classOf[String], classOf[Array[Byte]]))
    megaTileIrState = getRuntimeContext.getState(
      new ValueStateDescriptor[Array[Byte]]("mega-tile-ir", classOf[Array[Byte]]))
    largeTodayIrState = getRuntimeContext.getState(
      new ValueStateDescriptor[Array[Byte]]("mega-tile-large-today", classOf[Array[Byte]]))
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
    val megaTileAgg = new MegaTileAggregator(
      groupBy.getAggregations.iterator().toScala.toSeq,
      inputCols
    )
    processor = new MegaTileStreamProcessor(megaTileAgg)
    megaTileCodec = new MegaTileCodec(groupBy, inputCols)
    tileCodec = new TileCodec(groupBy, inputCols)
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

      // Restore processor state from Flink state
      restoreProcessorState()

      // Advance watermark (may trigger day transition)
      processor.advanceWatermark(ctx.timerService().currentWatermark())

      // Process event
      val result = processor.onEvent(row, tsMills)

      // Persist processor state back to Flink state
      persistProcessorState()

      // Emit results
      val keys = ctx.getCurrentKey
      if (result.todayEntry != null) {
        out.collect(new TimestampedTile(keys, megaTileCodec.encode(result.todayEntry),
                                        tsMills, event.startProcessingTimeMillis))
      }
      if (result.yesterdayEntry != null) {
        out.collect(new TimestampedTile(keys, megaTileCodec.encode(result.yesterdayEntry),
                                        result.yesterdayStart, event.startProcessingTimeMillis))
      }

      // Register eviction timer
      if (processor.hasSmallWindows) {
        val nextEviction = TsUtils.round(tsMills, processor.minSmallWindowTileSize) + processor.minSmallWindowTileSize
        ctx.timerService().registerEventTimeTimer(nextEviction)
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

      restoreProcessorState()
      processor.advanceWatermark(ctx.timerService().currentWatermark())

      val result = processor.onEviction(timestamp)
      persistProcessorState()

      if (result.todayEntry != null) {
        val keys = ctx.getCurrentKey
        out.collect(new TimestampedTile(keys, megaTileCodec.encode(result.todayEntry),
                                        timestamp, System.currentTimeMillis()))
      }

      // Register next eviction timer
      if (processor.hasSmallWindows) {
        val nextEviction = timestamp + processor.minSmallWindowTileSize
        ctx.timerService().registerEventTimeTimer(nextEviction)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error in mega tile eviction for groupBy=${groupBy.getMetaData.getName}", e)
        eventProcessingErrorCounter.inc()
    }
  }

  // Restore MegaTileStreamProcessor mutable state from Flink managed state
  private def restoreProcessorState(): Unit = {
    // Tiles
    processor.tiles.values.foreach(_.clear())
    val tileIter = tileState.iterator()
    while (tileIter.hasNext) {
      val entry = tileIter.next()
      val parts = entry.getKey.split(":")
      val hopSize = parts(0).toLong
      val tileStart = parts(1).toLong
      processor.tiles.get(hopSize).foreach { tierTiles =>
        val (ir, _) = tileCodec.decodeTileIr(entry.getValue)
        // decodeTileIr returns windowed form; we need base form
        // Use the base aggregator init size to extract base IR
        tierTiles(tileStart) = processor.megaTileAgg.baseAggregator.denormalize(
          processor.megaTileAgg.baseAggregator.normalize(ir.take(processor.megaTileAgg.baseAggregator.length)))
      }
    }

    // Cached small window IR + large window IRs
    Option(megaTileIrState.value()).foreach(bytes => processor.cachedSmallWindowIr = megaTileCodec.decode(bytes))
    Option(largeTodayIrState.value()).foreach(bytes => processor.largeTodayIr = megaTileCodec.decode(bytes))
    Option(largeYesterdayIrState.value()).foreach(bytes => processor.largeYesterdayIr = megaTileCodec.decode(bytes))
    Option(currentDayStartState.value()).foreach(v => processor.currentDayStart = v)
    Option(earliestTileStartState.value()).foreach(v => processor.earliestTileStart = v)
  }

  // Persist MegaTileStreamProcessor mutable state to Flink managed state
  private def persistProcessorState(): Unit = {
    // Tiles
    tileState.clear()
    for ((hopSize, tierTiles) <- processor.tiles; (tileStart, ir) <- tierTiles) {
      val key = s"$hopSize:$tileStart"
      tileState.put(key, tileCodec.makeTileIr(ir, isComplete = true))
    }

    // IRs
    megaTileIrState.update(megaTileCodec.encode(processor.cachedSmallWindowIr))
    largeTodayIrState.update(megaTileCodec.encode(processor.largeTodayIr))
    largeYesterdayIrState.update(megaTileCodec.encode(processor.largeYesterdayIr))
    currentDayStartState.update(processor.currentDayStart)
    earliestTileStartState.update(processor.earliestTileStart)
  }
}
