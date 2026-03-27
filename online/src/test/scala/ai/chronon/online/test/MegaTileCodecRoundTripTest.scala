package ai.chronon.online.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.NaiveAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import ai.chronon.online.MegaTileCodec
import ai.chronon.online.serde.ArrayRow
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

/**
  * Tests MegaTileStreamProcessor with serde round-trips at state boundaries,
  * simulating what MegaTileProcessFunction does in Flink (persist → restore).
  * Catches bugs where encode → decode doesn't preserve state.
  */
class MegaTileCodecRoundTripTest extends AnyFlatSpec {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  val AllWindows: Seq[Window] = Seq(
    new Window(6, TimeUnit.HOURS),
    new Window(1, TimeUnit.DAYS),
    new Window(47, TimeUnit.HOURS),
    new Window(2, TimeUnit.DAYS),
    new Window(49, TimeUnit.HOURS),
    new Window(3, TimeUnit.DAYS),
    new Window(7, TimeUnit.DAYS)
  )

  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val DayMillis: Long = new Window(1, TimeUnit.DAYS).millis
  val Epsilon = 1e-6

  val Schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType, "amount" -> DoubleType)

  def approxEqual(a: Any, b: Any): Boolean = (a, b) match {
    case (null, null)                                  => true
    case (null, _) | (_, null)                         => false
    case (x: Double, y: Double)                        => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Float, y: Float)                          => Math.abs(x - y) <= Epsilon.toFloat * Math.max(1.0f, Math.max(Math.abs(x), Math.abs(y)))
    case (x: java.util.List[_], y: java.util.List[_]) => x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i)))
    case (x: Array[_], y: Array[_])                    => x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b) }
    case _                                             => a == b
  }

  def generateEvents(daySpan: Int, count: Int): Array[Row] = {
    val rng = new Random(42)
    val baseTs = 1774000000000L
    val spanMillis = daySpan.toLong * DayMillis
    (0 until count).map { _ =>
      val ts = baseTs + (rng.nextDouble() * spanMillis).toLong
      val num = rng.nextInt(1000).toLong
      val amount = rng.nextDouble() * 500.0
      new ArrayRow(Array(ts, num, amount), ts): Row
    }.toArray
  }

  def naiveAggregate(allEvents: Array[Row], queryTimes: Array[Long], aggregations: Seq[Aggregation]): Array[Array[Any]] = {
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(Schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    naiveAgg.aggregate(allEvents, queryTimes).map(ir => rowAgg.finalize(ir))
  }

  def buildGroupBy(aggregations: Seq[Aggregation]): GroupBy = {
    import scala.collection.JavaConverters._
    val gb = new GroupBy()
    gb.setAggregations(aggregations.asJava)
    val meta = new MetaData()
    meta.setName("test_mega_tile_codec")
    gb.setMetaData(meta)
    gb
  }

  /** Simulate persist → restore: encode state, create fresh processor, decode into it. */
  def roundTripProcessorState(source: MegaTileStreamProcessor, codec: MegaTileCodec): MegaTileStreamProcessor = {
    val dest = new MegaTileStreamProcessor(source.megaTileAgg)

    // Tiles: encode/decode as base IRs
    dest.tiles.values.foreach(_.clear())
    for ((hopSize, tierTiles) <- source.tiles; (tileStart, ir) <- tierTiles) {
      dest.tiles.get(hopSize).foreach(_(tileStart) = codec.decodeBaseIr(codec.encodeBaseIr(ir)))
    }

    // Windowed IRs
    dest.cachedSmallWindowIr = codec.decode(codec.encode(source.cachedSmallWindowIr))
    dest.largeTodayIr = codec.decode(codec.encode(source.largeTodayIr))
    dest.largeYesterdayIr = codec.decode(codec.encode(source.largeYesterdayIr))

    dest.currentDayStart = source.currentDayStart
    dest.earliestTileStart = source.earliestTileStart
    dest
  }

  def streamProcessorWithSerdeAggregate(allEvents: Array[Row],
                                         queryTimes: Array[Long],
                                         aggregations: Seq[Aggregation],
                                         batchEnd: Long,
                                         roundTripInterval: Int = 50): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, Schema, tailBufferMillis = TailBufferMillis)
    var processor = new MegaTileStreamProcessor(megaTileAgg)
    val merger = new MegaTileMerger(megaTileAgg)
    val codec = new MegaTileCodec(buildGroupBy(aggregations), Schema)

    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, Schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val sortedEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted

    val evictionInterval = processor.minSmallWindowTileSize
    var nextEvictionTs = Long.MaxValue
    val kvStore = mutable.Map[Long, Array[Any]]()
    var eventIdx = 0
    var eventsSinceRoundTrip = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    def firePendingEvictions(upToTs: Long): Unit = {
      if (!processor.hasSmallWindows) return
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.todayEntry != null) kvStore(r.todayStart) = r.todayEntry
        nextEvictionTs += evictionInterval
      }
    }

    for (queryTs <- sortedQueries) {
      while (eventIdx < sortedEvents.length && sortedEvents(eventIdx).ts <= queryTs) {
        val event = sortedEvents(eventIdx)
        firePendingEvictions(event.ts)
        processor.advanceWatermark(event.ts)

        val result = processor.onEvent(event, event.ts)
        if (result.todayEntry != null) kvStore(result.todayStart) = result.todayEntry
        if (result.yesterdayEntry != null) kvStore(result.yesterdayStart) = result.yesterdayEntry

        if (nextEvictionTs == Long.MaxValue && processor.hasSmallWindows) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }

        // Serde round-trip every N events
        eventsSinceRoundTrip += 1
        if (eventsSinceRoundTrip >= roundTripInterval) {
          processor = roundTripProcessorState(processor, codec)
          eventsSinceRoundTrip = 0
        }

        eventIdx += 1
      }

      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)

      val (todayStart, yesterdayStart) = merger.streamingDayKeys(queryTs)
      val todayIr = processor.packTodayEntry()
      val yesterdayIr = kvStore.getOrElse(yesterdayStart, null)
      resultsByQueryTs(queryTs) = merger.merge(finalBatchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd)
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive with serde round-trips (batch fresh)" in {
    val events = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(batchEnd + 6 * 3600 * 1000L, batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)
    val results = streamProcessorWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(s"serde_fresh: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
                 approxEqual(results(i), naive(i)))
    }
  }

  it should "match naive with serde round-trips (all agg types)" in {
    val events = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.MIN, "num", AllWindows),
      Builders.Aggregation(Operation.MAX, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)
    val results = streamProcessorWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(s"serde_all_agg: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
                 approxEqual(results(i), naive(i)))
    }
  }
}
