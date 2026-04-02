package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

/** Pure Scala state manager for the MegaTile streaming pipeline.
  * All state access goes through a TileStore — Flink backs it with MapState/ValueState + codec,
  * tests back it with InMemoryTileStore. No bulk restore/persist cycle; reads are lazy,
  * writes are immediate to the touched entries only.
  *
  * State layout:
  *   - Small window tiles: per-tier base IRs. Source of truth for eviction rebuilds.
  *   - cachedSmallWindowIr: sawtooth running sum, corrected on eviction.
  *   - Large window today/yesterday IRs: per-day accumulators.
  *   - Day transitions are watermark-driven (advanceWatermark), not event-driven.
  */
class MegaTileStreamProcessor(val megaTileAgg: MegaTileAggregator, val store: TileStore) {

  val DayMillis: Long = 24 * 3600 * 1000L

  private val windowedAgg = megaTileAgg.windowedAggregator
  private val baseAgg = megaTileAgg.baseAggregator
  private val isNoBatch = megaTileAgg.isNoBatch
  private val columnHopSize = megaTileAgg.columnHopSize

  val smallWindowTiers: Set[Long] = {
    val tiers = mutable.Set.empty[Long]
    var col = 0
    while (col < windowedAgg.length) {
      if (isNoBatch(col)) tiers += columnHopSize(col)
      col += 1
    }
    tiers.toSet
  }

  val hasSmallWindows: Boolean = smallWindowTiers.nonEmpty
  val minSmallWindowTileSize: Long = if (hasSmallWindows) smallWindowTiers.min else DayMillis

  def onEvent(row: Row, eventTs: Long): EmitResult = {
    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
      store.putCurrentDayStart(currentDayStart)
    }

    var todayDirty = false
    var yesterdayDirty = false

    // Update tiles + cachedSmallWindowIr for small-window tiers
    if (hasSmallWindows) {
      var tilesUpdated = false
      val tileStarts = megaTileAgg.tileStartsForEvent(eventTs)
      for ((hopSize, tileStart) <- tileStarts) {
        if (smallWindowTiers.contains(hopSize)) {
          val floor = megaTileAgg.retentionFloor(hopSize, eventTs, currentDayStart)
          val ceiling = currentDayStart + 2 * DayMillis
          if (tileStart >= floor && tileStart < ceiling) {
            val existing = store.getTile(hopSize, tileStart)
            val ir = if (existing != null) existing else baseAgg.init
            baseAgg.update(ir, row)
            store.putTile(hopSize, tileStart, ir)
            val earliest = store.getEarliestTileStart
            if (tileStart < earliest) store.putEarliestTileStart(tileStart)
            tilesUpdated = true
          }
        }
      }

      // Sawtooth: merge event into running IR for all small-window columns.
      if (tilesUpdated) {
        val cachedIr = store.getCachedSmallWindowIr
        var col = 0
        while (col < windowedAgg.length) {
          if (isNoBatch(col)) {
            windowedAgg.columnAggregators(col).update(cachedIr, row)
          }
          col += 1
        }
        store.putCachedSmallWindowIr(cachedIr)
        todayDirty = true
      }
    }

    // Update large window IR — route by event day
    val todayStart = currentDayStart
    val nextDayStart = todayStart + DayMillis
    val yesterdayStart = todayStart - DayMillis

    if (eventTs >= nextDayStart) {
      // Future event — clamp to today. Day transitions are watermark-driven.
      val ir = store.getLargeTodayIr
      updateLargeWindowColumns(ir, row)
      store.putLargeTodayIr(ir)
      todayDirty = true
    } else if (eventTs >= todayStart) {
      val ir = store.getLargeTodayIr
      updateLargeWindowColumns(ir, row)
      store.putLargeTodayIr(ir)
      todayDirty = true
    } else if (eventTs >= yesterdayStart) {
      // Late event from yesterday — within 2d tolerance
      val ir = store.getLargeYesterdayIr
      updateLargeWindowColumns(ir, row)
      store.putLargeYesterdayIr(ir)
      yesterdayDirty = true
    }

    EmitResult(
      todayEntry = if (todayDirty) packTodayEntry() else null,
      todayStart = todayStart,
      yesterdayEntry = if (yesterdayDirty) packYesterdayEntry() else null,
      yesterdayStart = yesterdayStart
    )
  }

  def advanceWatermark(watermarkTs: Long): Unit = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return
    val wmDay = TsUtils.round(watermarkTs, DayMillis)
    if (wmDay > currentDayStart) {
      val newYesterday =
        if (wmDay == currentDayStart + DayMillis) store.getLargeTodayIr
        else windowedAgg.init
      store.putLargeYesterdayIr(newYesterday)
      store.putLargeTodayIr(windowedAgg.init)
      store.putCurrentDayStart(wmDay)
    }
  }

  def onEviction(timerTs: Long): EmitResult = {
    val earliest = store.getEarliestTileStart
    val currentDayStart = store.getCurrentDayStart
    if (!hasSmallWindows || earliest == Long.MaxValue) {
      return EmitResult(null, currentDayStart, null, currentDayStart - DayMillis)
    }

    val todayStart = currentDayStart

    // Build the retained-tile snapshot while collecting stale keys, then delete stale state after iteration.
    val staleEntries = mutable.ArrayBuffer.empty[(Long, Long)]
    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      smallWindowTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap
    var newEarliest = Long.MaxValue
    val iter = store.tileIterator
    while (iter.hasNext) {
      val (hopSize, tileStart, ir) = iter.next()
      if (smallWindowTiers.contains(hopSize)) {
        val floor = megaTileAgg.retentionFloor(hopSize, timerTs, todayStart)
        if (tileStart < floor) {
          staleEntries += ((hopSize, tileStart))
        } else {
          tiles(hopSize)(tileStart) = ir
          if (tileStart < newEarliest) newEarliest = tileStart
        }
      }
    }
    staleEntries.foreach { case (h, t) => store.removeTile(h, t) }
    store.putEarliestTileStart(newEarliest)

    val rebuiltIr = megaTileAgg.buildMegaTileIr(tiles, now = timerTs, batchEnd = todayStart)
    store.putCachedSmallWindowIr(rebuiltIr)

    EmitResult(
      todayEntry = packTodayEntry(),
      todayStart = todayStart,
      yesterdayEntry = null,
      yesterdayStart = todayStart - DayMillis
    )
  }

  def packTodayEntry(): Array[Any] = {
    val cachedIr = store.getCachedSmallWindowIr
    val largeIr = store.getLargeTodayIr
    val entry = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      entry(col) = if (isNoBatch(col)) cachedIr(col) else largeIr(col)
      col += 1
    }
    entry
  }

  def packYesterdayEntry(): Array[Any] = {
    val largeIr = store.getLargeYesterdayIr
    val entry = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      entry(col) = if (isNoBatch(col)) null else largeIr(col)
      col += 1
    }
    entry
  }

  private def updateLargeWindowColumns(ir: Array[Any], row: Row): Unit = {
    var col = 0
    while (col < windowedAgg.length) {
      if (!isNoBatch(col)) {
        windowedAgg.columnAggregators(col).update(ir, row)
      }
      col += 1
    }
  }
}

case class EmitResult(
    todayEntry: Array[Any],
    todayStart: Long,
    yesterdayEntry: Array[Any],
    yesterdayStart: Long
)
