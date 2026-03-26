package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

/**
  * Pure Scala state manager for the MegaTile streaming pipeline.
  * Contains ALL logic for tile updates, day transitions, late event routing,
  * eviction, and IR packing. No Flink imports — testable in isolation.
  *
  * State layout:
  *   - Small window tiles: per-tier Map[tileStart -> base IR], only for tiers with isNoBatch columns.
  *     On emission, small-window columns are built from tiles via buildMegaTileIr (correct per-column scoping).
  *   - Large window today/yesterday IRs: per-day accumulators for large/unwindowed columns.
  *     Incremental update on each event.
  *   - Day transitions are watermark-driven (advanceWatermark), not event-driven,
  *     to prevent future-timestamped events from prematurely rotating state.
  */
class MegaTileStreamProcessor(val megaTileAgg: MegaTileAggregator) {

  val DayMillis: Long = 24 * 3600 * 1000L

  private val windowedAgg = megaTileAgg.windowedAggregator
  private val baseAgg = megaTileAgg.baseAggregator
  private val isNoBatch = megaTileAgg.isNoBatch
  private val columnHopSize = megaTileAgg.columnHopSize

  // Tiers that have at least one small-window column (tiles only needed for these)
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

  // ---- Mutable state ----

  // Small window tiles: per small-window tier only
  val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
    smallWindowTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap

  // Large window per-day accumulators (only large/unwindowed column positions populated)
  var largeTodayIr: Array[Any] = windowedAgg.init
  var largeYesterdayIr: Array[Any] = windowedAgg.init

  var currentDayStart: Long = -1L
  var earliestTileStart: Long = Long.MaxValue

  /**
    * Process a new event. Returns an EmitResult describing what should be
    * written to KV store (today and/or yesterday entries).
    */
  def onEvent(row: Row, eventTs: Long): EmitResult = {
    // Lazy init currentDayStart from first event
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
    }

    var todayDirty = false
    var yesterdayDirty = false

    // Update tiles for small-window tiers.
    // Tiles are the source of truth for small-window columns; on emission,
    // buildMegaTileIr scopes each column to its effective window.
    if (hasSmallWindows) {
      val tileStarts = megaTileAgg.tileStartsForEvent(eventTs)
      for ((hopSize, tileStart) <- tileStarts) {
        if (smallWindowTiers.contains(hopSize)) {
          val floor = megaTileAgg.retentionFloor(hopSize, eventTs, currentDayStart)
          val ceiling = currentDayStart + 2 * DayMillis // reject tiles >= 2 days ahead (state leak guard)
          if (tileStart >= floor && tileStart < ceiling) {
            val tierTiles = tiles(hopSize)
            val ir = tierTiles.getOrElseUpdate(tileStart, baseAgg.init)
            baseAgg.update(ir, row)
            if (tileStart < earliestTileStart) earliestTileStart = tileStart
            todayDirty = true
          }
        }
      }
    }

    // Update large window IR — route by event day
    val todayStart = currentDayStart
    val nextDayStart = todayStart + DayMillis
    val yesterdayStart = todayStart - DayMillis

    if (eventTs >= nextDayStart) {
      // Future event — clamp to today. Day transitions are watermark-driven.
      updateLargeWindowColumns(largeTodayIr, row)
      todayDirty = true
    } else if (eventTs >= todayStart) {
      updateLargeWindowColumns(largeTodayIr, row)
      todayDirty = true
    } else if (eventTs >= yesterdayStart) {
      // Late event from yesterday — within 2d tolerance
      updateLargeWindowColumns(largeYesterdayIr, row)
      yesterdayDirty = true
    }
    // else: > 2 days late → dropped for large windows (tiles may still capture it above)

    EmitResult(
      todayEntry = if (todayDirty) packTodayEntry(eventTs) else null,
      todayStart = todayStart,
      yesterdayEntry = if (yesterdayDirty) packYesterdayEntry() else null,
      yesterdayStart = yesterdayStart
    )
  }

  /**
    * Advance the watermark. Day transitions happen ONLY here, never in onEvent.
    * This prevents future-timestamped events from prematurely rotating state.
    */
  def advanceWatermark(watermarkTs: Long): Unit = {
    if (currentDayStart == -1L) return
    val wmDay = TsUtils.round(watermarkTs, DayMillis)
    if (wmDay > currentDayStart) {
      largeYesterdayIr = largeTodayIr
      largeTodayIr = windowedAgg.init
      currentDayStart = wmDay
    }
  }

  /**
    * Evict stale tiles. Called on timer (watermark-driven interval = minSmallWindowTileSize).
    * Returns an EmitResult with updated today entry.
    */
  def onEviction(timerTs: Long): EmitResult = {
    if (!hasSmallWindows || earliestTileStart == Long.MaxValue) {
      return EmitResult(null, currentDayStart, null, currentDayStart - DayMillis)
    }

    val todayStart = currentDayStart

    // Evict stale tiles per tier
    for (hopSize <- smallWindowTiers) {
      val floor = megaTileAgg.retentionFloor(hopSize, timerTs, todayStart)
      val tierTiles = tiles(hopSize)
      val staleKeys = tierTiles.keys.filter(_ < floor).toSeq
      staleKeys.foreach(tierTiles.remove)
    }

    // Update earliestTileStart from remaining tiles
    earliestTileStart = Long.MaxValue
    for ((_, tierTiles) <- tiles; ts <- tierTiles.keys) {
      if (ts < earliestTileStart) earliestTileStart = ts
    }

    EmitResult(
      todayEntry = packTodayEntry(timerTs),
      todayStart = todayStart,
      yesterdayEntry = null,
      yesterdayStart = todayStart - DayMillis
    )
  }

  /**
    * Pack today's KV entry.
    * Small-window columns are built from tiles via buildMegaTileIr (correct per-column scoping).
    * Large-window columns come from the running daily accumulator.
    */
  def packTodayEntry(now: Long): Array[Any] = {
    val todayStart = currentDayStart
    // Build small-window columns from tiles (correct per-column effective window)
    val smallIr = if (hasSmallWindows) megaTileAgg.buildMegaTileIr(tiles, now, batchEnd = todayStart) else null

    val entry = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      if (isNoBatch(col)) {
        entry(col) = if (smallIr != null) smallIr(col) else null
      } else {
        entry(col) = largeTodayIr(col)
      }
      col += 1
    }
    entry
  }

  /** Pack yesterday's KV entry: null for small-window cols, large-yesterday for large cols. */
  def packYesterdayEntry(): Array[Any] = {
    val entry = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      entry(col) = if (isNoBatch(col)) null else largeYesterdayIr(col)
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
