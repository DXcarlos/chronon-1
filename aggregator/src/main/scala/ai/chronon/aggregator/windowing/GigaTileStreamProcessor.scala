package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

/** Pure Scala state manager for the GigaTile streaming pipeline.
  *
  * Flink holds the FinalBatchIr in state (loaded from Iceberg) and one daily large-window
  * IR slot per day with streaming events between batchEndDay and the watermark day. The
  * runningLargeIr cache is the merged batch + every retained daily slot, refreshed on
  * eviction and on batch update.
  *
  * State layout:
  *   - tiles, cachedSmallWindowIr: small-window sawtooth (per-hop tile state + cached IR)
  *   - dailyLargeIrs: per-day accumulator keyed by day-start. Slots with dayStart < batchEndDay
  *     are pruned when batch advances (their data is now in batch).
  *   - batchIr: FinalBatchIr from Iceberg (collapsed + tail hops)
  *   - batchEndTs: when batch was last computed
  *   - runningLargeIr: fully merged large-window IR (batch + every retained daily slot)
  */
class GigaTileStreamProcessor(
    val megaTileAgg: MegaTileAggregator,
    val store: GigaTileStore,
    // Returns true if two windowed IRs are equal (for batch update mismatch detection).
    // Default: always report mismatch (always emit on batch update).
    val irEqual: (Array[Any], Array[Any]) => Boolean = (_, _) => false,
    // Bound on how stale the batch can get before late events for the oldest covered days
    // are dropped. Keeps daily-IR state from growing unbounded if the Iceberg connected
    // stream stalls. 32 days is well past any realistic batch SLA.
    val maxBatchStalenessDays: Int = 32
) {

  val DayMillis: Long = 24 * 3600 * 1000L
  private val maxStalenessMillis: Long = maxBatchStalenessDays.toLong * DayMillis

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

  // Eviction cadence: smallest hop across ALL windows (not just small).
  // Large-window-only GroupBys need eviction for tail hop correction.
  val minEvictionInterval: Long = megaTileAgg.activeTiers.min

  val minSmallWindowTileSize: Long = if (smallWindowTiers.nonEmpty) smallWindowTiers.min else minEvictionInterval

  // Default as-of bumped to the next small-window hop boundary so an event at exactly eventTs
  // is included in its own emit (chronon's as-of is exclusive). Production callers should pass
  // smallWindowAsOfTs explicitly — derived from processingTs / watermark — so retained late
  // events don't get their own permissive horizon.
  private def defaultSmallWindowAsOfTs(eventTs: Long): Long =
    TsUtils.round(eventTs, minSmallWindowTileSize) + minSmallWindowTileSize

  private var lastEvictionPackedIr: Array[Any] = _

  // Hop indices only used by small (NO BATCH) windows — stripped from batch IR on load.
  // 5-min tail hops for ≤12h windows are never used by mergeTailHopsForBatchColumns.
  private[windowing] val smallWindowOnlyHopIndices: Set[Int] = {
    val usedByBatch = mutable.Set.empty[Int]
    var col = 0
    while (col < windowedAgg.length) {
      val window = megaTileAgg.windowMappings(col).aggregationPart.window
      if (!isNoBatch(col) && window != null) {
        usedByBatch += megaTileAgg.tailHopIndicesArray(col)
      }
      col += 1
    }
    (0 until megaTileAgg.hopSizesArray.length).filterNot(usedByBatch.contains(_)).toSet
  }

  def onEvent(row: Row, eventTs: Long): GigaEmitResult = onEvent(row, eventTs, defaultSmallWindowAsOfTs(eventTs))

  /** smallWindowAsOfTs is the as-of timestamp used to gate cached small-window IR updates.
    * Late retained events still update the underlying base tile (so a future eviction can
    * rebuild correctly), but only events inside each column's effective horizon contribute
    * to the live cached IR that gets emitted.
    */
  def onEvent(row: Row, eventTs: Long, smallWindowAsOfTs: Long): GigaEmitResult = {
    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
      store.putCurrentDayStart(currentDayStart)
    }

    var dirty = false

    // --- Small windows: update tiles, then update cached IR only for columns whose
    // tile lies inside the smallWindowAsOfTs horizon. ---
    if (hasSmallWindows) {
      val acceptedTileStarts = mutable.Map.empty[Long, Long]
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
            acceptedTileStarts(hopSize) = tileStart
          }
        }
      }

      if (acceptedTileStarts.nonEmpty) {
        var cachedIr: Array[Any] = null
        var cachedIrUpdated = false
        var col = 0
        while (col < windowedAgg.length) {
          if (isNoBatch(col)) {
            val hopSize = columnHopSize(col)
            acceptedTileStarts.get(hopSize).foreach { tileStart =>
              val effStart = megaTileAgg.effectiveStart(col, smallWindowAsOfTs, currentDayStart)
              // Retained late events: keep the base tile but skip cached-IR update so the live
              // emit reflects only events inside the as-of horizon for this column's window.
              if (tileStart >= effStart && tileStart < smallWindowAsOfTs) {
                if (cachedIr == null) cachedIr = store.getCachedSmallWindowIr
                windowedAgg.columnAggregators(col).update(cachedIr, row)
                cachedIrUpdated = true
              }
            }
          }
          col += 1
        }
        if (cachedIrUpdated) {
          store.putCachedSmallWindowIr(cachedIr)
          dirty = true
        }
      }
    }

    // --- Large windows: route to the per-day slot for round(eventTs, DayMillis) ---
    // Accept events as far back as the staleness bound. Events whose dayStart is already
    // covered by batch are still accepted for the incremental running IR (so onEvent emits
    // see them immediately), and recomputeRunningLargeIr will drop their daily slot at the
    // next eviction — matching the original "incremental shows it, eviction may drop it"
    // trade-off when the late event was not in the prior batch's source data.
    val eventDayStart = TsUtils.round(eventTs, DayMillis)
    val oldestAcceptedDay = currentDayStart - maxStalenessMillis
    if (eventDayStart >= oldestAcceptedDay) {
      val existing = store.getDailyLargeIr(eventDayStart)
      val dayIr = if (existing != null) existing else windowedAgg.init
      updateLargeWindowColumns(dayIr, row)
      store.putDailyLargeIr(eventDayStart, dayIr)

      val runningIr = store.getRunningLargeIr
      updateLargeWindowColumns(runningIr, row)
      store.putRunningLargeIr(runningIr)
      dirty = true
    }

    if (dirty) GigaEmitResult(packAndFinalize()) else GigaEmitResult(null)
  }

  /** Watermark-driven day transition. Updates currentDayStart and rebuilds the small-window
    * cache because each column's effectiveStart shifts with the new as-of horizon. Daily
    * large-IR slots are unaffected — they're keyed by day-start and persist across rollovers
    * until batch covers them.
    */
  def advanceWatermark(watermarkTs: Long): Unit = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return
    val wmDay = TsUtils.round(watermarkTs, DayMillis)
    if (wmDay > currentDayStart) {
      val isAdjacentRollover = wmDay == currentDayStart + DayMillis
      store.putCurrentDayStart(wmDay)
      // Rebuild only on the adjacent-day case Bug B targets. Multi-day jumps and end-of-stream
      // (watermark = MAX) push the as-of so far past retained tiles that everything would be
      // marked stale and the cache cleared — emitting a spurious null that would overwrite the
      // PUSH KV row. Leave those cases to the next event-time eviction at a sane timer ts.
      if (isAdjacentRollover) rebuildCachedSmallWindowIr(watermarkTs, wmDay)
    }
  }

  /** Periodic eviction: corrects small window sawtooth and large window tail hop selection. */
  def onEviction(timerTs: Long): GigaEmitResult = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return GigaEmitResult(null)

    rebuildCachedSmallWindowIr(timerTs, currentDayStart)
    recomputeRunningLargeIr(timerTs, currentDayStart)

    val packed = pack()
    if (lastEvictionPackedIr != null && irEqual(lastEvictionPackedIr, packed)) {
      GigaEmitResult(null)
    } else {
      lastEvictionPackedIr = windowedAgg.clone(packed)
      GigaEmitResult(windowedAgg.finalize(packed))
    }
  }

  /** Process a new batch IR from the Iceberg connected stream.
    *
    * @param newBatchIr  decoded FinalBatchIr from the Iceberg upload table
    * @param newBatchEnd batch upload boundary timestamp (midnight-aligned)
    * @param currentWatermark Flink's current watermark (for tail hop selection as queryTs)
    */
  def onBatchUpdate(newBatchIr: FinalBatchIr, newBatchEnd: Long, currentWatermark: Long): GigaEmitResult = {
    val oldBatchEnd = store.getBatchEndTs
    if (newBatchEnd <= oldBatchEnd) return GigaEmitResult(null)

    val strippedBatchIr = stripSmallWindowHops(newBatchIr)
    store.putBatchIr(strippedBatchIr)
    store.putBatchEndTs(newBatchEnd)

    var currentDayStart = store.getCurrentDayStart

    if (newBatchEnd > currentDayStart) {
      if (currentDayStart < 0) {
        // Uninitialized (no events yet). Daily slots are empty so there's no overlap risk.
        currentDayStart = newBatchEnd
        store.putCurrentDayStart(currentDayStart)
      } else {
        // Defer until watermark advances past batchEnd. Daily slots between currentDayStart
        // and batchEnd would otherwise overlap with batch and double-count.
        return GigaEmitResult(null, needsEvictionTimer = true)
      }
    }

    // Prune daily slots now covered by batch. Batch ends are midnight-aligned, so any slot
    // with dayStart strictly less than batchEndDay is fully inside batch.
    val batchEndDay = TsUtils.round(newBatchEnd, DayMillis)
    val toRemove = mutable.ArrayBuffer.empty[Long]
    val iter = store.dailyLargeIrIterator
    while (iter.hasNext) {
      val (dayStart, _) = iter.next()
      if (dayStart < batchEndDay) toRemove += dayStart
    }
    toRemove.foreach(store.removeDailyLargeIr)

    val oldRunningIr = store.getRunningLargeIr
    recomputeRunningLargeIr(currentWatermark, currentDayStart)

    val newRunningIr = store.getRunningLargeIr

    if (!irEqual(oldRunningIr, newRunningIr)) {
      GigaEmitResult(packAndFinalize(), needsEvictionTimer = true)
    } else {
      GigaEmitResult(null, needsEvictionTimer = true)
    }
  }

  // --- Private helpers ---

  /** Recompute runningLargeIr from: batch (collapsed + tail hops) + every retained daily slot
    * with dayStart >= batchEndDay. For columns where the entire window has moved past
    * batchEndTs, the collapsed value is stale — zero it out so stale batch data doesn't
    * persist for idle entities.
    */
  private def recomputeRunningLargeIr(queryTs: Long, currentDayStart: Long): Unit = {
    val batchIr = store.getBatchIr
    val batchEndTs = store.getBatchEndTs
    val runningIr = if (batchIr != null) {
      val ir = windowedAgg.clone(batchIr.collapsed)
      megaTileAgg.mergeTailHopsForBatchColumns(ir, queryTs, batchEndTs, batchIr)
      var col = 0
      while (col < windowedAgg.length) {
        val window = megaTileAgg.windowMappings(col).aggregationPart.window
        if (!isNoBatch(col) && window != null && queryTs - megaTileAgg.windowMappings(col).millis >= batchEndTs) {
          ir(col) = null
        }
        col += 1
      }
      ir
    } else {
      windowedAgg.init
    }

    val batchEndDay = if (batchEndTs > 0) TsUtils.round(batchEndTs, DayMillis) else Long.MinValue
    val iter = store.dailyLargeIrIterator
    while (iter.hasNext) {
      val (dayStart, dayIr) = iter.next()
      if (dayStart >= batchEndDay && dayIr != null) {
        var col = 0
        while (col < windowedAgg.length) {
          if (!isNoBatch(col) && dayIr(col) != null) {
            runningIr(col) = windowedAgg.columnAggregators(col).merge(runningIr(col), dayIr(col))
          }
          col += 1
        }
      }
    }

    // Some column aggregators (FIRST, LAST, and similar non-mutating reducers) return one of
    // their merge inputs by reference. Without re-cloning, runningIr columns can end up
    // aliased to cached tail-hop or daily-slot IRs; subsequent in-place onEvent updates would
    // then mutate that shared state and corrupt future serves.
    store.putRunningLargeIr(windowedAgg.clone(runningIr))
  }

  /** Rebuild cachedSmallWindowIr from retained tiles using the supplied as-of horizon.
    * Called from advanceWatermark on day rollover (effective starts shift) and from
    * onEviction (sawtooth correction at hop boundaries).
    */
  private def rebuildCachedSmallWindowIr(asOfTs: Long, currentDayStart: Long): Unit = {
    if (!hasSmallWindows) return
    val earliest = store.getEarliestTileStart
    if (earliest == Long.MaxValue) return

    // Single-pass classify-and-collect. Flink-backed TileStore decodes values during
    // iteration, so a second full scan would deserialize every retained tile again.
    val staleEntries = mutable.ArrayBuffer.empty[(Long, Long)]
    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      smallWindowTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap
    var newEarliest = Long.MaxValue
    val iter = store.tileIterator
    while (iter.hasNext) {
      val (hopSize, tileStart, ir) = iter.next()
      if (smallWindowTiers.contains(hopSize)) {
        val floor = megaTileAgg.retentionFloor(hopSize, asOfTs, currentDayStart)
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

    val rebuiltIr = megaTileAgg.buildMegaTileIr(tiles, now = asOfTs, batchEnd = currentDayStart)
    store.putCachedSmallWindowIr(rebuiltIr)
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

  private def pack(): Array[Any] = {
    val cachedIr = store.getCachedSmallWindowIr
    val runningIr = store.getRunningLargeIr
    val packed = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      packed(col) = if (isNoBatch(col)) cachedIr(col) else runningIr(col)
      col += 1
    }
    packed
  }

  private[windowing] def packAndFinalize(): Array[Any] = windowedAgg.finalize(pack())

  /** Strip tail hops that are only used by small windows to reduce state size.
    * 5-min hops for ≤12h windows are never consumed by mergeTailHopsForBatchColumns.
    */
  private[windowing] def stripSmallWindowHops(batchIr: FinalBatchIr): FinalBatchIr = {
    if (smallWindowOnlyHopIndices.isEmpty || batchIr.tailHops == null) return batchIr
    val strippedHops = batchIr.tailHops.clone()
    for (idx <- smallWindowOnlyHopIndices) {
      if (idx < strippedHops.length) {
        strippedHops(idx) = Array.empty
      }
    }
    FinalBatchIr(batchIr.collapsed, strippedHops)
  }
}

case class GigaEmitResult(
    finalizedVector: Array[Any],
    needsEvictionTimer: Boolean = false
)
