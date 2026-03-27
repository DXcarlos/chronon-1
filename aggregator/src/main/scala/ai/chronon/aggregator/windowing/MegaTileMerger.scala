package ai.chronon.aggregator.windowing

import ai.chronon.api.TsUtils

/**
  * Fetcher-side merge logic for the per-day MegaTile KV scheme.
  *
  * Flink writes at most 2 entries per entity per day: (key, today_midnight) and (key, yesterday_midnight).
  * The fetcher reads both entries plus the batch IR and this class combines them into a finalized result.
  *
  * Per-column merge semantics:
  *   - Small windows (≤ tailBuffer): self-contained in daily entry. Pick today, fall back to yesterday.
  *   - Large windows (> tailBuffer): batch collapsed + streaming daily aggregates + tail hops.
  *   - Unwindowed: batch collapsed + streaming daily aggregates (no tail hops).
  */
class MegaTileMerger(megaTileAgg: MegaTileAggregator) {

  private val windowedAggregator = megaTileAgg.windowedAggregator
  private val isNoBatch = megaTileAgg.isNoBatch

  val DayMillis: Long = 24 * 3600 * 1000L

  /** Returns (todayStart, yesterdayStart) aligned to day boundaries. */
  def streamingDayKeys(now: Long): (Long, Long) = {
    val todayStart = TsUtils.round(now, DayMillis)
    (todayStart, todayStart - DayMillis)
  }

  /**
    * Merge batch IR + up to 2 daily streaming entries into a finalized result.
    *
    * @param batchIr     Finalized batch IR (collapsed + tail hops). May be null.
    * @param todayIr     Today's daily entry from stream KV. May be null.
    * @param yesterdayIr Yesterday's daily entry from stream KV. May be null.
    * @param todayStart  Midnight timestamp for today (the KV key timestamp).
    * @param queryTs     The query timestamp (wall-clock now).
    * @param batchEnd    The batch upload boundary timestamp.
    * @return Finalized feature values.
    */
  def merge(batchIr: FinalBatchIr,
            todayIr: Array[Any],
            yesterdayIr: Array[Any],
            todayStart: Long,
            queryTs: Long,
            batchEnd: Long): Array[Any] = {

    val resultIr = if (batchIr != null) windowedAggregator.clone(batchIr.collapsed)
                   else windowedAggregator.init

    var col = 0
    while (col < windowedAggregator.length) {
      if (isNoBatch(col)) {
        // Small window: self-contained in daily entry, ignore batch.
        // Fall back to yesterday only if today's ENTRY is missing (null array),
        // not if today exists but the column value is null (no events in window).
        if (todayIr != null) {
          resultIr(col) = todayIr(col)
        } else if (yesterdayIr != null) {
          resultIr(col) = yesterdayIr(col)
        }
      } else {
        // Large window / unwindowed: batch collapsed + streaming daily aggregates
        // resultIr(col) already has batchIr.collapsed(col) from clone
        if (todayIr != null && todayIr(col) != null) {
          resultIr(col) = windowedAggregator.columnAggregators(col).merge(resultIr(col), todayIr(col))
        }
        if (batchEnd < todayStart && yesterdayIr != null && yesterdayIr(col) != null) {
          resultIr(col) = windowedAggregator.columnAggregators(col).merge(resultIr(col), yesterdayIr(col))
        }
      }
      col += 1
    }

    // Tail hops for large windowed columns (skips small + unwindowed)
    if (batchIr != null) {
      megaTileAgg.mergeTailHopsForBatchColumns(resultIr, queryTs, batchEnd, batchIr)
    }

    windowedAggregator.finalize(resultIr)
  }
}
