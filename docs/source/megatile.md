# Mega Tile Design

## Problem

Current tiling writes many small hop-aligned tiles per entity (up to 288/day at 5-min resolution).
The serving side fetches all tiles via prefix scan and merges them per-window. This causes read
amplification and serving latency.

## Solution

Write **one mega tile per entity per day** to the KV store. The mega tile contains a **windowed IR**
where each column has the correct aggregate for its specific window. This reduces KV store reads from
O(N tiles) to 3 point gets: `(entity, today)` + `(entity, yesterday)` from stream KV + `(entity)` from batch KV.

## Opt-in

Enabled per GroupBy via the `OnlineStrategy` thrift enum:

```thrift
enum OnlineStrategy { DEFAULT = 0, STREAMING_MEGATILES = 1 }
struct GroupBy { ..., 8: optional OnlineStrategy onlineStrategy }
```

Both Flink and fetcher check `groupByOps.isMegaTilingEnabled`. The field is excluded from
`semanticHash` — changing the online strategy doesn't trigger batch recomputation.

## Window Categories

```
tailBuffer = 2d (default)

Small windows (≤ tailBuffer):
  effectiveStart = now - window.millis
  Flink covers full window via tiles + sawtooth running IR.
  Fetcher uses today's mega tile entry directly (self-contained).

Large windows (> tailBuffer) + unwindowed:
  effectiveStart = batchEnd (on fetcher side) or dayStart (on Flink side, per-day)
  Flink accumulates a daily running IR per day (today + yesterday).
  Fetcher merges batch collapsed + tail hops + streaming daily aggregates.
```

## Tier Assignment

Each column's hop size comes from `FiveMinuteResolution.calculateTailHop(window)`:

| window range | hop size |
|-------------|----------|
| < 12h | 5min (300,000 ms) |
| >= 12h, < 12d | 1hr (3,600,000 ms) |
| >= 12d | 1day (86,400,000 ms) |

Tiles are only maintained for **small-window tiers** — tiers that have at least one column
with `isNoBatch=true`. Large window columns don't use tiles; they use daily accumulators directly.

## Flink State Layout (MegaTileStreamProcessor + TileStore)

All state access goes through a `TileStore` abstraction. Flink backs it with `MapState`/`ValueState`
+ codec (serde on access, with decode memoization). Tests back it with `InMemoryTileStore`.

```
Small window state:
  tiles: MapState<"hopSize:tileStart", bytes>    // base (unwindowed) IRs, small-window tiers only
  cachedSmallWindowIr: ValueState<bytes>          // sawtooth running sum (windowed IR)

Large window state:
  largeTodayIr: ValueState<bytes>                 // daily accumulator for [todayStart, now)
  largeYesterdayIr: ValueState<bytes>             // daily accumulator for [yesterdayStart, todayStart)

Bookkeeping:
  currentDayStart: ValueState<Long>
  earliestTileStart: ValueState<Long>
```

### On Event

1. **Update tiles** (small-window tiers only):
   - Compute `tileStartsForEvent(eventTs)` → one tile per active tier
   - Guard: only create tiles within `[retentionFloor, currentDayStart + 2*DayMillis)`
     (rejects both too-old and too-future timestamps)
   - Read tile from store, update with `baseAggregator.update(ir, row)`, write back
   - ~2 tile codec ops per event (one per small-window tier)

2. **Update cachedSmallWindowIr** (sawtooth):
   - Merge event into running IR for all small-window columns (unconditional)
   - Over-inclusive at the tail — eviction corrects this
   - 1 windowed IR codec op

3. **Update large window daily IR** — route by event day:
   - `eventTs >= todayStart` → update `largeTodayIr`
   - `eventTs >= yesterdayStart` → update `largeYesterdayIr` (late event, within 2d tolerance)
   - `eventTs >= nextDayStart` → clamp to today (future event; day transitions are watermark-driven)
   - `eventTs < yesterdayStart` → drop for large windows (> 2d late)
   - 1 windowed IR codec op

4. **Emit** to KV store (only dirty targets):
   - Today's entry: pack `cachedSmallWindowIr` + `largeTodayIr` → `(entity, todayStart)`
   - Yesterday's entry (only if late event touched it): pack `null` + `largeYesterdayIr` → `(entity, yesterdayStart)`

**Total hot-path cost per event: ~6 codec ops** (vs ~102 with bulk restore/persist).

### Day Transitions (advanceWatermark)

Day transitions are **watermark-driven only** — never triggered by event timestamps. This prevents
future-timestamped events from prematurely rotating state.

```
wmDay = round(watermarkTs, DayMillis)
if wmDay > currentDayStart:
  // Single-day hop: carry today's aggregate to yesterday
  // Multi-day hop (e.g., after long idle): clear yesterday (stale beyond 2d tolerance)
  largeYesterdayIr = if (wmDay == currentDayStart + DayMillis) largeTodayIr else init
  largeTodayIr = init
  currentDayStart = wmDay
```

### Eviction (onEviction, fires every minSmallWindowTileSize)

1. Compute `retentionFloor` per small-window tier
2. Remove tiles below floor
3. **Rebuild `cachedSmallWindowIr`** from remaining tiles via `buildMegaTileIr(tiles, now, todayStart)`
   — corrects the sawtooth tail by scoping each column to its `effectiveStart`
4. Emit updated today entry

Eviction is the only O(tiles) operation and runs at timer cadence (every 5min or 1hr), not per event.

## Mega Tile Construction (buildMegaTileIr)

Used by eviction rebuild and by the MegaTileMergerTest simulation:

```
megaTileIr = windowedAggregator.init

for col in 0 until windowedAggregator.length:
  hopSize = calculateTailHop(windowMappings(col).window)
  effStart = effectiveStart(col, now, batchEnd)
  bucketIdx = baseIrIndices(col)

  for (tileStart, tileIr) in tiles[hopSize]:
    if tileStart >= effStart and tileStart < now:
      megaTileIr(col) = merge(megaTileIr(col), tileIr(bucketIdx))
```

Multiple windowed columns (e.g., `sum_6h`, `sum_2d`) that share the same bucket (`sum`)
read from different tiers and different time ranges but the same bucket index in the tile IR.

## Fetcher Merge (MegaTileMerger)

Fetcher reads 3 entries: `(entity, today)`, `(entity, yesterday)` from stream KV + `(entity)` from batch KV.

```
def merge(batchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd):
  resultIr = clone(batchIr.collapsed) or init

  for col in 0 until windowedAggregator.length:
    window = windowMappings(col).window

    if window != null and window.millis <= tailBufferMillis:   // SMALL WINDOW
      // Self-contained in daily entry. Fall back to yesterday only if
      // today's entire entry is absent (null array), not if column is null.
      resultIr(col) = if todayIr != null then todayIr(col)
                       else if yesterdayIr != null then yesterdayIr(col)

    else:                                                       // LARGE WINDOW / UNWINDOWED
      // Batch collapsed + streaming daily aggregates
      if todayIr(col) != null: resultIr(col) = merge(resultIr(col), todayIr(col))
      if batchEnd < todayStart and yesterdayIr(col) != null:
        resultIr(col) = merge(resultIr(col), yesterdayIr(col))

  // Tail hops for large windowed columns only (not small, not unwindowed)
  mergeTailHopsForBatchColumns(resultIr, queryTs, batchEnd, batchIr)

  return windowedAggregator.finalize(resultIr)
```

## Daily KV Key Scheme

KV key: `TileKey(streamingDataset, entityKeyBytes, DayMillis, dayStart)` — reuses existing TileKey
with `tileSizeMs = DayMillis`. Each day is a separate point-get key.

- Flink emits `todayStart` (not raw `eventTs`) as the tile timestamp, so the codec always writes
  to the correct daily key even for future-timestamped events.
- Fetcher constructs two explicit `GetRequest`s for today and yesterday using
  `MegaTileMerger.streamingDayKeys(queryTs)`. Query time is resolved once and propagated to
  avoid midnight-boundary inconsistency.

## Codec (MegaTileCodec)

Windowed IR (mega tile entries): `encode(ir)` / `decode(bytes)` using the windowed aggregator schema
(one IR slot per (agg, window) pair).

Base IR (individual tiles in Flink state): `encodeBaseIr(ir)` / `decodeBaseIr(bytes)` using the
unwindowed base aggregator schema (one IR slot per aggregation bucket).

AvroCodec instances are cached as `@transient lazy val` to avoid schema parsing per decode.

## Constraints

- **Max batch staleness**: 2 days. Beyond that, large windows have a coverage gap between
  batchEnd and yesterdayStart. Alert if batch is > 2 days stale.
- **Sawtooth approximation**: Accepted for all aggregation types. Between evictions, small-window
  columns are over-inclusive by up to one tile interval at the tail.
- **Late events**: Up to 2 days late are handled (routed to yesterday's large-window IR).
  Events > 2 days late are dropped for large windows (tiles may still capture them for small windows
  if within retention).
- **Future events**: Clamped to today for large windows. Day transitions are watermark-driven,
  so future timestamps cannot corrupt state.

## Scenario Tables

All windows, tailBuffer = 2d, batchEnd = Mar 25 00:00.

### Scenario 1: Batch fresh (now = Mar 25 14:00, batchEnd = Mar 25 00:00)

| window | tier | category | effectiveStart | today entry range | batch tail hops | batch collapsed | fetcher uses |
|--------|------|----------|---------------|-------------------|-----------------|-----------------|-------------|
| 6h | 5min | SMALL | Mar 25 08:00 | [08:00, 14:00) = 6h | — | — | today only |
| 1d | 1hr | SMALL | Mar 24 14:00 | [Mar 24 14:00, Mar 25 14:00) = 24h | — | — | today only |
| 47h | 1hr | SMALL | Mar 23 15:00 | [Mar 23 15:00, Mar 25 14:00) = 47h | — | — | today only |
| 2d | 1hr | SMALL | Mar 23 14:00 | [Mar 23 14:00, Mar 25 14:00) = 48h | — | — | today only |
| 49h | 1hr | LARGE | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 22 23:00, Mar 24 23:00) = 2d | [Mar 24 23:00, Mar 25 00:00) = 1h | today + collapsed + tail |
| 3d | 1hr | LARGE | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 22 00:00, Mar 24 00:00) = 2d | [Mar 24 00:00, Mar 25 00:00) = 1d | today + collapsed + tail |
| 7d | 1hr | LARGE | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 18 00:00, Mar 20 00:00) = 2d | [Mar 20 00:00, Mar 25 00:00) = 5d | today + collapsed + tail |

### Scenario 2: Batch delayed (now = Mar 25 02:00, batchEnd = Mar 24 00:00)

| window | tier | category | effectiveStart | today entry range | yesterday entry range | fetcher uses |
|--------|------|----------|---------------|-------------------|-----------------------|-------------|
| 6h | 5min | SMALL | Mar 24 20:00 | [20:00, 02:00) = 6h | (ignored) | today only |
| 1d | 1hr | SMALL | Mar 24 02:00 | [Mar 24 02:00, Mar 25 02:00) = 24h | (ignored) | today only |
| 49h | 1hr | LARGE | — | [Mar 25 00:00, Mar 25 02:00) = 2h | [Mar 24 00:00, Mar 25 00:00) = 24h | yesterday + today + collapsed + tail |
| 3d | 1hr | LARGE | — | [Mar 25 00:00, Mar 25 02:00) = 2h | [Mar 24 00:00, Mar 25 00:00) = 24h | yesterday + today + collapsed + tail |

For large windows with stale batch (`batchEnd < todayStart`): fetcher sums yesterday + today
to cover `[batchEnd, now)`, then merges with batch collapsed + tail hops.

### Flink state size (max tiles per tier)

| tier | driven by | max tiles |
|------|-----------|-----------|
| 5min | 6h window (if present) | 72 |
| 1hr | 2d window (max small) | 48 |

The 2d SMALL column always drives hourly tier state since `now - 2d` extends further back
than `todayStart`.

## Architecture

```
Flink write path:
  source → spark eval → watermarks → keyBy
    → MegaTileProcessFunction (delegates to MegaTileStreamProcessor via TileStore)
    → MegaTileAvroCodecFn (TileKey with DayMillis + dayStart)
    → AsyncKVStoreWriter

Fetcher read path:
  GroupByFetcher: 2 point-get requests (today + yesterday)
    → GroupByResponseHandler.mergeMegaTilesFromStreaming
    → MegaTileCodec.decode (today + yesterday entries)
    → MegaTileMerger.merge (batch + today + yesterday → finalized result)

Shared pipeline tails (BaseFlinkJob):
  buildTiledTail: keyBy → window aggregate → TiledAvroCodecFn → KV write
  buildMegaTiledTail: keyBy → MegaTileProcessFunction → MegaTileAvroCodecFn → KV write
  Both FlinkGroupByStreamingJob and ChainedGroupByJob use these.
```

## Test Coverage

Tests at three layers, all comparing against NaiveAggregator:

1. **MegaTileAggregatorTest** — tile building + serveMegaTile merge (6 tests)
2. **MegaTileMergerTest** — per-day entry split + MegaTileMerger.merge (5 tests)
3. **MegaTileStreamProcessorTest** — full Flink simulation with sawtooth + eviction (6 tests,
   including multi-day watermark gap)
4. **MegaTileCodecRoundTripTest** — serde round-trips via SerdeTileStore + key switch (3 tests)

Windows tested: 6h, 1d, 47h, 2d, 49h, 3d, 7d.
Aggregation types: SUM, COUNT, AVERAGE, MIN, MAX, LAST, FIRST.
