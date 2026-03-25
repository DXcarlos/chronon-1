# Mega Tile Design

## Problem

Current tiling writes many small hop-aligned tiles per entity (up to 288/day at 5-min resolution).
The serving side fetches all tiles via prefix scan and merges them per-window. This causes read
amplification and serving latency.

## Solution

Write **one mega tile per entity per day** to the KV store. The mega tile contains a **windowed IR**
where each column has the correct aggregate for its specific window. This reduces KV store reads from
O(N tiles) to 2 point gets: `(entity, today)` + `(entity, yesterday)`.

## Window Categories

```
tailBuffer = 2d (default)

For each windowed column:
  collapsedRange = max(0, window.millis - tailBufferMillis)

  if window.millis <= tailBufferMillis:     → NO BATCH
      effectiveStart = now - window.millis
      Flink covers full window. Serving uses mega tile directly.

  else:                                     → BATCH
      effectiveStart = batchEnd
      Mega tile covers [batchEnd, now). Serving merges with batch collapsed + tail hops.
```

## Tier Assignment

Each column's hop size comes from `FiveMinuteResolution.calculateTailHop(window)`:

| window range | hop size |
|-------------|----------|
| < 12h | 5min (300,000 ms) |
| >= 12h, < 12d | 1hr (3,600,000 ms) |
| >= 12d | 1day (86,400,000 ms) |

## Small Tile Bucketing

On each event at time T, for each active tier:
```
tileStart = TsUtils.round(T, hopSize)
ir = tiles[hopSize].getOrInit(tileStart, baseAggregator.init)
baseAggregator.update(ir, row)
```

Each event updates one tile per active tier. The `baseAggregator` is unwindowed
(one IR slot per aggregation bucket, not per window).

## Eviction

Per tier, the retention floor is the minimum effectiveStart across all columns at that tier:
```
for each tier (hopSize):
  columnsAtTier = columns where calculateTailHop(window) == hopSize
  retentionFloor = min over columnsAtTier of:
    if NO BATCH:  now - window.millis
    if BATCH:     batchEnd
  evictionCutoff = retentionFloor - hopSize   // one buffer tile
  remove all tiles in tier where tileStart < evictionCutoff
```

Eviction fires every `min_tile_size` interval (event-time timer in Flink). When tiles are evicted,
the mega tile is rebuilt from remaining tiles and written to KV store. This keeps the mega tile
fresh even for idle entities (timers fire from global watermark advancement).

## Mega Tile Construction (Windowed IR)

```
megaTileIr = windowedAggregator.init    // Array of length = #windowed columns

for col in 0 until windowedAggregator.length:
  hopSize = calculateTailHop(windowMappings(col).window)
  effStart = effectiveStart(col, now, batchEnd)
  bucketIdx = baseIrIndices(col)    // maps windowed col → unwindowed bucket

  for (tileStart, tileIr) in tiles[hopSize]:
    if tileStart >= effStart and tileStart < now:
      megaTileIr(col) = windowedAggregator(col).merge(megaTileIr(col), tileIr(bucketIdx))
```

Multiple windowed columns (e.g., sum_6h, sum_2d) that share the same bucket (sum)
read from different tiers and different time ranges but the same bucket index in the tile IR.

## Serving Merge

```
def serveMegaTile(batchIr, megaTileIr, queryTs):
  resultIr = windowedAggregator.init

  for col in 0 until windowedAggregator.length:
    window = windowMappings(col).aggregationPart.window

    if window == null:                             // unwindowed
      resultIr(col) = merge(batchIr.collapsed(col), megaTileIr(col))

    elif window.millis <= tailBufferMillis:         // NO BATCH
      resultIr(col) = megaTileIr(col)

    else:                                           // BATCH
      resultIr(col) = merge(batchIr.collapsed(col), megaTileIr(col))

  // Tail hops for BATCH columns (window > tailBuffer)
  mergeTailHops(resultIr, queryTs, batchEnd, batchIr)

  return windowedAggregator.finalize(resultIr)
```

## Daily KV Key Scheme

In production (Flink/Fetcher), the mega tile is stored as `(entity, day)`:
- Flink writes to `(entity, today)` on each event and eviction
- At midnight, starts writing to `(entity, new_day)` — ≤2d columns immediately correct
  because Flink state has small tiles spanning midnight
- Fetcher always reads `(entity, today)` + `(entity, yesterday)`
- For ≤2d columns: use today's windowed value (kept fresh by eviction timer)
- For >2d columns: merge daily aggregates from `[batchEnd, now)` + batch

## Scenario Tables

All windows, tailBuffer = 2d, batchEnd = Mar 25 00:00.

### Scenario 1: Batch fresh (now = Mar 25 14:00, batchEnd = Mar 25 00:00)

| window | tier | category | effectiveStart | mega tile range | batch tail hops | batch collapsed | fetcher uses |
|--------|------|----------|---------------|-----------------|-----------------|-----------------|-------------|
| 6h | 5min | NO BATCH | Mar 25 08:00 | [08:00, 14:00) = 6h | — | — | mega tile only |
| 1d | 1hr | NO BATCH | Mar 24 14:00 | [Mar 24 14:00, Mar 25 14:00) = 24h | — | — | mega tile only |
| 47h | 1hr | NO BATCH | Mar 23 15:00 | [Mar 23 15:00, Mar 25 14:00) = 47h | — | — | mega tile only |
| 2d | 1hr | NO BATCH | Mar 23 14:00 | [Mar 23 14:00, Mar 25 14:00) = 48h | — | — | mega tile only |
| 49h | 1hr | BATCH | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 22 23:00, Mar 24 23:00) = 2d | [Mar 24 23:00, Mar 25 00:00) = 1h | mega + collapsed + tail |
| 3d | 1hr | BATCH | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 22 00:00, Mar 24 00:00) = 2d | [Mar 24 00:00, Mar 25 00:00) = 1d | mega + collapsed + tail |
| 7d | 1hr | BATCH | Mar 25 00:00 | [Mar 25 00:00, Mar 25 14:00) = 14h | [Mar 18 00:00, Mar 20 00:00) = 2d | [Mar 20 00:00, Mar 25 00:00) = 5d | mega + collapsed + tail |

### Scenario 2: Batch delayed (now = Mar 25 02:00, batchEnd = Mar 24 00:00)

| window | tier | category | effectiveStart | mega tile range | batch tail hops | batch collapsed | fetcher uses |
|--------|------|----------|---------------|-----------------|-----------------|-----------------|-------------|
| 6h | 5min | NO BATCH | Mar 24 20:00 | [20:00, 02:00) = 6h | — | — | mega tile only |
| 1d | 1hr | NO BATCH | Mar 24 02:00 | [Mar 24 02:00, Mar 25 02:00) = 24h | — | — | mega tile only |
| 47h | 1hr | NO BATCH | Mar 23 03:00 | [Mar 23 03:00, Mar 25 02:00) = 47h | — | — | mega tile only |
| 2d | 1hr | NO BATCH | Mar 23 02:00 | [Mar 23 02:00, Mar 25 02:00) = 48h | — | — | mega tile only |
| 49h | 1hr | BATCH | Mar 24 00:00 | [Mar 24 00:00, Mar 25 02:00) = 26h | [Mar 21 23:00, Mar 23 23:00) = 2d | [Mar 23 23:00, Mar 24 00:00) = 1h | mega + collapsed + tail |
| 3d | 1hr | BATCH | Mar 24 00:00 | [Mar 24 00:00, Mar 25 02:00) = 26h | [Mar 21 00:00, Mar 23 00:00) = 2d | [Mar 23 00:00, Mar 24 00:00) = 1d | mega + collapsed + tail |
| 7d | 1hr | BATCH | Mar 24 00:00 | [Mar 24 00:00, Mar 25 02:00) = 26h | [Mar 17 00:00, Mar 19 00:00) = 2d | [Mar 19 00:00, Mar 24 00:00) = 5d | mega + collapsed + tail |

### Hourly tier state (max tiles across columns at that tier)

| scenario | 1d | 47h | 2d | 49h | 3d | 7d | **max** |
|----------|----|-----|----|----|----|----|---------|
| Batch fresh | 24 | 47 | 48 | 14 | 14 | 14 | **48** (2d) |
| Batch delayed | 24 | 47 | 48 | 26 | 26 | 26 | **48** (2d) |

The 2d NO BATCH column always drives hourly tier state since `now - 2d` extends further back
than `batchEnd`.
