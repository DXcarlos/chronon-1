# Sub-Daily Partition Support

This document describes the changes that make Chronon's time-coverage semantics explicit,
enabling any sub-daily partition schedule that cleanly divides 24 hours, with any anchor
offset. It walks through the driving examples, maps each example to the changes it required,
and lists current limitations.

## The problem

Chronon previously had no first-class concept of time coverage. It had daily partition
strings, and many systems inferred meaning from those strings. The following concepts were
tangled together in one `ds=yyyy-MM-dd` value:

- cron fire time, processing delay, data interval
- output partition identity, input dependency range
- sensor readiness boundary, workflow idempotency key, physical table partition

That works while everything is daily, dense, and midnight-anchored. It fails once a job runs
more than once a day, an output is 3-hourly, an input has a different grain than its
consumer, or a table is clustered/timestamp-backed instead of catalog-partitioned.

## The model

| Concept | Definition |
|---|---|
| Partition label | the formatted UTC instant of its interval **start** (e.g. `2026-06-03-04-00`) |
| Partition spec | `PartitionSpec(column, format, spanMillis, offsetMillis = 0)` |
| Grid | `{ k * spanMillis + offsetMillis }`; span must cleanly divide 24h or be whole days; `0 <= offset < span` |
| Coverage | half-open `[epoch(label), epoch(label) + span)` — always **derived** from the spec, never stored |
| Data watermark | exclusive epoch upper bound of the time a table's data covers |
| Readiness | `dataWatermark(upstream) > requiredRange.coverageEnd` — compared in **time space**, never via label strings |

Daily (`1d` span, `0` offset) is the degenerate case everywhere: every formula above reduces
exactly to the previous daily behavior, which keeps existing confs byte-identical and
existing job decisions unchanged.

The single most important design rule that fell out of review: **labels are identity, time is
coverage**. Whenever the code needs to ask "is data present through X?", it converts to epoch
millis and compares watermarks. Whenever it needs to name, store, or enumerate partitions, it
uses labels in the owning table's spec. The one remaining bridge is a documented shim:
listing APIs translate labels to the global spec's format only when the table's grid matches
the global grid (the legacy `yyyyMMdd`-table case); sub-daily labels always stay in their own
spec.

## Example 1: a GroupBy that refreshes its full KV upload every 3 hours at 01:00

```python
my_group_by = GroupBy(
    sources=[...],
    keys=["user_id"],
    aggregations=[Aggregation(input_column="amount", operation=Operation.SUM, windows=["7d"])],
    online=True,
    output_partition_interval="3h",   # NEW
    output_partition_offset="1h",     # NEW: boundaries at 01:00, 04:00, ..., 22:00 UTC
)
```

What happens per 04:00 run: backfill/upload computes aggregates as-of 04:00, the upload table
gains partition `2026-06-03-01-00` (covering `[01:00, 04:00)`), `uploadToKV` bulk-loads it,
and the fetcher's streaming seam moves to 04:00 — streaming events at or after 04:00 are
merged on top of the batch values at request time.

Changes this example required, and why:

1. **`PartitionSpec.offsetMillis` + grid math** (`api/PartitionSpec.scala`). The 01:00 anchor
   means partition starts are not multiples of the span. `floor(ts) = floorDiv(ts - offset,
   span) * span + offset` defines the grid; `at(ts)` floors onto it (a no-op for daily since
   date-only formats already truncate). Construction validates: span divides 24h or is whole
   days, offset in `[0, span)`, the format round-trips two adjacent grid instants (rejects a
   3h span with a date-only format, or a 30-minute offset with an hour-resolution format),
   and labels sort lexicographically across day/month/year rollovers.
2. **`epochMillis` is strict** — an off-grid label (e.g. `02-30` or `02:30` under a 3h@01:00
   grid) throws, naming the nearest grid label. Snapping would make range iteration silently
   miss physical partitions.
3. **Python authoring + thrift carriage**: `output_partition_interval`/`output_partition_offset`
   on `GroupBy()`/`Join()`/`StagingQuery()` write `executionInfo.outputTableInfo
   .partitionInterval/partitionOffset` (`partitionOffset` is new: `Query` field 26,
   `TableInfo` field 103). `MetaDataUtils.layer` respects author-declared fields and only
   fills gaps from the global spec; the offset is serialized only when nonzero so existing
   compiled daily confs stay byte-identical.
4. **`GroupByUpload` takes the upload spec** from the node's range, so `batchEndDate =
   spec.after(endDs)` is `+3h` instead of `+1 day`, and event sources are scanned via
   coverage so daily-partitioned inputs still resolve correctly.
5. **`GroupByServingInfo.batchEndTs` (new thrift field 9)** — the authoritative watermark of
   the last upload, written as the exact epoch boundary the batch covers up to.
   `GroupByServingInfoParsed.batchEndTsMillis` prefers it and falls back to parsing
   `batchEndDate` for uploads from older versions (with the new `partitionInterval`/
   `partitionOffset` fields 7-8 informing the parse when present). The fetcher consumes
   `batchEndTsMillis` as a long, unchanged.
6. **Sawtooth aggregator fixes** (`aggregator/windowing/`). Two real bugs surfaced by testing
   a 04:00 batch end against ground truth:
   - `tailTs` was `batchEndTs - window`, not hop-aligned. Queries round their window start to
     hop boundaries, so a non-midnight batch end permanently dropped the rows between the hop
     boundary and the raw tail (a 14d window lost up to a day's tail data). `tailTs` now
     rounds down to the hop grid — a no-op for midnight batch ends.
   - `mergeTailHops` reduced hops via `bulkMerge`, which folds into its first non-null
     element. When a query had no streaming rows in a window (head IR null), the first
     element was a **shared batch tail hop**, mutated in place — corrupting the cached batch
     IR for every later query. This was a latent production cache-corruption bug for mutable
     IRs (averages, sets) independent of sub-daily; it is fixed by cloning the first non-null
     element when the head is null.

## Example 2: a Join on a 3-hourly schedule over mixed-grain upstreams

```python
my_join = Join(
    left=EventSource(table="events.checkouts", query=Query(partition_interval="3h", partition_offset="1h", ...)),
    right_parts=[JoinPart(group_by=temporal_gb), JoinPart(group_by=snapshot_gb)],
    output_partition_interval="3h",
    output_partition_offset="1h",
)
```

The join's output table is 3-hourly. Upstream sources may be 3-hourly, hourly, or daily, and
each declares its own grain on its `Query` (`partition_interval`, `partition_offset`).

Changes this example required, and why:

1. **Cross-spec range translation via coverage** (`PartitionRange.coveringRange`). The old
   `translate()` floors both endpoint labels, so translating a range *into a finer spec*
   kept only the first sub-partition of the end label's interval. `coveringRange(otherSpec)`
   produces every `otherSpec` label whose coverage intersects the range's coverage (the end
   label derives from the end of coverage). It is the primitive behind:
   - the left scan (`JoinUtils.leftDf`) — a daily left table under a 3h output range scans
     the whole covering day, not its first 3 hours;
   - GroupBy source scans (`GroupBy.getIntersectedRange`) for right-side inputs;
   - `TableUtils.unfilledRanges`, which now keeps sub-daily ranges in their own spec (the
     "canonicalize to global" step only applies to grid-matching specs) and uses
     `PartitionRange.coveredPartitions` for honest cross-grain input checks: a coarser input
     label covers all the finer output partitions inside it; a finer-grained input covers a
     coarser output partition only when **all** its covering labels exist.
2. **Per-node specs from metadata** (`MetadataOps.outputPartitionSpec` / `dateRangeSpec`).
   Batch runners and jobs (`BatchNodeRunner`, `StepRunner`, `SourceJob`, `JoinPartJob`,
   `MergeJob`, `JoinBootstrapJob`, `JoinDerivationJob`, `ModularMonolith`) interpret their
   `DateRange`/CLI labels with the node's output spec when it is sub-daily, and with the
   global spec for daily-grid nodes (whatever the table's storage format — jobs translate
   internally, as before). `--start-ds/--end-ds` are validated against the grid up front, so
   a daily-formatted argument against an hourly node fails loudly instead of being
   truncate-parsed.
3. **Dependency readiness in time space.** `DependencyResolver.computeInputRange` output is
   reduced to a required **coverage end** (epoch), and `BatchNodeRunner` compares it against
   `TableUtils.dataWatermarkMillis(table, inputSpec)`. A 3h run needing `[01:00, 04:00)` of a
   daily upstream becomes "watermark must pass 04:00", with the daily upstream's watermark
   being `epoch(maxLabel) + 1d`. No label translation, no format coupling. This also fixed a
   pre-existing bug: an hourly input translated to daily used to read as "day ready" with
   only part of the day present; the watermark version requires the full day.
   One more pre-existing hazard fixed here: **unbounded aggregation windows**
   (`Window(Int.MaxValue, DAYS)` from unwindowed aggregations) are normalized to null
   offsets ("no bound") instead of entering label arithmetic — `Int.MaxValue` days lands in
   BCE territory whose year-of-era labels (`"+5877588-..."`) sort BEFORE real dates and
   poison range minimums. The old calendar-based arithmetic produced the same garbage but
   formatted it sign-less, so it sorted larger and lost the min by accident.
4. **Snapshot parts: daily lookback with per-row PIT binding**
   (`JoinUtils.snapshotLookbackRange`). Snapshots stay at the daily grain. Computation/scan
   ranges derive from `leftRange.coveringRange(dailySpec).shift(-1)` (daily-left degenerates
   to the previous `shift(-1)` exactly), and the merge binds **per row**, not per left
   partition: the join key is `TimePartitionColumn` (the day of the row's `ts`) against
   `snapshot ds + 1`.

   **The point-in-time invariant this preserves:** a row at time T joins the latest snapshot
   whose as-of boundary is at or before T — never a future snapshot, staleness bounded by one
   snapshot interval. Snapshot *changes* are therefore respected at row granularity: a 3h left
   partition straddling midnight splits, with pre-midnight rows seeing the old snapshot
   version and post-midnight rows seeing the new one. The merge logic needed no change for
   sub-daily outputs precisely because it was already row-time based; the planned
   spec-aligned-snapshot follow-up (below) generalizes the same invariant to sub-daily
   snapshot grids and must hold it: bind to the latest snapshot with as-of ≤ row ts, on the
   snapshot table's declared grid.
5. **Validation where semantics don't generalize** (`SubDailyValidation`): ENTITIES-left
   joins (the left is itself a daily snapshot) and ENTITIES uploads are rejected at planning
   time and again at the runner entry points for non-daily output specs.

## Example 3: sensing upstreams that don't write sub-daily partitions

Upstream tables outside Zipline often land data incrementally into a single daily partition,
or have no catalog partitions at all (timestamp-backed, or clustered warehouse tables).
Decided behavior:

- **Catalog-partitioned upstream, no other signal**: partition-exists is trusted — label `D`
  present means day `D` fully covered (today's daily-over-daily semantics, kept). Tables that
  land incrementally must declare a time signal to gate intraday consumers correctly.

  The practical consequence for sub-daily pipelines is ENFORCED at three layers — at
  `zipline compile` in the author's dev loop (`conf_validator._validate_sub_daily_sources`),
  at planning time (`SubDailyValidation.assertSourcesIntradayReady`), and at the runtime
  entry points as defense in depth: every driving EVENTS source of a
  sub-daily conf must either declare a grain at least as fine as the output interval
  (`partition_interval` <= output interval; offsets don't matter, coverage math handles
  them) or be `time_partitioned` — otherwise the conf is invalid, because its schedule
  promises freshness its inputs can't deliver: every intraday run of day `D` would either
  fire on a possibly-incomplete daily partition or wait and execute as a burst after `D`
  closes, buying nothing over a daily schedule. Exempt by design: ENTITIES sources
  (dimensions/snapshots, where one-day staleness is the intended lookback semantics),
  cumulative sources (latest-partition semantics), chained JoinSources, and
  snapshot-accuracy join parts. The check also catches a quieter mistake: consuming a
  sub-daily chronon table without declaring its grain on the `Query`, which would silently
  mis-scan it as daily.
- **`time_partitioned=True` upstream**: the partition column *is* the time column; the
  watermark derives from a value scan (`MAX` of the column), so "pick up when the data lands"
  works without catalog partitions.
- **`triggerExpr`**: unchanged escape hatch for custom SQL readiness.

Changes this example required, and why:

1. **`TableUtils.dataWatermark(table, spec)`** — the one place coverage is computed: the last
   partition label (in the table's own spec) and the end of its coverage interval.
   `tableCoversRange`, sensor checks, `postJobActions`, and `StepRunner`'s trailing check all
   reduced to `watermark > range.coverageEnd`, deleting a family of cross-format label
   comparisons (this was the abstraction-leak cleanup: coverage questions moved to time
   space, and most `gridEquals` conditionals disappeared with them).

   **Cost contract — readiness checks must be metadata-fast.** Time-space comparison does not
   mean querying data: the watermark is one metadata-tier read and the millis arithmetic is
   local CPU on the returned label. The tiering, per format:
   1. **Catalog partitions** (partition listing / INFORMATION_SCHEMA / Iceberg metadata).
   2. **File statistics** where the format exposes them: Iceberg resolves boundaries from
      manifest column bounds and Delta from Delta-log JSON stats
      (`statsLastAvailablePartition` on each format), so clustered/unpartitioned tables of
      either format — where the catalog reports no partitions — still answer from metadata,
      offset-grid-floored via `spec.at` on the raw bound millis (a `date_format` here would
      emit off-grid labels for sub-daily or offset-anchored specs).
   3. **Value scan** (`SELECT MAX(col)`) strictly as the last resort, only for tables with
      neither catalog partitions nor usable stats — e.g. clustered BigQuery tables, which
      expose no boundary stats, where the scan is a single aggregation-pushdown read.

   The sensor retry loop performs exactly one such read per attempt, and per-table status
   computation reuses a single watermark read for both readiness and the diagnostic label.
   The logical-partition distinct fallback follows the same discipline: it fires only when
   the catalog listing is empty, and only compute planning needs it - readiness never lists
   partition sets.
2. **Clustered-table coverage (the join-part reuse bug)** — two distinct halves:
   - *Boundaries (watermarks)*: a clustered-but-not-partitioned table failed
     `tableCoversRange` and forced full recomputes, because `BigQueryNative.partitions` threw
     on missing `is_partitioning_column` and the generic scan fallback reads via
     `sparkSession.read.table`, which `BigQueryNative` forbids. Fixes: a missing partitioning
     column is a graceful "unpartitioned" signal, and `BigQueryNative` overrides the boundary
     scans with aggregation-pushdown BQ SQL, so `MAX(ds)` of a clustered table yields a real
     watermark for `analyzeJoinPartsForReuse`.
   - *Logical partitions (the set, not just the max)*: compute planning
     (`unfilledRanges`, step runners) set-diffs the table's partitions against the requested
     range — an empty catalog listing reads as "everything missing" and recomputes
     fully-populated tables even when the watermark says covered. When the catalog reports no
     partitions, `TableUtils.partitions` now falls back to
     `Format.scanDistinctPartitions` — the distinct values of the string partition column
     (generic Spark implementation; query-pushdown override for BigQuery) — so clustered
     tables expose their true logical partitions, holes included.
   - *Numeric time columns are epoch millis by chronon convention.* The boundary scans and
     Delta stats interpret numeric columns as millis directly — casting them through
     `TimestampType` would read them as seconds and scramble units by 1000x (the old `DATE`
     casts returned null for numerics, hiding the case entirely).
3. **`Format.virtualPartitions` generalization**: derives labels via `spec.at(min/max epoch)`
   instead of a `DateType` cast, so timestamp-backed tables enumerate correctly under
   sub-daily and offset-anchored specs.

## Schedules

`offline_schedule`/`online_schedule` remain cron-string trigger hints on
`executionInfo` — schedule is deliberately **not** part of partition semantics. The
orchestration platform maps a cron fire to the partition implied by the node's output spec;
nothing in this repo derives meaning from the cron string. (Deriving the grid from cron fire
times was prototyped and rejected: it re-entangles trigger time with data interval — a daily
job firing at 02:00 means processing *delay*, not a 02:00 anchor.)

### Schedule expectations

- **Declare cadence twice, on purpose**: the grid via
  `output_partition_interval`/`output_partition_offset`, the trigger via the cron. They are
  different concepts; keeping them separate is what lets a job fire late without shifting
  partition identity.
- **Fire at or after each boundary.** For a 3h@01:00 grid, a fire at 04:00 (or 04:15, or
  whenever upstreams typically land) computes the partition labeled `01:00` covering
  `[01:00, 04:00)`. Firing later than the boundary expresses processing delay; it never
  changes which partition is produced.
- **Over- and under-firing are both safe.** Extra fires are idempotent no-ops (the partition
  label is the idempotency unit); missed fires leave holes that the next run's range
  computation (`unfilledRanges` / `getMissingSteps`) backfills. A cron coarser than the
  interval just means stale-but-correct outputs.

### Orchestrator expectations

The platform consumes per-node metadata; everything it needs is explicit there:

- **Read the spec from `executionInfo.outputTableInfo`** (column/format/interval/offset) per
  node — never assume daily, never parse meaning out of label strings.
- **Map a cron fire to a partition** as: the latest grid label whose coverage has closed at
  fire time, i.e. `spec.before(spec.at(fireTimeMillis))`.
- **Workflow/step identity must carry the full label** in the node's output-spec format
  (`2026-06-03-04-00`, not a date). `--start-ds/--end-ds` handed to `BatchNodeRunner` must be
  in that format for sub-daily nodes (validated fail-fast); daily-grid nodes keep the global
  daily format regardless of their storage format.
- **Compare labels only within one spec.** Lexicographic ordering holds inside a spec, not
  across specs; anything cross-spec (allocators, missing-step diffs, status rollups) must go
  through coverage/time-space, the same way `BatchNodeRunner` does.
- **`stepDays` stays day-denominated** in ExecutionInfo; `getMissingSteps`/`stepsByDays`
  convert to partition counts, so existing slicing logic carries over unchanged.
- **Readiness should defer to the sensor nodes** (watermark vs required coverage end, with
  the metadata-first/stats/scan tiering). A platform re-implementing readiness must follow
  the same rule — `dataWatermark > requiredRange.coverageEnd` — and must not reintroduce
  label-string comparisons across grains or formats.
- **Cross-grain dependencies are already resolved in the metadata**: `TableDependency`
  offsets are `Window`s, input ranges come from `DependencyResolver.computeInputRange`, and
  coarser/finer coverage conversion is `coveringRange`/`coveredPartitions` — the platform
  should not invent its own grain math.

## Backward compatibility

- Daily confs compile byte-identically: new thrift fields are optional and never set for
  daily; planners emit `partitionOffset` only when nonzero; `WindowUtils.fromMillis` keeps
  emitting `Window(24, HOURS)` for whole days, matching the historical serialization.
- Daily job decisions are equivalence-proven: `watermark > coverageEnd` reduces to the old
  `maxPartition >= range.end` for same-spec comparisons; `stepsByDays(n) == steps(n)`;
  snapshot lookback degenerates to `shift(-1)`; macro rendering (`{{ start_date }}`,
  `offset=`, bounds, `{{ max_date }}`) is exercised by exact-SQL regression tests.
- `GroupByServingInfo` from older uploads (no `batchEndTs`) still parses via
  `batchEndDate` + `dateFormat`.
- Python defaults unchanged: `TableDependency.to_thrift` still emits `Window(1, DAYS)` when
  no interval is given.

Release notes for non-default deployments:

- `PartitionSpec` construction now validates formats. A conf carrying a non-sortable format
  (e.g. `MM-dd-yyyy`) fails at startup instead of producing wrong range arithmetic. Common
  formats (`yyyy-MM-dd`, `yyyyMMdd`, `yyyy-MM-dd-HH`) pass.
- Deployments that override `spark.chronon.partition.format` globally to something other
  than `yyyy-MM-dd`: `--start-ds/--end-ds` and thrift `DateRange` labels are now interpreted
  in the global spec's format for daily nodes (previously a hardcoded `yyyy-MM-dd` parse and
  a forced daily translation in `toDateRange`). Mixed-version rollouts in such deployments
  should pin label formats during the transition.

## Limitations

1. **Snapshot grain is daily for now; spec-aligned snapshots are the designed follow-up.**
   Snapshot-accuracy join parts under sub-daily outputs use the daily-lookback binding above.
   The follow-up aligns snapshot grain with the RHS table's declared
   `partition_interval`/`partition_offset` (falling back to daily when only daily is
   declared — which is just the default spec, not a special case):
   - thread the RHS source's spec into the `snapshotLookbackRange` call sites and part-table
     specs (the helper is already spec-parameterized);
   - generalize the two merge binding expressions (left `TimePartitionColumn`, right
     `ds + 1`) from date functions to grid-floor arithmetic — the concentrated risk, since a
     timezone or boundary slip mis-binds rows silently; needs adversarial tests on boundary
     rows, the midnight-straddling partition, and non-UTC sessions, holding the PIT
     invariant: latest snapshot with as-of ≤ row ts;
   - audit `snapshotEntities`/`snapshotEvents` windowed aggregation for residual daily
     assumptions;
   - relax `SubDailyValidation` from model-based rejection to binding-well-definedness.
2. **ENTITIES-left joins and ENTITIES uploads stay daily** (rejected loudly otherwise) — and
   deliberately so. Aligning entity GroupBy partitions to sub-daily intervals is the wrong
   trade even where it's technically possible: every entity snapshot partition is a **full
   copy of dimensional state**, so a 3h grid multiplies storage and scan cost by 8x per day
   while each windowed aggregation over snapshots reads 8x the partitions — for features that
   mostly didn't change between snapshots. The freshness that sub-daily entity snapshots
   would buy is what TEMPORAL accuracy already provides at row granularity via the mutation
   stream, without re-materializing the dimension table eight times a day. Rule of thumb: if
   intraday entity state matters, declare mutations and use temporal accuracy; snapshots
   remain the daily, storage-proportional representation.
3. **Span must cleanly divide 24h** (or be whole days) and span/offset must be whole minutes.
   Irregular cadences (90m works; 7h does not) and DST-aware local-time grids are out of
   scope — everything is UTC.
4. **Partition-exists is trusted for catalog-partitioned upstreams.** A daily partition that
   receives intraday appends will gate a sub-daily consumer as soon as the label appears;
   declare `time_partitioned=True` (or a `triggerExpr`) on such sources to gate on the data
   itself.
5. **`Driver.scala` ad-hoc subcommands remain daily**; `BatchNodeRunner` is the supported
   path for sub-daily nodes. `Query.subPartitionsToWaitFor` remains unimplemented at runtime
   (pre-existing).
6. **Team-level default intervals are deferred** — sub-daily is declared per conf via
   `output_partition_interval`/`output_partition_offset` (outputs) and
   `partition_interval`/`partition_offset` on `Query`/`TableDependency` (inputs).
7. **Label format hygiene matters**: prefer dash-separated formats (`yyyy-MM-dd-HH`,
   `yyyy-MM-dd-HH-mm`). Spaces/colons work in SQL but URL-escape in object-store paths
   (construction warns); quotes are rejected outright.
8. **Orchestration-platform work is out of scope here**: cron-fire-to-partition mapping,
   sub-daily workflow identity, and allocator changes live outside this repo. This repo emits
   correct per-node metadata (specs, dependencies, offsets) for the platform to consume.
