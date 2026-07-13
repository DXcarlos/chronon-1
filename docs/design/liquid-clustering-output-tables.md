# RFC: Liquid Clustering Support for Chronon Output Tables

## Summary

Enable Chronon to materialize output tables (GroupBy, Join, StagingQuery) using **liquid clustering** (`CLUSTER BY`) instead of traditional Hive-style partitioning (`PARTITIONED BY`). The feature is opt-in and format-aware since only Delta Lake (≥3.1 / Databricks 13.3+) and Snowflake currently support liquid clustering.

---

## Motivation

### Problem

Today, Chronon output tables are **always** created with `PARTITIONED BY (ds STRING)` via `CreationUtils.createTableSql()`. This works well for Hive/Parquet but is suboptimal for modern lakehouse engines:

1. **Delta Lake with liquid clustering** provides better data layout adaptability — the engine re-clusters files automatically based on query patterns, eliminating the need to choose a fixed partition scheme upfront.
2. **Snowflake** uses clustering keys natively (`CLUSTER BY (ds)`) and does not support Hive-style partitions at all.
3. Tables with liquid clustering avoid small-file problems common with daily partitioning on small datasets.
4. Liquid clustered tables can efficiently skip data based on multiple columns simultaneously (e.g., `ds` + `key`), improving read performance for downstream consumers.

### Current Behavior

```sql
-- What Chronon generates today (CreationUtils.createTableSql):
CREATE TABLE IF NOT EXISTS catalog.schema.my_output (
    feature_col1 BIGINT,
    feature_col2 STRING
)
USING delta
PARTITIONED BY (
    ds STRING
)
```

### Desired Behavior

```sql
-- With liquid clustering enabled:
CREATE TABLE IF NOT EXISTS catalog.schema.my_output (
    feature_col1 BIGINT,
    feature_col2 STRING,
    ds STRING
)
USING delta
CLUSTER BY (ds)
```

---

## Current Architecture (Relevant Components)

### Write Path

```
DataFrame.save()                            [Extensions.scala:146]
  → TableUtils.insertPartitions()           [TableUtils.scala:294]
    → Format.createTable()                  [Format.scala:33]
      → CreationUtils.createTableSql()      [CreationUtils.scala:19]
        → "PARTITIONED BY (...)"
    → DataFrame.write.mode(Overwrite).insertInto(tableName)
```

### Existing Unpartitioned Table Handling (Iceberg)

`TableUtils.insertPartitions()` (line 359) already handles unpartitioned tables specially:

```scala
val isIceberg = tableFormatProvider.readFormat(tableName).contains(Iceberg)
val hasPartitionSpec = isIceberg && Try(Iceberg.partitionColumnNames(tableName)(sparkSession).nonEmpty).getOrElse(false)
if (isIceberg && partitionColumns.nonEmpty && !hasPartitionSpec) {
  // Uses MERGE INTO with ON FALSE for atomic delete+insert
}
```

This MERGE INTO path works for Iceberg (which lacks `replaceWhere` support), but is **not the optimal strategy for Delta Lake**. For clustered Delta tables, `replaceWhere` is preferred — see [Write Strategy Analysis](#write-strategy-analysis) below.

### Configuration Model

| Level | Parameter | Currently Used For |
|-------|-----------|-------------------|
| Object-level | `table_properties: Dict[str, str]` | Custom TBLPROPERTIES |
| Object-level | `partition_interval` / `partition_offset` | Output grid |
| Team-level | `Team.tableProperties` | Team-wide table properties |
| Team-level (conf) | `spark.chronon.table_write.format` | Format selection (delta/iceberg/hive) |
| Team-level (conf) | `spark.chronon.partition.column` | Partition column name |

### Format Implementations

| Format | File | `tableTypeString` | Supports LC? |
|--------|------|-------------------|--------------|
| `DeltaLake` | `DeltaLake.scala` | `"delta"` | ✅ Delta 3.1+ |
| `DatabricksDeltaLake` | `DatabricksDeltaLake.scala` | `"delta"` | ✅ DBR 13.3+ |
| `Iceberg` | `Iceberg.scala` | `"iceberg"` | ❌ (no equivalent) |
| `Hive` | `Hive.scala` | `""` | ❌ |
| `Snowflake` | `Snowflake.scala` | N/A (external) | ✅ native |

---

## Write Strategy Analysis

When writing to a clustered (non-partitioned) Delta table, we need to **replace** data for specific partition-column values (e.g., `ds = '2024-07-11'`) without affecting other data. Four strategies were evaluated:

| Strategy | Atomicity | Performance | OSS Delta | Databricks |
|----------|-----------|-------------|-----------|------------|
| MERGE INTO ON FALSE | ✅ Atomic | ❌ Expensive (evaluates full target for NOT MATCHED BY SOURCE) | ✅ | ✅ |
| DELETE + INSERT | ❌ Two commits | ✅ Efficient | ✅ | ✅ |
| **`replaceWhere`** | ✅ Atomic | ✅ Best (data-skipping on predicate, no row-level join) | ✅ (OSS Delta 1.0+) | ✅ |
| `REPLACE USING` | ✅ Atomic | ✅ Efficient | ❌ DBR 16.3+ only | ✅ |

### Decision: `replaceWhere` for Delta, MERGE INTO for Iceberg

- **Delta Lake (clustered)**: Use `replaceWhere`. It is atomic (single commit), leverages Delta's data-skipping stats to identify affected files without a full table scan, and is supported on both OSS Delta and Databricks.
- **Iceberg (unpartitioned)**: Keep the existing MERGE INTO with ON FALSE path (Iceberg does not support `replaceWhere`).

### How `replaceWhere` works on clustered tables

```scala
// Delta identifies files whose min/max stats overlap the predicate,
// marks them as removed, and writes new files — all in one commit.
// IMPORTANT: replaceWhere MUST be used with saveAsTable(), NOT insertInto().
// insertInto() ignores all DataFrameWriter options, so replaceWhere would
// be silently dropped and SaveMode.Overwrite would wipe the entire table.
df.write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", "ds IN ('2024-07-11', '2024-07-12')")
  .saveAsTable(tableName)
```

This is more efficient than MERGE INTO because:
1. No row-level comparison — operates at file granularity via stats
2. Data-skipping applies directly to the `replaceWhere` predicate
3. Single atomic commit (no window of missing data)
4. Works identically on partitioned and liquid-clustered tables

Reference: [Databricks Selective Overwrite documentation](https://docs.databricks.com/aws/en/delta/selective-overwrite)

---

## Proposed Design

### 1. Configuration Interface (Python API)

Add a new `cluster_by_columns` parameter at three levels:

#### a) Object-level (GroupBy / Join / StagingQuery)

```python
GroupBy(
    sources=...,
    keys=["user_id"],
    aggregations=[...],
    # New parameter:
    cluster_by_columns=["ds", "user_id"],  # replaces PARTITIONED BY with CLUSTER BY
)

Join(
    left=...,
    right_parts=[...],
    cluster_by_columns=["ds"],
)

StagingQuery(
    query="...",
    cluster_by_columns=["ds", "event_type"],
)
```

**Semantics:**
- When `cluster_by_columns` is set and non-empty, the output table is created with `CLUSTER BY (col1, col2, ...)` instead of `PARTITIONED BY`.
- The partition column (default `ds`) is still **present as a regular column** in the table — it just isn't a physical partition boundary. The existing `insertPartitions` MERGE INTO path handles the write correctly.
- When `cluster_by_columns` is `None` (default), behavior is unchanged (Hive-style partitioning).

#### b) Team-level (via Spark conf)

```python
# In teams.py:
default = Team(
    conf=ConfigProperties(common={
        "spark.chronon.table_write.format": "delta",
        # New conf key:
        "spark.chronon.output.cluster_by_columns": "ds",  # comma-separated
    }),
)
```

**Priority (highest wins):** Object-level → Team-level spark conf → None (partitioned)

#### c) MetaData / Thrift (for serialized configs)

```thrift
// In api.thrift, inside MetaData struct:
struct MetaData {
    ...
    7: optional list<string> clusterByColumns
}
```

### 2. Thrift Schema Changes

```thrift
// api.thrift — MetaData struct addition:
struct MetaData {
    ...
    // Existing:
    5: optional list<string> additionalOutputPartitionColumns
    6: optional map<string, string> tableProperties

    // New:
    7: optional list<string> clusterByColumns
}
```

### 3. Scala Implementation Changes

#### a) `CreationUtils.scala` — New DDL generation method

```scala
object CreationUtils {

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: List[String],
                     tableProperties: Map[String, String],
                     tableTypeString: String,
                     clusterByColumns: List[String] = List.empty  // NEW PARAM
                    ): String = {

    val useClusterBy = clusterByColumns.nonEmpty

    // When clustering, all columns are data columns (no partition extraction)
    val dataSchema = if (useClusterBy) schema
                     else StructType(schema.filterNot(field => partitionColumns.contains(field.name)))

    val createFragment = s"""CREATE TABLE IF NOT EXISTS $tableName (
       |    ${dataSchema.toDDL}
       |)
       |${if (tableTypeString.isEmpty) "" else s"USING $tableTypeString"}""".stripMargin

    val layoutFragment = if (useClusterBy) {
      s"CLUSTER BY (${clusterByColumns.mkString(", ")})"
    } else if (partitionColumns.nonEmpty) {
      val partitionDefinitions = schema
        .filter(field => partitionColumns.contains(field.name))
        .map(field => s"${field.name} ${field.dataType.catalogString}")
      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}
         |)""".stripMargin
    } else ""

    val propertiesFragment = ...  // unchanged

    Seq(createFragment, layoutFragment, propertiesFragment).mkString("\n")
  }
}
```

#### b) `Format.scala` — Extended `createTable` signature

```scala
trait Format {
  def createTable(tableName: String,
                  schema: StructType,
                  partitionColumns: List[String],
                  providedProperties: Map[String, String],
                  semanticHash: Option[String] = None,
                  clusterByColumns: List[String] = List.empty  // NEW
                 )(implicit sparkSession: SparkSession): Unit
}
```

Each format can validate:
- `DeltaLake` / `DatabricksDeltaLake`: Accept `clusterByColumns` — passes to DDL.
- `Iceberg` / `Hive`: Reject `clusterByColumns` with a clear error: "Liquid clustering is not supported for format X. Remove `cluster_by_columns` or use Delta Lake."

#### c) `TableUtils.insertPartitions()` — Write path changes

```scala
def insertPartitions(df: DataFrame,
                     tableName: String,
                     tableProperties: Map[String, String] = null,
                     partitionColumns: List[String] = List(partitionColumn),
                     autoExpand: Boolean = false,
                     semanticHash: Option[String] = None,
                     clusterByColumns: List[String] = List.empty  // NEW
                    ): Unit = {

  // Table creation
  if (!tableReachable(tableName, ignoreFailure = true)) {
    tableFormatProvider.writeFormat.createTable(
      tableName, schema, partitionColumns, tableProperties,
      semanticHash, clusterByColumns)(sparkSession)
  }

  // Write path selection
  val isClustered = clusterByColumns.nonEmpty
  val isDelta = tableFormatProvider.readFormat(tableName)
    .exists(f => f == DeltaLake || f == DatabricksDeltaLake)
  val isIceberg = tableFormatProvider.readFormat(tableName).contains(Iceberg)
  val hasPartitionSpec = isIceberg && Try(
    Iceberg.partitionColumnNames(tableName)(sparkSession).nonEmpty).getOrElse(false)

  if (isClustered && isDelta) {
    // BEST PATH for clustered Delta: replaceWhere
    // Atomic, uses data-skipping stats, no row-level comparison.
    // NOTE: MUST use saveAsTable(), NOT insertInto() — insertInto() ignores
    // all DataFrameWriter options, so replaceWhere would be silently dropped.
    val replaceWherePredicate = partitionColumns.map { pc =>
      val values = finalizedDf.select(col(pc)).distinct().collect()
        .map(row => lit(row.get(0)).expr.sql)
      s"`$pc` IN (${values.mkString(", ")})"
    }.mkString(" AND ")

    finalizedDf.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", replaceWherePredicate)
      .saveAsTable(tableName)

  } else if (isIceberg && partitionColumns.nonEmpty && !hasPartitionSpec) {
    // Fallback for unpartitioned Iceberg: MERGE INTO with ON FALSE
    // (Iceberg does not support replaceWhere)
    val tempView = s"__chronon_insert_${tableName.replace('.', '_')}_${System.nanoTime()}"
    finalizedDf.createOrReplaceTempView(tempView)
    val deleteCondition = partitionColumns
      .map { pc =>
        val values = finalizedDf.select(col(pc)).distinct().collect()
          .map(row => lit(row.get(0)).expr.sql)
        s"target.`$pc` IN (${values.mkString(", ")})"
      }.mkString(" AND ")
    val mergeSQL = s"""MERGE INTO $tableName AS target
       |USING $tempView AS source
       |ON FALSE
       |WHEN NOT MATCHED BY SOURCE AND $deleteCondition THEN DELETE
       |WHEN NOT MATCHED THEN INSERT *""".stripMargin
    sparkSession.sql(mergeSQL)
    sparkSession.catalog.dropTempView(tempView)

  } else {
    // Standard partitioned write (Hive-style)
    finalizedDf.write.mode(SaveMode.Overwrite).insertInto(tableName)
  }
}
```

**Key design decisions:**
- **Delta clustered → `replaceWhere` + `saveAsTable`**: Atomic, efficient, uses data-skipping. No full-table scan. Must use `saveAsTable()`, not `insertInto()` (which ignores `replaceWhere`).
- **Iceberg unpartitioned → MERGE INTO ON FALSE**: Iceberg lacks `replaceWhere`; MERGE INTO is the only atomic option.
- **Hive/partitioned → `insertInto` with Overwrite**: Existing behavior, unchanged.

#### d) `Extensions.scala` — Pass-through in `save()`

```scala
def save(tableName: String,
         tableProperties: Map[String, String] = null,
         partitionColumns: Seq[String] = List(tableUtils.partitionColumn),
         autoExpand: Boolean = false,
         semanticHash: Option[String] = None,
         clusterByColumns: Seq[String] = Seq.empty  // NEW
        ): Unit = {
  TableUtils(df.sparkSession).insertPartitions(
    df, tableName, tableProperties, partitionColumns.toList,
    autoExpand, semanticHash, clusterByColumns.toList)
}
```

#### e) Callers: `StagingQuery.scala`, `GroupBy.scala`, and every modular Join batch job

Chronon's Join execution has two planners: the legacy monolith planner (single `MergeJob` writes
the final output) and the modular planner (`modular_execution=True`), which splits the join into
several independently-orchestrated batch jobs, each writing its own table. `clusterByColumns` must
be threaded through **every** one of these write sites, not just the final output, since each is a
real physical table subject to the same `PARTITIONED BY` vs. `CLUSTER BY` choice:

| Job | Table written |
|---|---|
| `batch/StagingQuery.scala` | StagingQuery output |
| `GroupBy.scala` (`computeBackfill`) | GroupBy backfill output |
| `batch/SourceJob.scala` | Shared per-source snapshot table |
| `batch/JoinBootstrapJob.scala` | Per-join bootstrap table |
| `batch/JoinPartJob.scala` | Per-join-part cache table |
| `batch/JoinDerivationJob.scala` | Final Join output — **modular planner** |
| `batch/MergeJob.scala` | Final Join output — **monolith planner** |

Each reads `clusterByColumns` from its own `metaData` and passes it through:

```scala
// StagingQuery.scala
private val clusterByCols: Seq[String] =
  Option(stagingQueryConf.metaData.clusterByColumns).map(_.toScala).getOrElse(Seq.empty)

// In compute():
df.save(outputTable, tableProps, partitionCols,
        autoExpand = ..., clusterByColumns = clusterByCols)
```

⚠️ **Gap found during implementation:** an initial pass only wired `StagingQuery` and `MergeJob`
(the monolith planner's final-output writer), following the existing precedent that
`additionalOutputPartitionColumns` was StagingQuery-only. This missed that `MergeJob` is *not* the
final writer under `modular_execution=True` — `JoinDerivationJob` is — so any Join run with the
modular planner silently ignored `cluster_by_columns`. GroupBy backfill output was also missed.
Both gaps are closed: all seven call sites above now forward `clusterByColumns`.

### 4. Python API Changes

#### a) `query.py` — No changes needed (query is for input reading)

#### b) `group_by.py` / `join.py` / `staging_query.py`

```python
def GroupBy(
    ...,
    cluster_by_columns: Optional[List[str]] = None,  # NEW
) -> ttypes.GroupBy:
    ...
    meta_data = api.MetaData(
        ...,
        clusterByColumns=cluster_by_columns,
    )
```

Same for `Join(...)` and `StagingQuery(...)`.

### 5. Partition Discovery for Clustered Output Tables

When Chronon reads its **own** output tables (e.g., for `unfilledRanges` checks), it uses `TableUtils.partitions()`. For clustered tables, this must work without `SHOW PARTITIONS`:

- **Delta Lake** already has a fallback: `scanDistinctPartitions()` which does `SELECT DISTINCT partition_col FROM table`. This works for clustered tables.
- **Important: `DateType` support.** If the partition column (e.g. `featureDt`) is `DateType` rather than `StringType` in the table schema (common when a StagingQuery aliases a DATE source column as the partition key), `scanDistinctPartitions` must cast DATE values to `yyyy-MM-dd` strings. Without this, the method silently returns an empty list and `unfilledRanges` reports all partitions as missing.
- Additionally, `DeltaLake.statsDateRange()` can compute min/max from the transaction log stats — already tested with `CLUSTER BY` (see `DeltaLakeTest.scala:133`).
- **Partition column resolution:** Chronon determines which column to scan via `spark.chronon.partition.column` (default `ds`), **not** from `cluster_by_columns`. The two are independent — `cluster_by_columns` only controls the physical table layout (`CLUSTER BY` clause) and write strategy (`replaceWhere` predicate).

### 6. Validation Rules

In `conf_validator.py`:

```python
def _validate_cluster_by_columns(self, meta_data, config_name):
    if not meta_data.clusterByColumns:
        return []
    errors = []
    # 1. cluster_by_columns requires Delta format
    write_format = self._get_effective_write_format(meta_data)
    if write_format and write_format not in ("delta",):
        errors.append(
            f"{config_name}: cluster_by_columns requires Delta Lake format, "
            f"got '{write_format}'"
        )
    # 2. cluster_by_columns should include the partition column
    partition_col = self._get_partition_column(meta_data)
    if partition_col not in meta_data.clusterByColumns:
        errors.append(
            f"{config_name}: cluster_by_columns should include the partition "
            f"column '{partition_col}' for efficient temporal queries and "
            f"unfilled-range detection"
        )
    return errors
```

---

## Migration & Compatibility

### New Tables
- Simply set `cluster_by_columns=["ds"]` — table is created with `CLUSTER BY`.

### Existing Partitioned Tables
- **Cannot be migrated in-place.** Delta Lake does not allow converting a partitioned table to liquid clustering (would require recreating the table).
- Recommendation: Document that this is a **new table creation** setting. Existing tables retain their partitioning scheme.
- If users want to migrate: drop-and-recreate (manual), or change `output_namespace` to write to a new location.

### Backward Compatibility
- Default behavior is unchanged (`cluster_by_columns=None` → `PARTITIONED BY`).
- Thrift field is optional — old configs continue to work.
- The MERGE INTO write path already exists in production (for unpartitioned Iceberg tables).

---

## Implementation Plan

### Phase 1: Core Infrastructure (Scala)
1. Add `clusterByColumns` field to MetaData in Thrift
2. Modify `CreationUtils.createTableSql()` to support `CLUSTER BY`
3. Extend `Format.createTable()` signature
4. Update `DeltaLake` and `DatabricksDeltaLake` to accept clustering
5. Add validation in `Iceberg`/`Hive` to reject clustering
6. Widen the MERGE INTO write condition in `TableUtils.insertPartitions()`
7. Thread `clusterByColumns` through `Extensions.save()` → callers

### Phase 2: Python API + Validation
8. Add `cluster_by_columns` to `GroupBy()`, `Join()`, `StagingQuery()` Python functions
9. Add validation in `conf_validator.py`
10. Add team-level conf support via `spark.chronon.output.cluster_by_columns`

### Phase 3: Tests
11. Unit test: `CreationUtils` generates correct DDL with `CLUSTER BY`
12. Unit test: `insertPartitions` uses MERGE INTO path for clustered tables
13. Integration test: E2E GroupBy backfill with `cluster_by_columns=["ds"]` on Delta
14. Integration test: `unfilledRanges` / partition discovery works on clustered output
15. Validation test: Error when `cluster_by_columns` used with Hive/Iceberg format

### Phase 4: Documentation
16. Update `Source.md` or create new doc for output table configuration
17. Add examples to existing GroupBy/Join/StagingQuery docs

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Delta version too old for CLUSTER BY | Table creation fails | Validate at compile time (warn) + clear runtime error message |
| `replaceWhere` constraint check rejects rows outside predicate | Write fails if DataFrame has unexpected ds values | Build predicate from actual DataFrame values (IN list), not from config — always matches |
| `unfilledRanges` fails on clustered tables | Backfills break | Already handled: Delta `scanDistinctPartitions()` and `statsDateRange()` work without partitions |
| Partition column is `DateType` instead of `StringType` | `scanDistinctPartitions` silently returns empty → full recompute every run | Added `DateType` handling: cast to `yyyy-MM-dd` strings before collecting distinct values |
| `replaceWhere` used with `insertInto()` | `insertInto` ignores all options → full table overwrite on every write | Must use `saveAsTable()` with explicit `.format("delta")` for `replaceWhere` to take effect |
| Users accidentally set clustering on Iceberg | Confusion | Compile-time validation error with clear message |
| `replaceWhere` on OSS Delta without data-skipping | Slower than expected | Still atomic and correct; performance degrades gracefully to full-file scan (same as MERGE INTO would) |

---

## Alternatives Considered

### A) Use `table_properties` only (no first-class field)

Users could technically do:
```python
table_properties={"delta.clustering.columns": "ds,user_id"}
```

**Rejected because:**
- Doesn't change DDL generation (still emits `PARTITIONED BY`)
- Doesn't trigger the MERGE INTO write path
- Not portable across formats
- Error-prone — users must know internal Delta property names

### B) Add a boolean `unpartitioned` flag

**Rejected because:**
- Clustering needs column names (which columns to cluster by)
- Less expressive than a list of columns
- Doesn't convey intent

### C) Modify `Format.createTable()` to auto-detect from table_properties

**Rejected because:**
- Coupling DDL generation to property key parsing is fragile
- Different formats use different property keys for clustering
- Makes behavior implicit and hard to debug

---

## Example Usage

```python
from ai.chronon.group_by import GroupBy
from ai.chronon.query import Query, select
from ai.chronon.types import Source, EventSource

user_features = GroupBy(
    sources=[Source(
        events=EventSource(
            table="catalog.raw.user_events",
            query=Query(
                selects=select("user_id", "event_value", "event_type"),
                time_column="UNIX_TIMESTAMP(event_ts) * 1000",
                partition_column="event_ts",
                time_partitioned=True,
            )
        )
    )],
    keys=["user_id"],
    aggregations=[...],
    output_namespace="catalog.features",
    cluster_by_columns=["ds", "user_id"],  # Output uses liquid clustering
)
```

Generated DDL:
```sql
CREATE TABLE IF NOT EXISTS catalog.features.user_features (
    user_id STRING,
    event_value_sum BIGINT,
    ds STRING
)
USING delta
CLUSTER BY (ds, user_id)
```

Write semantic (`replaceWhere` instead of partition overwrite):
```scala
// Chronon builds the predicate from the DataFrame's distinct ds values:
df.write
  .mode("overwrite")
  .option("replaceWhere", "ds IN ('2024-07-11', '2024-07-12')")
  .insertInto("catalog.features.user_features")
// Delta atomically removes files matching the predicate and writes new files.
```

---

## Files to Modify

| File | Change |
|------|--------|
| `thrift/api.thrift` | Add `clusterByColumns` to MetaData |
| `spark/.../CreationUtils.scala` | Add CLUSTER BY DDL branch |
| `spark/.../Format.scala` | Extend `createTable` signature |
| `spark/.../DeltaLake.scala` | Accept and pass clustering |
| `spark/.../DatabricksDeltaLake.scala` | Accept and pass clustering |
| `spark/.../Iceberg.scala` | Reject with error |
| `spark/.../Hive.scala` | Reject with error |
| `spark/.../TableUtils.scala` | Widen MERGE INTO condition for clustering |
| `spark/.../Extensions.scala` | Pass `clusterByColumns` through `save()` |
| `spark/.../batch/StagingQuery.scala` | Read and pass `clusterByColumns` |
| `spark/.../GroupBy.scala` (`computeBackfill`) | Read and pass `clusterByColumns` |
| `spark/.../batch/SourceJob.scala` | Read and pass `clusterByColumns` |
| `spark/.../batch/JoinBootstrapJob.scala` | Read and pass `clusterByColumns` |
| `spark/.../batch/JoinPartJob.scala` | Read and pass `clusterByColumns` |
| `spark/.../batch/JoinDerivationJob.scala` | Read and pass `clusterByColumns` (modular planner's final output) |
| `spark/.../batch/MergeJob.scala` | Read and pass `clusterByColumns` (monolith planner's final output) |
| `python/src/ai/chronon/group_by.py` | Add `cluster_by_columns` param |
| `python/src/ai/chronon/join.py` | Add `cluster_by_columns` param |
| `python/src/ai/chronon/staging_query.py` | Add `cluster_by_columns` param |
| `python/src/ai/chronon/cli/compile/conf_validator.py` | Add validation |
| `docs/source/authoring_features/Source.md` | Document feature |

---

## Open Questions

1. **Should `cluster_by_columns` default include `ds` automatically?** Or require the user to explicitly include it? (Recommendation: require explicit — more transparent.)

2. **Should we support `ALTER TABLE ... CLUSTER BY` for adding clustering to existing unpartitioned tables?** (Recommendation: out of scope for v1 — focus on table creation.)

3. **Team-level default**: Should the team conf `spark.chronon.output.cluster_by_columns` apply to ALL new tables in the team, or only when the object explicitly opts in? (Recommendation: team-level sets a default that objects can override or disable with `cluster_by_columns=[]`.)
