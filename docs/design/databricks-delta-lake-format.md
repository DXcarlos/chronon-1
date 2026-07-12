# RFC: Databricks Delta Lake Format Provider for Chronon

## Summary

Introduce a `DatabricksDeltaLake` format and companion `DatabricksFormatProvider` to support running Chronon natively on **Databricks Runtime** (DBR 13.x–16.x LTS) without depending on OSS `delta-spark` classes that are absent from the runtime classpath.

---

## Motivation

### Problem

Chronon's existing `DeltaLake` format object imports `org.apache.spark.sql.delta.DeltaLog` (the OSS Delta Lake API). This class is **not available** on Databricks Runtime because Databricks ships its own internal Delta implementation under a different package (`com.databricks.sql.transaction.tahoe.*`). When a Chronon job runs on DBR and resolves a table as Delta format, the following error occurs:

```
java.lang.NoClassDefFoundError: org/apache/spark/sql/delta/DeltaLog$
    at ai.chronon.spark.catalog.DeltaLake$.partitions(DeltaLake.scala:56)
    at ai.chronon.spark.catalog.Format.$anonfun$primaryPartitions$1(Format.scala:97)
    ...
Caused by: java.lang.ClassNotFoundException: org.apache.spark.sql.delta.DeltaLog$
    at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:445)
```

This breaks all Chronon operations that involve partition discovery for Delta tables, including:
- `StagingQuery` backfills via `tableUtils.unfilledRanges()`
- `GroupBy` batch computations
- `Join` backfills
- Any sensor / readiness check

### Why Not Just Add `delta-spark` as a Dependency?

1. **Databricks Runtime already bundles Delta Lake internally** — adding the OSS `delta-spark` JAR creates class conflicts and version mismatches.
2. **The OSS `DeltaLog` API has version-sensitive signatures** — `DeltaLog.update()` changes between Delta 3.2, 3.3, and 3.5. The Chronon build currently compiles against delta-spark 3.3.2 (EMR 7.12.0), which may not match the Delta version bundled in DBR 14.x/15.x/16.x.
3. **Databricks users should not need to manage Delta version compatibility** — the provider pattern allows runtime-appropriate behavior selection.

### Who Is Affected?

Any Chronon user running on Databricks (AWS, Azure, or GCP workspaces) with Delta tables. This is a growing segment as Databricks is the primary lakehouse platform for many organizations.

---

## Current Architecture

### Format Trait Hierarchy

```
Format (trait)
├── Hive        (object) — SHOW PARTITIONS parsing
├── DeltaLake   (object) — OSS DeltaLog API
├── Iceberg     (object) — .partitions metadata table
└── [proposed] DatabricksDeltaLake (object) — Spark SQL only
```

### FormatProvider Pattern

```scala
trait FormatProvider {
  def readFormat(tableName: String): Option[Format]
  def writeFormat: Format
}
```

Configured via Spark config:
```
spark.chronon.table.format_provider.class=<fully.qualified.ClassName>
```

Existing providers:
- `DefaultFormatProvider` — routes Delta → `DeltaLake` (OSS)
- `GcpFormatProvider` — extends Default, adds BigQuery support

### Critical Call Path (from the error stack trace)

```
Driver.main()
  → StagingQuery.computeStagingQuery()
    → tableUtils.unfilledRanges(outputTable, range)
      → tableUtils.partitions(outputTable)
        → formatProvider.readFormat(tableName)  // returns DeltaLake
        → DeltaLake.primaryPartitions(...)
          → DeltaLake.partitions(...)
            → DeltaLog.forTable(spark, path)  // 💥 NoClassDefFoundError on DBR
```

---

## Proposed Design

### New Components

#### 1. `DatabricksDeltaLake` (case object extends `Format`)

A Delta Lake format implementation that uses **only standard Spark SQL APIs** — no references to `org.apache.spark.sql.delta.*` classes.

**Module location:** `spark/src/main/scala/ai/chronon/spark/catalog/DatabricksDeltaLake.scala`

**Rationale for placement in `spark` module (not a cloud-specific module):**
- Has zero cloud-specific dependencies
- Works identically on Databricks for AWS, Azure, and GCP
- Shares the same package with other Format implementations for sealed access patterns

#### 2. `DatabricksFormatProvider` (class extends `DefaultFormatProvider`)

Routes Delta tables to `DatabricksDeltaLake` instead of `DeltaLake`.

**Module location:** `spark/src/main/scala/ai/chronon/spark/catalog/DatabricksFormatProvider.scala`

### API Contract

`DatabricksDeltaLake` implements the full `Format` trait contract:

| Method | Strategy | Trade-off vs OSS DeltaLake |
|--------|----------|---------------------------|
| `partitions()` | `SELECT DISTINCT <partition_cols> FROM table` | No DeltaLog read; relies on Databricks data skipping |
| `partitionColumnNames()` | `DESCRIBE DETAIL` → `partitionColumns` | Identical (both use DESCRIBE DETAIL) |
| `firstAvailablePartition()` | metadata → `SELECT MIN(col)` scan | No Delta log stats tier |
| `lastAvailablePartition()` | metadata → `SELECT MAX(col)` scan | No Delta log stats tier |
| `virtualPartitions()` | metadata → base trait `MIN/MAX` scan | No Delta log stats tier |
| `createTable()` | Inherited from `Format` trait | Identical |
| `supportSubPartitionsFilter` | `true` | Identical |
| `tableTypeString` | `"delta"` | Identical |

### Performance Trade-off Analysis

The OSS `DeltaLake` object has a **Delta log stats tier** that reads file-level `minValues`/`maxValues` from the transaction log without scanning data. `DatabricksDeltaLake` omits this tier because:

1. **DeltaLog API unavailable** — The class simply doesn't exist in DBR's classpath under the OSS package name.
2. **Databricks optimizer compensates** — On DBR, `SELECT MIN/MAX` queries on partition columns benefit from:
   - Data skipping (Z-ordering / liquid clustering)
   - Column statistics in the Delta log (optimizer-level, not application-level)
   - Photon engine optimizations
3. **Partition boundary queries are infrequent** — Typically called once per job invocation (in `unfilledRanges`), not in hot loops.
4. **Partition listing via `SELECT DISTINCT`** — For the typical Chronon pattern (string `ds` column), this is a single metadata-level aggregation on Databricks.

**Expected latency impact:** Negligible for daily partition granularity. For sub-daily grids with many partitions, the scan approach may be ~1-3 seconds slower than log-based stats, but this occurs once per job and is dwarfed by actual compute time.

### Configuration

Users activate the Databricks provider via Spark configuration:

```python
# In Databricks notebook or cluster config:
spark.conf.set(
    "spark.chronon.table.format_provider.class",
    "ai.chronon.spark.catalog.DatabricksFormatProvider"
)
```

Or in cluster Spark Config:
```
spark.chronon.table.format_provider.class=ai.chronon.spark.catalog.DatabricksFormatProvider
```

### Write Format Routing

When `spark.chronon.table_write.format=delta`, `DatabricksFormatProvider.writeFormat` returns `DatabricksDeltaLake` (not the OSS `DeltaLake`), ensuring table creation also avoids OSS Delta APIs.

---

## Detailed Design

### DatabricksDeltaLake Implementation

```scala
package ai.chronon.spark.catalog

case object DatabricksDeltaLake extends Format {

  override def tableTypeString: String = "delta"
  override def supportSubPartitionsFilter: Boolean = true

  // Partition column discovery — DESCRIBE DETAIL works on both OSS and DBR
  override def partitionColumnNames(tableName: String)(implicit sparkSession: SparkSession): Seq[String] =
    Try {
      sparkSession.sql(s"DESCRIBE DETAIL $tableName")
        .select("partitionColumns")
        .head().getSeq[String](0).toList
    }.getOrElse(Seq.empty)

  // Partition listing — SELECT DISTINCT instead of DeltaLog.allFiles
  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partCols = partitionColumnNames(tableName)
    if (partCols.isEmpty) return List.empty

    val df = sparkSession.read.table(tableName)
    val filtered = if (partitionFilters.isEmpty) df else df.where(partitionFilters)
    filtered
      .select(partCols.map(col): _*)
      .distinct()
      .collect()
      .map { row =>
        partCols.zipWithIndex.map { case (colName, idx) =>
          colName -> Option(row.get(idx)).map(_.toString).orNull
        }.toMap
      }
      .toList
  }

  // Boundary methods — delegate to metadata + scan (no stats tier)
  override def firstAvailablePartition(...) =
    metadataFirstAvailablePartition(...)
      .orElse(scanFirstAvailablePartition(...))

  override def lastAvailablePartition(...) =
    metadataLastAvailablePartition(...)
      .orElse(scanLastAvailablePartition(...))
}
```

### DatabricksFormatProvider Implementation

```scala
package ai.chronon.spark.catalog

class DatabricksFormatProvider(override val sparkSession: SparkSession)
    extends DefaultFormatProvider(sparkSession) {

  override def readFormat(tableName: String): Option[Format] =
    Option(
      if (isIcebergTable(tableName)) Iceberg
      else if (isDeltaTable(tableName)) DatabricksDeltaLake
      else if (sparkSession.catalog.tableExists(tableName)) Hive
      else null
    )

  override def writeFormat: Format = {
    val typeString = sparkSession.conf.get("spark.chronon.table_write.format", "").toLowerCase
    typeString match {
      case "delta" => DatabricksDeltaLake
      case other   => FormatProvider.formatFromTypeString(other)
    }
  }
}
```

### Error Handling

`DatabricksDeltaLake.partitions()` handles:
- **Table not found** → returns `List.empty` (consistent with other formats)
- **DESCRIBE DETAIL failure** (table is not Delta) → returns `Seq.empty` for partitionColumnNames
- **SELECT DISTINCT failure** → logs warning, returns `List.empty`

This follows the existing pattern where `Format.primaryPartitions()` wraps `partitions()` in a `Try` and degrades gracefully.

---

## Alternatives Considered

### 1. Reflection-based DeltaLog Access

Use reflection to call the Databricks-internal Delta classes at runtime:

```scala
val deltaLogClass = Class.forName("com.databricks.sql.transaction.tahoe.DeltaLog")
val forTableMethod = deltaLogClass.getMethod("forTable", classOf[SparkSession], classOf[String])
```

**Rejected because:**
- Internal Databricks packages have no stability guarantees
- Would break across DBR versions (13.x → 14.x → 15.x → 16.x)
- Error-prone: method signatures, return types change without notice
- Harder to test in OSS Spark environments

### 2. Single DeltaLake Object with Runtime Detection

Modify the existing `DeltaLake` object to detect the runtime and conditionally use DeltaLog or SQL fallback:

```scala
case object DeltaLake extends Format {
  private lazy val useDeltaLog: Boolean = Try(Class.forName("org.apache.spark.sql.delta.DeltaLog")).isSuccess

  override def partitions(...) =
    if (useDeltaLog) deltaLogPartitions(...)
    else sqlFallbackPartitions(...)
}
```

**Rejected because:**
- Pollutes the OSS implementation with conditional logic
- Class loading can have side effects (static initializers)
- Testing requires mocking class loading — brittle
- Harder to reason about behavior in code review

### 3. Separate `cloud_databricks` Module

Place the implementation in a dedicated build module (like `cloud_gcp`):

**Rejected because:**
- `DatabricksDeltaLake` has zero external dependencies beyond Spark SQL
- A separate module adds build complexity for no dependency isolation benefit
- The GCP module exists because it depends on BigQuery JARs — no such case here
- All other format implementations live in the `spark` module

### 4. Shade/Relocate OSS delta-spark

Bundle `delta-spark` with package relocation to avoid conflicts:

**Rejected because:**
- Delta Lake has deep integration with Spark internals (catalyst rules, session extensions)
- Shading Spark-internal classes is fragile and known to cause subtle bugs
- Adds 10+ MB to the assembly JAR
- Still requires version-matching with DBR's internal Delta behavior

---

## Integration with Existing Features

### Liquid Clustering (RFC: `liquid-clustering-output-tables.md`)

`DatabricksDeltaLake` is designed to work with the proposed liquid clustering feature:
- When tables are created with `CLUSTER BY` instead of `PARTITIONED BY`, `partitionColumnNames()` returns empty (Delta DESCRIBE DETAIL shows `[]` for clustered tables).
- The `scanDistinctPartitions()` fallback in `Format` then provides partition discovery via `SELECT DISTINCT ds`.
- `DatabricksDeltaLake`'s SQL-only approach naturally handles clustered tables since it doesn't rely on DeltaLog's partition metadata.

### FormatProvider.formatFromTypeString()

The global `formatFromTypeString()` in `FormatProvider` object continues to return `DeltaLake` (OSS) for `"delta"`. Only `DatabricksFormatProvider.writeFormat` overrides this for the Databricks path. This preserves backward compatibility for EMR/Dataproc/OSS Spark users.

---

## Testing Strategy

### Unit Tests

1. **`DatabricksDeltaLakeTest`** — Verify behavior with local Spark (no Delta runtime):
   - `tableTypeString` returns `"delta"`
   - `supportSubPartitionsFilter` returns `true`
   - Empty results for non-existent tables (graceful degradation)
   - Partition column discovery via DESCRIBE DETAIL (requires Delta-enabled local Spark)

2. **`DatabricksFormatProviderTest`** — Verify routing logic:
   - Non-existent table → `None`
   - Delta table → `DatabricksDeltaLake` (not `DeltaLake`)
   - Write format with `spark.chronon.table_write.format=delta` → `DatabricksDeltaLake`
   - Write format with empty config → `Hive`

### Integration Tests (Databricks-specific)

These should be run on a Databricks cluster to validate end-to-end:

1. Create a Delta table with known partitions
2. Configure `DatabricksFormatProvider`
3. Verify `partitions()` returns correct partition maps
4. Verify `firstAvailablePartition()` / `lastAvailablePartition()` return correct boundaries
5. Run a `StagingQuery` backfill end-to-end
6. Verify `unfilledRanges()` correctly identifies missing partitions

### Backward Compatibility

- **No changes to `DeltaLake` or `DefaultFormatProvider`** — existing EMR/OSS users are unaffected
- **Opt-in only** — requires explicit Spark config to activate
- **Same `Format` trait contract** — all downstream code (TableUtils, StagingQuery, Driver) works without modification

---

## Rollout Plan

1. **Phase 1:** Merge `DatabricksDeltaLake` + `DatabricksFormatProvider` + unit tests
2. **Phase 2:** Add integration test notebook for Databricks CI
3. **Phase 3:** Update documentation (user guide, Databricks setup guide)
4. **Phase 4:** Consider making `DatabricksFormatProvider` auto-detect DBR (via `spark.databricks.clusterUsageTags.sparkVersion` config) to eliminate manual configuration

---

## Open Questions

1. **Should `DatabricksFormatProvider` be auto-activated on DBR?** — We could detect `spark.databricks.clusterUsageTags.sparkVersion` and auto-select the provider. This improves UX but adds implicit behavior. Current proposal: explicit opt-in.

2. **Should we expose Delta log stats on DBR via Databricks SQL functions?** — Databricks offers `DESCRIBE HISTORY` and `_delta_log` access patterns that could provide stats without the OSS DeltaLog API. This could be a future optimization.

3. **Interaction with Unity Catalog volumes** — Does `DESCRIBE DETAIL` work correctly for UC-managed Delta tables? Preliminary testing says yes, but should be validated across DBR versions.

---

## References

- [Delta Lake OSS — SHOW PARTITIONS not supported](https://github.com/delta-io/delta/issues/996)
- [Databricks Runtime release notes — internal Delta packaging](https://docs.databricks.com/release-notes/runtime/)
- [Chronon FormatProvider pattern](../spark/src/main/scala/ai/chronon/spark/catalog/FormatProvider.scala)
- [RFC: Liquid Clustering Support](./liquid-clustering-output-tables.md)
