---
title: "Data Quality"
order: 2
---

# Data Quality Metrics

Chronon automatically computes and stores data quality metrics for both offline batch jobs and near real-time streaming contexts, enabling users to monitor the health and quality of their feature pipelines over time.

## Overview

Chronon's data quality system provides two modes of metrics collection:

1. **Offline Batch Metrics**: Statistics extracted from materialized feature tables and persisted to a KV store for later retrieval and analysis. These metrics are computed automatically at the end of each batch job run. For Join definitions, setting `enable_stats_compute=True` also creates a downstream data quality workflow that computes feature statistics for the Join output.

2. **Near Real-Time Streaming Metrics**: Feature-level null rates and counts collected during feature fetching operations, providing immediate visibility into data quality as features are served.

Both modes provide insights into data completeness, distribution, and schema evolution.

## How Metrics Are Computed

### Offline Batch Metrics

#### 1. Batch Job Execution

When a batch job completes (Join, GroupBy, or Staging Query), Chronon automatically triggers data quality metrics extraction.

The metrics extraction process:
- Occurs after the main computation completes successfully
- Is non-blocking and won't fail the job if metrics extraction encounters errors
- Skips sensor nodes since they don't produce output data
- Requires the `--tableStatsDataset` parameter to be provided when running the batch job

#### 2. Join Feature Data Quality

For Join feature sets, enable downstream feature statistics by setting `enable_stats_compute=True` on the Join definition:

```python
v1 = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=user_features.v1),
    ],
    online=True,
    enable_stats_compute=True,
)
```

When enabled, Chronon adds stats compute and upload nodes to the Join workflow. These nodes compute feature-level statistics from the Join output and publish them for downstream data quality monitoring. In Zipline, the computed statistics are surfaced in the UI under **Offline > Metrics**.

![Join feature data quality metrics in Zipline](../../images/dataquality.gif)

#### 3. Statistics Extraction (Iceberg Tables)

For Iceberg tables, Chronon leverages Iceberg's built-in partition-level statistics stored in manifest files. The extraction process reads these manifest files directly without scanning the actual data, making metrics extraction highly efficient.

The extraction process:
1. Loads the Iceberg table metadata
2. Reads manifest files for the current snapshot
3. Aggregates file-level statistics by partition
4. Excludes partition columns from statistics (since they have fixed values within a partition)

#### 4. Metrics Computed

Chronon currently computes the following metrics per partition per column:

##### Null Counts
- **Row Count**: Total number of rows in the partition
- **Null Count**: Number of null values per column
- Useful for monitoring data completeness and detecting missing data issues


Future support may include:
- **Distinct Counts**: Approximate number of unique values (framework in place)
- **Min/Max Values**: Value ranges for numeric and comparable types (extracted but not yet persisted)

#### 5. Storage Format

Metrics are stored in the KV store with the following structure:

**Key Format**: `<table_name>#<metric_type>#<YYYY-MM>` (e.g., `my_table#NULL_COUNTS#2025-08`)

**Value Format**: Thrift-encoded `TileStats` objects containing:
```
TileStats {
  nullCounts: {
    rowCount: <total_rows>
    nullCounts: {
      <field_id>: <null_count>,
      <field_id>: <null_count>,
      ...
    }
  }
}
```

**Schema Mapping Key**: `<table_name>#schema`

**Schema Mapping Value**: JSON mapping of field IDs to column names:
```json
{
  "1": "user_id",
  "2": "feature_1",
  "3": "feature_2"
}
```

Each metric value is timestamped with the partition date (in milliseconds), allowing time-series queries.

### Near Real-Time Streaming Metrics

In addition to offline batch metrics, Chronon tracks data quality metrics in near real-time as features are fetched and served. These streaming metrics provide immediate feedback on feature quality in production.

#### How Streaming Metrics Work

When features are fetched during join operations, Chronon automatically logs feature-level metrics to track data quality:

1. **Automatic Collection**: Metrics are collected transparently during the feature fetching process without requiring additional configuration
2. **Feature-Level Granularity**: Each individual feature in the response is tracked separately
3. **Exception Filtering**: Features that result in exceptions are excluded from null rate calculations
4. **Real-Time Visibility**: Metrics are emitted immediately, enabling near real-time monitoring and alerting

#### Metrics Collected

The following metrics are tracked per feature during fetch operations:

- **`feature.null_count`**: Counter incremented each time a feature value is null
- **`feature.count`**: Counter incremented for every feature fetched (null or non-null)

These metrics are tagged with:
- **`feature`**: The name of the specific feature
- **`join`**: The join name being fetched
- **`group_by`**: The group by source (if applicable)
- **`environment`**: The execution environment (e.g., `join.fetch`)
- **`team`**: The team owning the feature
- **`production`**: Whether this is a production deployment

#### Calculating Null Rates

The null rate for any feature can be calculated as:

```
null_rate = feature.null_count / feature.count
```

Your metrics backend (e.g., Prometheus, Datadog, CloudWatch) can compute this ratio and alert when null rates exceed acceptable thresholds.

#### Implementation Details

The streaming metrics collection is implemented in the `Fetcher` class (`ai/chronon/online/fetcher/Fetcher.scala:117`):

```scala
def logFeatureNullRates(responseMap: Map[String, AnyRef],
                        context: metrics.Metrics.Context): Unit = {
  responseMap.foreach { case (featureName, value) =>
    if (!featureName.endsWith(FeatureExceptionSuffix)) {
      if (value == null)
        context.increment(
          metrics.Metrics.Name.FeatureNulls,
          Map(metrics.Metrics.Tag.Feature -> featureName)
        )
      context.increment(
        metrics.Metrics.Name.FeatureCount,
        Map(metrics.Metrics.Tag.Feature -> featureName)
      )
    }
  }
}
```

This function is called automatically during join part fetching operations, ensuring comprehensive coverage of all feature requests.

#### Enabling Metrics Collection

To enable streaming metrics collection, set the following system property when running your feature serving application:

```bash
-Dai.chronon.metrics.enabled=true
```

By default, Chronon uses OpenTelemetry for metrics reporting. Ensure your OpenTelemetry configuration is set up to export metrics to your preferred backend.

#### Example: Monitoring with Prometheus

If using Prometheus as your metrics backend, you can create alerting rules based on these metrics:

```yaml
groups:
  - name: chronon_data_quality
    rules:
      - alert: HighFeatureNullRate
        expr: |
          (
            rate(join_fetch_feature_null_count{production="true"}[5m])
            /
            rate(join_fetch_feature_count{production="true"}[5m])
          ) > 0.1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High null rate detected for feature {{ $labels.feature }}"
          description: "Feature {{ $labels.feature }} in join {{ $labels.join }} has a null rate of {{ $value | humanizePercentage }} over the last 5 minutes."
```

## Accessing Offline Batch Metrics (Self-Hosted)

For users self-hosting Chronon, offline batch data quality metrics can be accessed through the KV store interface provided by their `Api` implementation.

### Implementing a Metrics KV Store

Your `Api` implementation should provide a `genMetricsKvStore` method:

```scala
class MyApiImpl extends Api {
  def genMetricsKvStore(tableStatsDataset: String): KVStore = {
    // Return a KVStore implementation for storing metrics
    // This could be the same store as your regular KV store,
    // or a separate instance optimized for time-series data
    new MyMetricsKVStore(tableStatsDataset)
  }
}
```

### Querying Metrics

Metrics can be retrieved using the KV store's `multiGet` method with time range filters:

```scala
import ai.chronon.online.KVStore._
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.observability.TileStats

val metricsKvStore = api.genMetricsKvStore("data_quality_metrics")

// Query null count metrics for a specific table and date range
val request = GetRequest(
  keyBytes = "my_feature_table#NULL_COUNTS".getBytes,
  dataset = "my_feature_table_BATCH",
  startTsMillis = Some(startDateMillis),
  endTsMillis = Some(endDateMillis)
)

val response = Await.result(metricsKvStore.multiGet(Seq(request)), 30.seconds)

response.head.values match {
  case Success(timedValues) =>
    timedValues.foreach { tv =>
      val stats = ThriftJsonCodec.fromCompactBase64[TileStats](
        new String(tv.value, StandardCharsets.UTF_8)
      )
      val nullCounts = stats.getNullCounts
      println(s"Date: ${tv.ts}, Row Count: ${nullCounts.getRowCount}")
      nullCounts.getNullCounts.asScala.foreach { case (fieldId, nullCount) =>
        println(s"  Field $fieldId: $nullCount nulls")
      }
    }
  case Failure(e) =>
    println(s"Error retrieving metrics: $e")
}

// Query schema mapping
val schemaRequest = GetRequest(
  keyBytes = "my_feature_table#schema".getBytes,
  dataset = "my_feature_table_BATCH"
)

val schemaResponse = Await.result(metricsKvStore.multiGet(Seq(schemaRequest)), 30.seconds)
// Parse JSON to get field ID -> column name mapping
```

### Example: BigTable Implementation

The repository includes a reference implementation for Google Cloud BigTable that demonstrates best practices:

- Time-series optimized row key design: `<table_name>#<YYYY-MM>#<metric_type>`
- Efficient time range queries using BigTable filters
- 90-day TTL for automatic cleanup
- GC rules to cap maximum cell count per row (1000 cells)

This implementation can serve as a reference for building your own metrics store on other backends (e.g., DynamoDB, Cassandra, HBase).

## Native Support in Zipline Cloud

For users of **Zipline**, data quality metrics are natively supported with:

- **Automatic Collection**: Metrics are automatically collected for all feature pipelines without additional configuration
- **Dashboards & Visualizations**: Pre-built UI for exploring data quality trends over time
- **Join Feature Metrics**: Join feature statistics enabled with `enable_stats_compute=True` are shown in the Zipline UI under **Offline > Metrics**
- **Alerting**: Configurable alerts for data quality issues (e.g., spike in null counts, row count anomalies)
- **Schema Evolution Tracking**: Automatic tracking of schema changes with impact analysis
- **Historical Analysis**: Long-term retention and advanced analytics on data quality trends
- **Cross-Pipeline Monitoring**: Unified view of data quality across all feature pipelines

To learn more about Zipline's managed features, contact the Zipline team.

## Limitations & Future Enhancements

### Current Limitations

**Offline Batch Metrics:**
- Only Iceberg tables are supported for automatic statistics extraction
- Non-Iceberg tables require manual instrumentation
- Statistics are partition-level aggregates (not column-level distributions)
- Min/Max values and distinct counts are extracted but not yet persisted

**Streaming Metrics:**
- Currently limited to null counts and total counts
- Does not track value distributions or ranges
- Requires OpenTelemetry setup for metrics export

### Future Enhancements
- Column-level histograms and distribution metrics
- Data quality validation rules and anomaly detection
- Integration with data observability platforms
- Custom metric definitions and aggregations
- Extended streaming metrics (min/max, distinct counts, percentiles)
- Move streaming data quality metrics to a dedicated streaming pipeline instead of computing them in the fetcher path for better performance and separation of concerns
