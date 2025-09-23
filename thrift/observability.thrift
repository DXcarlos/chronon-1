namespace py gen_thrift.observability
namespace java ai.chronon.observability

include "common.thrift"

enum Cardinality {
    LOW = 0,
    HIGH = 1
}

/**
+----------------------------------+-------------------+----------------+----------------------------------+
| Metric                           | Moderate Drift    | Severe Drift   | Notes                            |
+----------------------------------+-------------------+----------------+----------------------------------+
| Jensen-Shannon Divergence        | 0.05 - 0.1        | > 0.1          | Max value is ln(2) ≈ 0.69        |
+----------------------------------+-------------------+----------------+----------------------------------+
| Hellinger Distance               | 0.1 - 0.25        | > 0.25         | Ranges from 0 to 1               |
+----------------------------------+-------------------+----------------+----------------------------------+
| Population Stability Index (PSI) | 0.1 - 0.2         | > 0.2          | Industry standard in some fields |
+----------------------------------+-------------------+----------------+----------------------------------+
**/
enum DriftMetric {
    JENSEN_SHANNON = 0,
    HELLINGER = 1,
    PSI = 3
}

struct TileKey {
  1: optional string column
  2: optional string slice
  3: optional string name // name of the join, groupBy, stagingQuery etc
  4: optional i64 sizeMillis
}

// summary of distribution & coverage etc for a given (table, column, slice, tileWindow)
// for categorical types, distribution is histogram, otherwise percentiles
// we also handle container types by counting inner value distribution and inner value coverage
struct TileSummary {
  1: optional list<double> percentiles
  2: optional map<string, i64> histogram
  3: optional i64 count
  4: optional i64 nullCount

  // for container types
  5: optional i64 innerCount // total of number of entries within all containers of this column
  6: optional i64 innerNullCount
  7: optional list<i32> lengthPercentiles

  // high cardinality string type
  8: optional list<i32> stringLengthPercentiles
}

struct TileSeriesKey {
    1: optional string column // name of the column - avg_txns
    2: optional string slice // value of the slice - merchant_category
    3: optional string groupName // name of the columnGroup within node, for join - joinPart name, externalPart name etc
    4: optional string nodeName // name of the node - join name etc
}

// Internal use for persisting data quality metrics to KVStore.
struct PartitionStats {
    1: optional map<i32, i64> nullCounts
    2: optional i64 rowCount
}

// array of tuples of (TileSummary, timestamp) ==(pivot)==> TileSummarySeries
struct TileSummarySeries {
  1: optional list<list<double>> percentiles
  2: optional map<string, list<i64>> histogram
  3: optional list<i64> count
  4: optional list<i64> nullCount

  // for container types
  5: optional list<i64> innerCount // total of number of entries within all containers of this column
  6: optional list<i64> innerNullCount
  7: optional list<list<i32>> lengthPercentiles

  // high cardinality string type
  8: optional list<list<i32>> stringLengthPercentiles

  200: optional list<i64> timestamps
  300: optional TileKey key
}

// (DriftMetric + old TileSummary + new TileSummary) = TileDrift
struct TileDrift {

  // for continuous values - scalar values or within containers
  // (lists - for eg. via last_k or maps for eg. via bucketing)
  1: optional double percentileDrift
  // for categorical values - scalar values or within containers
  2: optional double histogramDrift

  // for all types
  3: optional double countChangePercent
  4: optional double nullRatioChangePercent

  // additional tracking for container types
  5: optional double innerCountChangePercent // total of number of entries within all containers of this column
  6: optional double innerNullCountChangePercent
  7: optional double lengthPercentilesDrift

  // additional tracking for string types
  8: optional double stringLengthPercentilesDrift
}

// PivotUtils.pivot(Array[(Long, TileDrift)])  = TileDriftSeries
// used in front end after this is computed
struct TileDriftSeries {
  1: optional list<double> percentileDriftSeries
  2: optional list<double> histogramDriftSeries
  3: optional list<double> countChangePercentSeries
  4: optional list<double> nullRatioChangePercentSeries

  5: optional list<double> innerCountChangePercentSeries
  6: optional list<double> innerNullCountChangePercentSeries
  7: optional list<double> lengthPercentilesDriftSeries
  8: optional list<double> stringLengthPercentilesDriftSeries

  200: optional list<i64> timestamps

  300: optional TileSeriesKey key
}

struct DriftSpec {
    // slices is another key to summarize the data with - besides the column & slice
    // currently supports only one slice
    1: optional list<string> slices
    // additional things you want us to monitor drift on
    // eg., specific column values or specific invariants
    // shopify_txns = IF(merchant = 'shopify', txn_amount, NULL)
    // likes_over_dislines = IF(dislikes > likes, 1, 0)
    // or any other expression that you care about
    2: optional map<string, string> derivations

    // we measure the unique counts of the columns and decide if they are categorical and numeric
    // you can use this to override that decision by setting cardinality hints
    3: optional map<string, Cardinality> columnCardinalityHints

    4: optional common.Window tileSize

    // the current tile summary will be compared with older summaries using the metric
    // if the drift is more than the threshold, we will raise an alert
    5: optional list<common.Window> lookbackWindows

    // default drift metric to use
    6: optional DriftMetric driftMetric = DriftMetric.JENSEN_SHANNON
}

struct JoinDriftRequest {
    1: required string name
    2: required i64 startTs
    3: required i64 endTs
    6: optional string offset // Format: "24h" or "7d"
    7: optional DriftMetric algorithm
    8: optional string columnName
}

struct JoinDriftResponse {
    1: required list<TileDriftSeries> driftSeries
}

struct JoinSummaryRequest {
    1: required string name
    2: required i64 startTs
    3: required i64 endTs
    5: optional string percentiles  // Format: "p5,p50,p95"
    8: required string columnName
}

struct MetricsRequest {
    1: required string confName
    2: required i64 startTs
    3: required i64 endTs
}

struct MetricsResponse {
    1: required TileSummarySeries series
}
