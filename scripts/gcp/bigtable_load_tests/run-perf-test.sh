#!/bin/bash

# Run the BigTable bulkPut load test harness.
# Can be run locally (requires gcloud auth and project/instance env vars) or
# on a GCE VM after syncing the repo.
#
# Required env vars:
#   GCP_PROJECT_ID            GCP project containing the BigTable instance
#   GCP_BIGTABLE_INSTANCE_ID  target BigTable instance
#
# Optional env vars:
#   BT_LOAD_NUM_ROWS          row count; default 50000000 (50M)
#   BT_LOAD_BQ_DATASET        BQ dataset for synthetic source table; default chronon_perf_test
#   BT_LOAD_BQ_TABLE          BQ table name; default bt_bulk_load_perf_src
#   BT_LOAD_PARTITION         ds value used in data gen and bulkPut; default 2026-06-11
#   BT_LOAD_DESTINATION_DATASET  destinationOnlineDataSet passed to bulkPut; default bt_load_perf_test
#   BT_LOAD_SKIP_DATA_GEN     set "true" to skip BQ table creation; default false
#   BT_LOAD_UPLOADER          "bigquery" (default) or "spark" (submits Spark job to Dataproc)
#   BT_LOAD_DATAPROC_CLUSTER  Dataproc cluster name (required when BT_LOAD_UPLOADER=spark)
#   BT_LOAD_JAR_URI           GCS URI of the cloud_gcp assembly jar (required when BT_LOAD_UPLOADER=spark)
#   BT_LOAD_EXTRA_JAR_URIS    comma-separated GCS URIs of extra jars, e.g. the spark-bigtable connector

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# --- Validate required env vars -----------------------------------------------
if [ -z "$GCP_PROJECT_ID" ]; then
    log_error "GCP_PROJECT_ID is not set"
    exit 1
fi

if [ -z "$GCP_BIGTABLE_INSTANCE_ID" ]; then
    log_error "GCP_BIGTABLE_INSTANCE_ID is not set"
    exit 1
fi

# --- Defaults -----------------------------------------------------------------
BT_LOAD_NUM_ROWS=${BT_LOAD_NUM_ROWS:-50000000}
BT_LOAD_BQ_DATASET=${BT_LOAD_BQ_DATASET:-chronon_perf_test}
# BT_LOAD_BQ_TABLE intentionally has no default here — the harness derives it from
# BT_LOAD_NUM_ROWS (e.g. bt_bulk_load_perf_src_50M).  Set explicitly to override.
BT_LOAD_PARTITION=${BT_LOAD_PARTITION:-2026-06-11}
BT_LOAD_DESTINATION_DATASET=${BT_LOAD_DESTINATION_DATASET:-bt_load_perf_test}
BT_LOAD_SKIP_DATA_GEN=${BT_LOAD_SKIP_DATA_GEN:-false}
BT_LOAD_UPLOADER=${BT_LOAD_UPLOADER:-bigquery}
# BT_LOAD_DATAPROC_CLUSTER, BT_LOAD_JAR_URI: no defaults — required only when BT_LOAD_UPLOADER=spark.
# BT_BATCH_TABLE_OVERRIDE intentionally has no default — when unset, bulkPut uses GROUPBY_BATCH.

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"

echo ""
echo "==================================================================="
log_info "BigTable bulkPut Load Test"
echo "==================================================================="
log_info "Project:             $GCP_PROJECT_ID"
log_info "BigTable instance:   $GCP_BIGTABLE_INSTANCE_ID"
log_info "Row count:           $BT_LOAD_NUM_ROWS"
log_info "BQ dataset:          $BT_LOAD_BQ_DATASET"
log_info "BQ table:            ${BT_LOAD_BQ_TABLE:-(derived from row count)}"
log_info "Partition (ds):      $BT_LOAD_PARTITION"
log_info "Destination dataset: $BT_LOAD_DESTINATION_DATASET"
log_info "Skip data gen:       $BT_LOAD_SKIP_DATA_GEN"
log_info "Uploader:            $BT_LOAD_UPLOADER"
if [ "$BT_LOAD_UPLOADER" = "spark" ]; then
  log_info "Dataproc cluster:    ${BT_LOAD_DATAPROC_CLUSTER:-(not set)}"
  log_info "Jar URI:             ${BT_LOAD_JAR_URI:-(not set)}"
fi
log_info "BT table override:   ${BT_BATCH_TABLE_OVERRIDE:-(none, uses GROUPBY_BATCH)}"
echo "==================================================================="
echo ""

TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
RESULTS_FILE="bt-load-test-results-${TIMESTAMP}.log"

cd "$REPO_ROOT"

env_args=(
  CHRONON_PERF_TEST_ENABLED=true
  GCP_PROJECT_ID="$GCP_PROJECT_ID"
  GCP_BIGTABLE_INSTANCE_ID="$GCP_BIGTABLE_INSTANCE_ID"
  BT_LOAD_NUM_ROWS="$BT_LOAD_NUM_ROWS"
  BT_LOAD_BQ_DATASET="$BT_LOAD_BQ_DATASET"
  BT_LOAD_PARTITION="$BT_LOAD_PARTITION"
  BT_LOAD_DESTINATION_DATASET="$BT_LOAD_DESTINATION_DATASET"
  BT_LOAD_SKIP_DATA_GEN="$BT_LOAD_SKIP_DATA_GEN"
  BT_LOAD_UPLOADER="$BT_LOAD_UPLOADER"
)
if [ -n "$BT_LOAD_DATAPROC_CLUSTER" ]; then
  env_args+=(BT_LOAD_DATAPROC_CLUSTER="$BT_LOAD_DATAPROC_CLUSTER")
fi
if [ -n "$BT_LOAD_JAR_URI" ]; then
  env_args+=(BT_LOAD_JAR_URI="$BT_LOAD_JAR_URI")
fi
if [ -n "$BT_LOAD_EXTRA_JAR_URIS" ]; then
  env_args+=(BT_LOAD_EXTRA_JAR_URIS="$BT_LOAD_EXTRA_JAR_URIS")
fi
# Only forward BT_LOAD_BQ_TABLE if explicitly set; otherwise let the harness derive it from row count.
if [ -n "$BT_LOAD_BQ_TABLE" ]; then
  env_args+=(BT_LOAD_BQ_TABLE="$BT_LOAD_BQ_TABLE")
fi
# Only forward BT_BATCH_TABLE_OVERRIDE if explicitly set; otherwise bulkPut uses GROUPBY_BATCH.
if [ -n "$BT_BATCH_TABLE_OVERRIDE" ]; then
  env_args+=(BT_BATCH_TABLE_OVERRIDE="$BT_BATCH_TABLE_OVERRIDE")
fi

env "${env_args[@]}" \
./mill cloud_gcp.test.testOnly "ai.chronon.integrations.cloud_gcp.bt_load.BigTableBulkLoadPerfTestHarness" 2>&1 | tee "$RESULTS_FILE"

log_success "Test complete! Full results in: $RESULTS_FILE"

echo ""
echo "==================================================================="
echo "THROUGHPUT RESULT"
echo "==================================================================="
grep "bulkPut complete" "$RESULTS_FILE" || log_warning "Could not find throughput line in results"
echo "==================================================================="
echo ""

log_info "To re-run at a different scale (skipping data gen):"
log_info "  BT_LOAD_NUM_ROWS=100000000 BT_LOAD_SKIP_DATA_GEN=true ./run-perf-test.sh"
