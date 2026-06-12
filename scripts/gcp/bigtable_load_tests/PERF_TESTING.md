# BigTable bulkPut Load Test

Load tests the `BigTableKVStoreImpl.bulkPut` path at various row counts (50M, 100M, 200M, 225M).
Two uploaders are supported:

| Uploader | Path | Requires |
|---|---|---|
| `bigquery` (default) | BQ `EXPORT DATA` job streams rows directly into BigTable | BQ reservation in the target region |
| `spark` | Dataproc Spark job via `Spark2BigTableLoader` | Dataproc cluster + assembly jar on GCS |

## What it tests

Two sequential phases:

1. **Data generation** — creates a synthetic BigQuery source table with the schema expected
   by `bulkPut`: `key_bytes BYTES` (~32B, MD5-based, uniformly distributed), `value_bytes BYTES`
   (~2KB), `ds STRING`. Runs entirely inside BigQuery — no local data movement.

2. **bulkPut** — loads the BQ table into BigTable and logs wall-clock duration and rows/s throughput.

---

## BigQuery uploader (default)

### Prerequisites

- `gcloud auth application-default login` (or a service account with BQ + BT IAM roles)
- A BQ reservation assigned to the project in the target region (BQ→BT export requires a reservation;
  on-demand slots are not supported). Set `GCP_LOCATION` to match the region where the reservation
  lives (e.g. `us-central1`).
- A real BigTable instance (BQ EXPORT cannot target the emulator)
- Mill build tool (`./mill` in repo root)

### Running

```bash
cd scripts/gcp/bigtable_load_tests

# 50M rows (default)
GCP_PROJECT_ID=my-project GCP_BIGTABLE_INSTANCE_ID=my-instance GCP_LOCATION=us-central1 ./run-perf-test.sh

# 100M rows
GCP_PROJECT_ID=my-project GCP_BIGTABLE_INSTANCE_ID=my-instance GCP_LOCATION=us-central1 BT_LOAD_NUM_ROWS=100000000 ./run-perf-test.sh

# Reuse existing BQ table (skip data gen)
GCP_PROJECT_ID=my-project GCP_BIGTABLE_INSTANCE_ID=my-instance GCP_LOCATION=us-central1 BT_LOAD_SKIP_DATA_GEN=true ./run-perf-test.sh
```

---

## Spark uploader

Uses `Spark2BigTableLoader` submitted as a Dataproc Spark job. Does not require a BQ reservation.

### Prerequisites

- A running Dataproc cluster in the same region as the BigTable instance
- The `cloud_gcp` assembly jar built and uploaded to GCS
- The `spark-bigtable` connector jar (v0.9.1+) uploaded to GCS separately — it cannot be shaded
  into the assembly because Spark loads it via `ServiceLoader`

### One-time jar setup

```bash
# Build the assembly jar
./mill clean cloud_gcp.assembly

# Upload assembly jar
gsutil cp out/cloud_gcp/assembly.dest/out.jar \
  gs://YOUR_BUCKET/jars/cloud_gcp_lib_deploy.jar

# Download and upload the spark-bigtable connector (0.9.1+)
curl -L -o /tmp/spark-bigtable_2.12-0.9.1.jar \
  https://repo1.maven.org/maven2/com/google/cloud/spark/bigtable/spark-bigtable_2.12/0.9.1/spark-bigtable_2.12-0.9.1.jar

gsutil cp /tmp/spark-bigtable_2.12-0.9.1.jar \
  gs://YOUR_BUCKET/jars/spark-bigtable_2.12-0.9.1.jar
```

Re-upload the assembly jar whenever `Spark2BigTableLoader` or its dependencies change.
The connector jar only needs to be re-uploaded if you upgrade its version.

### Running

```bash
cd scripts/gcp/bigtable_load_tests

GCP_PROJECT_ID=my-project \
GCP_BIGTABLE_INSTANCE_ID=my-instance \
GCP_LOCATION=us-central1 \
BT_LOAD_UPLOADER=spark \
BT_LOAD_DATAPROC_CLUSTER=my-cluster \
BT_LOAD_JAR_URI=gs://YOUR_BUCKET/jars/cloud_gcp_lib_deploy.jar \
BT_LOAD_EXTRA_JAR_URIS=gs://YOUR_BUCKET/jars/spark-bigtable_2.12-0.9.1.jar \
BT_LOAD_NUM_ROWS=100000000 \
./run-perf-test.sh
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GCP_PROJECT_ID` | (required) | GCP project |
| `GCP_BIGTABLE_INSTANCE_ID` | (required) | BigTable instance |
| `GCP_LOCATION` | `us-central1` | Region for BQ jobs and Dataproc; must match the BQ reservation region |
| `BT_LOAD_NUM_ROWS` | `50000000` | Row count — table name is auto-suffixed (e.g. `bt_bulk_load_perf_src_50M`) |
| `BT_LOAD_BQ_DATASET` | `chronon_perf_test` | BQ dataset for the synthetic source table |
| `BT_LOAD_BQ_TABLE` | _(derived from row count)_ | Override the BQ table name |
| `BT_LOAD_PARTITION` | `2026-06-11` | `ds` value written into every row; passed as `partition` to `bulkPut` |
| `BT_LOAD_DESTINATION_DATASET` | `bt_load_perf_test` | `destinationOnlineDataSet` arg to `bulkPut` |
| `BT_LOAD_SKIP_DATA_GEN` | `false` | Skip Phase 1 (reuse existing BQ table) |
| `BT_LOAD_UPLOADER` | `bigquery` | `bigquery` or `spark` |
| `BT_LOAD_DATAPROC_CLUSTER` | — | Dataproc cluster name (required for `spark`) |
| `BT_LOAD_JAR_URI` | — | GCS URI of `cloud_gcp` assembly jar (required for `spark`) |
| `BT_LOAD_EXTRA_JAR_URIS` | — | Comma-separated GCS URIs of extra jars; use for `spark-bigtable` connector |

## Reading results

The script extracts the throughput summary line from the log:

```
bulkPut complete: uploader=bigquery, numRows=50000000, elapsed=423.1s, throughput=118157 rows/s
```

Full logs are written to `bt-load-test-results-YYYYMMDD-HHMMSS.log` in the directory
where the script is run.

## Running directly via mill (without the script)

```bash
# BigQuery uploader
GCP_PROJECT_ID=my-project GCP_BIGTABLE_INSTANCE_ID=my-instance GCP_LOCATION=us-central1 \
BT_LOAD_NUM_ROWS=50000000 CHRONON_PERF_TEST_ENABLED=true \
./mill cloud_gcp.test.testOnly "ai.chronon.integrations.cloud_gcp.bt_load.BigTableBulkLoadPerfTestHarness"

# Spark uploader
GCP_PROJECT_ID=my-project GCP_BIGTABLE_INSTANCE_ID=my-instance GCP_LOCATION=us-central1 \
BT_LOAD_UPLOADER=spark BT_LOAD_DATAPROC_CLUSTER=my-cluster \
BT_LOAD_JAR_URI=gs://YOUR_BUCKET/jars/cloud_gcp_lib_deploy.jar \
BT_LOAD_EXTRA_JAR_URIS=gs://YOUR_BUCKET/jars/spark-bigtable_2.12-0.9.1.jar \
BT_LOAD_NUM_ROWS=100000000 CHRONON_PERF_TEST_ENABLED=true \
./mill cloud_gcp.test.testOnly "ai.chronon.integrations.cloud_gcp.bt_load.BigTableBulkLoadPerfTestHarness"
```
