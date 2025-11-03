
# 🧠 Zipline AI: Sample Chronon Project

This repository demonstrates how to author and run [Chronon](https://chronon.ai) pipelines, including GroupBy and Join definitions, using GCP (BigQuery + Iceberg) as the storage backend.

Chronon is a unified platform for **feature engineering**, enabling **online and offline consistency**, **real-time feature generation**, and **historical backfills** from a single codebase.

---

## 📦 Project Structure

```bash
.
├── group_bys/           # GroupBy definitions (feature aggregations)
├── joins/               # Join definitions (how sources and GroupBys are combined)
├── sources/             # Chronon Source definitions (event tables)
├── compiled/            # Generated configs and outputs
├── teams.py             # Chronon Team configurations
└── README.md
```

---

## 🚀 Quick Start

### 🛠️ Requirements

To get started, make sure you have the following set up:

- ✅ **Python** 3.11 or higher
- ✅ **Zipline CLI** — Only for **upgrades or downgrades**, install via:
  ```bash
  ./zipline-cli-install.sh
- ✅ gcloud CLI — authenticated and configured with the correct GCP project
- ✅ Google Cloud credentials — either:
  - Application Default Credentials (ADC)
  - A service account with access to BigQuery and GCS
- ✅Add this to your shell config (e.g., .bashrc, .zshrc):

```bash
# From the same directory as this README
export PYTHONPATH="$(pwd):$PYTHONPATH"
```

---
## Requirements

Teams define metadata, Spark config, and environment variables.

In [teams.py](teams.py), fill in the appropriate values in the TODO section.

Make sure to replace placeholders like `<project-id>` and `<gcs-prefix>` with real values.

### Partition format and column
Chronon expects tables to be date partitioned. Please specify the partition format and the column in teams.py here:

```python
            "spark.chronon.partition.format": "<date-format>", # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>", # ex: "ds",
```

---

## 🧪 Compiling

To generate the user configs from the Python chronon objects to be used in the CLI, run:

```bash
zipline compile
```

This will create a `compiled` directory.

---

## 🧪 Running a GroupBy or Join Backfill

Run a GroupBy backfill from the CLI:

```bash
zipline run \
--mode backfill \
--conf compiled/group_bys/<TEAM_NAME>/<GROUPBY_NAME>
```

Run a Join backfill from the CLI:

```bash
zipline run \
--mode backfill \
--conf compiled/joins/<TEAM_NAME>/<JOIN_NAME>
```

Results are written to the configured BigQuery + Iceberg tables under the `outputNamespace` (e.g. `default.group_by_v1` or `default.v1`).

---

## 🧪 Running a GroupBy upload (GBU) job.

```bash
zipline run \
--mode upload \
--conf compiled/group_bys/<TEAM_NAME>/<GROUP_BY_NAME> \
--ds <DATE>
```

Results are written to the configured BigQuery + Iceberg tables under the `outputNamespace` (e.g. `default.group_by_v1` or `default.v1`).

---

## 🧪 Upload the GBU values to online KV store.

```bash
zipline run \
--mode upload-to-kv \
--conf compiled/group_bys/<TEAM_NAME>/<GROUP_BY_NAME> \
--partition-string <DATE>
```

---

## 🧪 Upload the metadata of Chronon GroupBy or Join to online KV store for serving.

GroupBy metadata upload:
```bash
zipline run \
--mode metadata-upload \
--conf compiled/group_bys/<TEAM_NAME>/<GROUP_BY_NAME>
```

Join metadata upload:
```bash
zipline run \
--mode metadata-upload \
--conf compiled/joins/<TEAM_NAME>/<JOIN_NAME>
```

---

## 🧪 Fetch feature values from Chronon GroupBy or Join.

**Note:** This is only for debugging purposes. Not for production use.

Fetching from a GroupBy:
```bash
zipline run \
--mode fetch \
--conf compiled/group_bys/<TEAM_NAME>/<GROUP_BY_NAME> \
--name <GROUP_BY_NAME> \
-k '{"<ENTITY_KEY>": "<VALUE>"}'
```

Fetching from a Join:
```bash
zipline run \
--mode fetch \
--conf compiled/joins/<TEAM_NAME>/<JOIN_NAME> \
--name <JOIN_NAME> \
-k '{"<ENTITY_KEY>": "<VALUE>"}'
```

---

## 📚 Resources

- [Chronon Docs](https://chronon.ai)
- [GitHub](https://github.com/airbnb/chronon)
- [Community Slack](https://join.slack.com/t/chrononworkspace/shared_invite/zt-33zbnzwac-ghPZXpYNZJsArXZ5WdBy9g)

---

## 👋 About

This project is a reference scaffold for building scalable feature pipelines using Chronon on GCP. It provides end-to-end visibility from source to production features.