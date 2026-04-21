"""Cloud-native partition presence checks via raw CLI tools.

Each helper shells out to the vendor's data-plane CLI (``bq`` / ``aws athena``
/ ``snow``) and asserts that every expected date has at least one row on the
target table. Preferred over the zipline ``check-partitions`` subcommand when
the test wants an external, CLI-level probe rather than a round-trip through
zipline.

Assumes the caller has active cloud credentials — the same credentials the
rest of the integration suite already relies on.
"""

import json
import os
import subprocess
import time
from typing import Iterable


def _run(cmd: list[str], timeout: int = 120) -> str:
    """Run a subprocess and return stdout. Raises on non-zero exit."""
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result.stdout


# ---------------------------------------------------------------------------
# GCP — bq query
# ---------------------------------------------------------------------------

def _bq_partition_exists(project: str, table: str, ds: str) -> bool:
    query = f"SELECT 1 FROM `{project}.{table}` WHERE ds = '{ds}' LIMIT 1"
    stdout = _run(
        [
            "bq", "query",
            "--project_id", project,
            "--use_legacy_sql=false",
            "--format=json",
            "--max_rows=1",
            query,
        ]
    )
    rows = json.loads(stdout) if stdout.strip() else []
    return len(rows) > 0


def assert_gcp_partitions_exist(project: str, table: str, dates: Iterable[str]) -> None:
    missing = [ds for ds in dates if not _bq_partition_exists(project, table, ds)]
    if missing:
        raise AssertionError(f"Missing partitions on {project}.{table}: {missing}")


# ---------------------------------------------------------------------------
# AWS — aws athena
# ---------------------------------------------------------------------------

def _athena_start(query: str, workgroup: str, output_loc: str, region: str) -> str:
    stdout = _run(
        [
            "aws", "athena", "start-query-execution",
            "--query-string", query,
            "--work-group", workgroup,
            "--result-configuration", f"OutputLocation={output_loc}",
            "--region", region,
            "--output", "json",
        ]
    )
    return json.loads(stdout)["QueryExecutionId"]


def _athena_wait(execution_id: str, region: str, timeout: int = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        stdout = _run(
            [
                "aws", "athena", "get-query-execution",
                "--query-execution-id", execution_id,
                "--region", region,
                "--output", "json",
            ]
        )
        state = json.loads(stdout)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = json.loads(stdout)["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {execution_id} ended in {state}: {reason}")
        time.sleep(2)
    raise TimeoutError(f"Athena query {execution_id} did not complete within {timeout}s")


def _athena_has_rows(execution_id: str, region: str) -> bool:
    stdout = _run(
        [
            "aws", "athena", "get-query-results",
            "--query-execution-id", execution_id,
            "--max-items", "2",
            "--region", region,
            "--output", "json",
        ]
    )
    rows = json.loads(stdout).get("ResultSet", {}).get("Rows", [])
    # First row is the header; presence of >= 1 data row ⇒ partition has data.
    return len(rows) >= 2


def _athena_partition_exists(
    database: str, table: str, ds: str, workgroup: str, output_loc: str, region: str
) -> bool:
    query = f'SELECT 1 FROM "{database}"."{table}" WHERE ds = \'{ds}\' LIMIT 1'
    execution_id = _athena_start(query, workgroup, output_loc, region)
    _athena_wait(execution_id, region)
    return _athena_has_rows(execution_id, region)


def assert_aws_partitions_exist(
    database: str, table: str, dates: Iterable[str],
    workgroup: str, output_loc: str, region: str,
) -> None:
    missing = [
        ds for ds in dates
        if not _athena_partition_exists(database, table, ds, workgroup, output_loc, region)
    ]
    if missing:
        raise AssertionError(f"Missing partitions on {database}.{table}: {missing}")


# ---------------------------------------------------------------------------
# Azure — snow sql (Snowflake CLI)
# ---------------------------------------------------------------------------

def _snow_partition_exists(
    connection: str, database: str, schema: str, table: str, ds: str
) -> bool:
    query = f'SELECT 1 FROM "{database}"."{schema}"."{table}" WHERE ds = \'{ds}\' LIMIT 1'
    cmd = ["snow", "sql", "-q", query, "--format", "json"]
    if connection:
        cmd.extend(["--connection", connection])
    stdout = _run(cmd)
    # `snow sql --format json` returns a list of result rows (empty if no match).
    try:
        rows = json.loads(stdout) if stdout.strip() else []
    except json.JSONDecodeError:
        # Some snow versions wrap in {"result": [...]}; fall back.
        parsed = json.loads(stdout)
        rows = parsed.get("result", []) if isinstance(parsed, dict) else parsed
    return len(rows) > 0


def assert_azure_partitions_exist(
    connection: str, database: str, schema: str, table: str, dates: Iterable[str],
) -> None:
    missing = [
        ds for ds in dates
        if not _snow_partition_exists(connection, database, schema, table, ds)
    ]
    if missing:
        raise AssertionError(
            f"Missing partitions on {database}.{schema}.{table}: {missing}"
        )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def assert_partitions_exist(cloud: str, table: str, dates: Iterable[str]) -> None:
    """Dispatch to the per-cloud partition check using canary-tuned defaults.

    Config via env vars (all have canary defaults):
      GCP   — GCP_BQ_PROJECT
      AWS   — AWS_GLUE_DATABASE, AWS_ATHENA_WORKGROUP,
              AWS_ATHENA_RESULT_LOCATION, AWS_REGION
      Azure — SNOWFLAKE_CONNECTION, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
    """
    if cloud == "gcp":
        project = os.environ.get("GCP_BQ_PROJECT", "canary-443022")
        # table arrives as "data.<name>" — split for the bq fully-qualified form.
        assert_gcp_partitions_exist(project, table, dates)

    elif cloud == "aws":
        database = os.environ.get("AWS_GLUE_DATABASE", "default")
        workgroup = os.environ.get("AWS_ATHENA_WORKGROUP", "primary")
        output_loc = os.environ.get(
            "AWS_ATHENA_RESULT_LOCATION",
            "s3://zipline-warehouse-canary/athena-results/",
        )
        region = os.environ.get("AWS_REGION", "us-west-2")
        # AWS tables arrive as "data.<name>"; Glue database override via env.
        # Keep the caller's full "data.<name>" for logging, but query against
        # the Glue database env var + bare table name.
        bare_table = table.split(".", 1)[-1]
        assert_aws_partitions_exist(database, bare_table, dates, workgroup, output_loc, region)

    elif cloud == "azure":
        connection = os.environ.get("SNOWFLAKE_CONNECTION", "")
        database = os.environ.get("SNOWFLAKE_DATABASE", "CHRONON")
        schema = os.environ.get("SNOWFLAKE_SCHEMA", "data")
        bare_table = table.split(".", 1)[-1]
        assert_azure_partitions_exist(connection, database, schema, bare_table, dates)

    else:
        raise ValueError(f"Unknown cloud: {cloud!r}")
