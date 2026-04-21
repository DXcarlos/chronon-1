"""Cloud-native partition presence checks via raw CLI tools.

Each helper shells out to the vendor's data-plane CLI (bq / aws / snow) and
asserts that *all* expected dates have at least one row on the target table.
Preferred over the zipline ``check-partitions`` subcommand when the test wants
an external, CLI-level probe rather than a round-trip through zipline.

Assumes the caller has active cloud credentials (ADC for GCP, AWS profile,
Snowflake connection) — the same credentials the integration tests already
rely on.
"""

import json
import subprocess
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


def _bq_partition_exists(project: str, table: str, ds: str) -> bool:
    """Run ``bq query`` to check if the partition ds=<ds> has any rows.

    *table* is the fully-qualified ``dataset.table`` name (e.g. ``data.foo__0``).
    """
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


def assert_gcp_partitions_exist(
    project: str,
    table: str,
    dates: Iterable[str],
) -> None:
    """Assert every date in *dates* has at least one row on the BQ table.

    Raises ``AssertionError`` listing any missing partitions.
    """
    missing = [ds for ds in dates if not _bq_partition_exists(project, table, ds)]
    if missing:
        raise AssertionError(
            f"Missing partitions on {project}.{table}: {missing}"
        )


def assert_partitions_exist(cloud: str, table: str, dates: Iterable[str], **kwargs) -> None:
    """Dispatch to the per-cloud partition check.

    Kwargs:
      project (GCP): BigQuery project id. Defaults to ``canary-443022``.

    AWS + Azure checks are stubbed (skip) until we wire ``aws athena`` and
    ``snow sql`` respectively. File a test-level ``pytest.skip`` in the caller
    for those clouds until then.
    """
    if cloud == "gcp":
        project = kwargs.get("project", "canary-443022")
        assert_gcp_partitions_exist(project, table, dates)
    else:
        raise NotImplementedError(
            f"Partition check for cloud={cloud!r} not yet implemented. "
            "Skip the caller or extend helpers/partitions.py."
        )
