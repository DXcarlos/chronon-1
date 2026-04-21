"""Cloud-agnostic hub backfill integration tests.

Exercises: compile -> upload diffs -> backfill -> poll workflow to success.
Replaces the former test_gcp_hub_quickstart.py and test_aws_hub_quickstart.py.
"""

from datetime import date, timedelta
from urllib.parse import quote

import pytest
import requests
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.hub_api import _get_auth_headers
from .helpers.workflow import poll_workflow


# NodeRunStatus.SUCCEEDED = 3 in orchestration.thrift (platform repo). The Hub
# serializes thrift enums via Jackson, which may emit either the int value or
# the name depending on module config — guard against both. The orchestration
# thrifts aren't generated into this Python repo, so we can't reference an
# enum symbol directly.
_SUCCEEDED_STATUS = {3, "SUCCEEDED"}


def _expand_range(start: str, end: str) -> list[str]:
    """Inclusive daily expansion of a YYYY-MM-DD range."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _succeeded_partitions_by_table(hub_url: str, workflow_id: str) -> dict[str, set[str]]:
    """Return ``{outputTable -> {ds, ...}}`` from successful stepRuns in the workflow.

    Reads the workflow's conf/mode/range via ``GET /workflow/v2/<id>`` then
    enumerates ``nodeExecutions`` + ``stepRuns`` via
    ``GET /confs/v2/<conf>/status/<mode>``. Cross-cloud by construction — the
    Hub is the source of truth, no data-plane CLI needed.
    """
    headers = _get_auth_headers()

    wf_resp = requests.get(f"{hub_url}/workflow/v2/{workflow_id}", headers=headers)
    wf_resp.raise_for_status()
    workflow = wf_resp.json().get("workflow", wf_resp.json())

    status_resp = requests.get(
        f"{hub_url}/confs/v2/{quote(workflow['confName'], safe='')}/status/"
        f"{quote(workflow['mode'], safe='')}",
        params={
            "start": workflow["startPartition"],
            "end": workflow["endPartition"],
            "workflowId": workflow_id,
        },
        headers=headers,
    )
    status_resp.raise_for_status()

    result: dict[str, set[str]] = {}
    for node in status_resp.json().get("nodeExecutions", []):
        output_table = node.get("outputTable")
        if not output_table:
            continue
        partitions: set[str] = set()
        for step in node.get("stepRuns", []):
            if step.get("status") not in _SUCCEEDED_STATUS:
                continue
            s, e = step.get("startPartition"), step.get("endPartition")
            if s and e:
                partitions.update(_expand_range(s, e))
        result[output_table] = partitions
    return result

# Demo join conf paths differ across clouds (variable names / versions vary).
DEMO_DERIVATIONS = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}


@pytest.mark.integration
def test_backfill_no_data(confs, chronon_root, hub_url, cloud):
    """Backfill with dates that have no input data should result in a failed workflow."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs(DEMO_DERIVATIONS[cloud]), "1969-01-01", "1969-01-01",
    )
    with pytest.raises(RuntimeError, match="ended with status FAILED"):
        poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)


# Conf for multi-day backfill that expects success.
# GCP/AWS: join derivation (exercises full multi-step DAG).
# Azure: staging query (user_activities/checkouts not yet seeded in Snowflake canary).
MULTIDAY_BACKFILL = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/staging_queries/azure/exports.dim_listings__0",
}


@pytest.mark.integration
def test_backfill_multiday(confs, chronon_root, hub_url, cloud):
    """Multi-day backfill exercises multi-step allocation."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs(MULTIDAY_BACKFILL[cloud]),
        "2026-03-01", "2026-03-03",
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)


@pytest.mark.integration
def test_backfill_start_cutoff_enforcement(
    test_id, confs, chronon_root, hub_url, cloud
):
    """TableDependency.start_cutoff is enforced end-to-end by the orchestrator.

    Fixture: python/test/canary/staging_queries/<cloud>/cutoff_example.py
      - downstream depends on export_a with plain offset=0
      - downstream depends on export_b with start_cutoff="2026-02-25"

    Backfilling downstream for [2026-03-01, 2026-03-03] requires:
      - export_a partitions for 3 days (the backfill range) — plain offset=0.
      - export_b partitions for 7 days (2026-02-25..2026-03-03) — the platform
        orchestrator expands the dep range to [start_cutoff, query_end] and
        demands every date in that range be Filled.
      - downstream partitions for the 3 backfill-range days.

    The 4 pre-backfill days on export_b (2026-02-25..02-28) are the load-bearing
    assertion — they prove start_cutoff actually drove the scheduler. If the
    cutoff were ignored, export_b would only have been scheduled for the 3
    backfill-range days.

    Partition coverage is read back from the Hub's own ``/confs/v2/.../status/...``
    endpoint — the orchestrator's ledger of which step ran for which partitions
    is cross-cloud by construction, so no per-cloud data-plane CLI is needed.
    """
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds, end_ds = "2026-03-01", "2026-03-03"
    downstream_conf = confs(f"compiled/staging_queries/{cloud}/cutoff_example.downstream__0")
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url, downstream_conf, start_ds, end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)

    backfill_range = {"2026-03-01", "2026-03-02", "2026-03-03"}
    cutoff_expanded = backfill_range | {
        "2026-02-25", "2026-02-26", "2026-02-27", "2026-02-28",
    }

    partitions_by_table = _succeeded_partitions_by_table(hub_url, workflow_id)
    tbl = lambda name: f"data.{cloud}_cutoff_example_{test_id}_{name}__0"

    assert backfill_range.issubset(partitions_by_table.get(tbl("downstream"), set())), (
        f"downstream missing partitions; got {partitions_by_table.get(tbl('downstream'))}"
    )
    assert backfill_range.issubset(partitions_by_table.get(tbl("export_a"), set())), (
        f"export_a missing partitions; got {partitions_by_table.get(tbl('export_a'))}"
    )
    # Load-bearing: the 4 pre-backfill days (2026-02-25..02-28) prove start_cutoff
    # drove the orchestrator to schedule export_b outside the downstream's range.
    assert cutoff_expanded.issubset(partitions_by_table.get(tbl("export_b"), set())), (
        f"export_b missing start_cutoff-expanded partitions; got "
        f"{partitions_by_table.get(tbl('export_b'))}"
    )
