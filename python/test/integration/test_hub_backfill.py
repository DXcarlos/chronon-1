"""Cloud-agnostic hub backfill integration tests.

Exercises: compile -> upload diffs -> backfill -> poll workflow to success.
Replaces the former test_gcp_hub_quickstart.py and test_aws_hub_quickstart.py.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.partitions import assert_partitions_exist
from .helpers.workflow import poll_workflow

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

    Partition presence is checked via raw cloud CLIs:
      GCP   — ``bq query``
      AWS   — ``aws athena`` (start-query-execution + poll + get-query-results)
      Azure — ``snow sql`` against Snowflake
    """
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds, end_ds = "2026-03-01", "2026-03-03"
    downstream_conf = confs(f"compiled/staging_queries/{cloud}/cutoff_example.downstream__0")
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url, downstream_conf, start_ds, end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)

    backfill_range = ["2026-03-01", "2026-03-02", "2026-03-03"]
    cutoff_expanded = [
        "2026-02-25", "2026-02-26", "2026-02-27", "2026-02-28",
        "2026-03-01", "2026-03-02", "2026-03-03",
    ]

    assert_partitions_exist(
        cloud, f"data.{cloud}_cutoff_example_{test_id}_downstream__0", backfill_range,
    )
    assert_partitions_exist(
        cloud, f"data.{cloud}_cutoff_example_{test_id}_export_a__0", backfill_range,
    )
    # Load-bearing: the 4 pre-backfill days prove start_cutoff drove scheduling.
    assert_partitions_exist(
        cloud, f"data.{cloud}_cutoff_example_{test_id}_export_b__0", cutoff_expanded,
    )
