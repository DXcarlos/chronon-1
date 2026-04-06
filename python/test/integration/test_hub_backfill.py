"""Cloud-agnostic hub backfill integration tests.

Exercises: compile -> upload diffs -> backfill -> poll workflow to success.
Tests cover the full chaining DAG, derivations, sparse data, and no-data scenarios.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

STANDARD_RANGE = ("2026-01-10", "2026-01-14")  # 5 days, all clouds
SPARSE_RANGE = ("2026-03-15", "2026-03-25")    # 11 days, AWS only (every-other-day data)

CHAINING_CONF = {
    "gcp": "compiled/joins/gcp/demo_chaining.downstream_join__0",
    "aws": "compiled/joins/aws/demo_chaining.downstream_join__0",
}

DERIVATIONS_CONF = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}

SPARSE_CONF = {
    "aws": "compiled/group_bys/aws/sparse_activities.v1__0",
}


@pytest.mark.integration
def test_chaining_backfill(confs, chronon_root, hub_url, cloud):
    """Full DAG backfill: sensors -> staging queries -> GroupBys -> parent join -> chained GB -> downstream join."""
    if cloud == "azure":
        pytest.skip("USER_ACTIVITIES not seeded in Snowflake")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds, end_ds = STANDARD_RANGE
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs[CHAINING_CONF[cloud]], start_ds, end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=2400, interval=45)


@pytest.mark.integration
def test_derivations_backfill(confs, chronon_root, hub_url, cloud):
    """Derivations backfill reuses upstream staging query data across all clouds."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds, end_ds = STANDARD_RANGE
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs[DERIVATIONS_CONF[cloud]], start_ds, end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=2400, interval=45)


@pytest.mark.integration
def test_sparse_backfill(confs, chronon_root, hub_url, cloud):
    """Sparse data backfill exercises handling of missing partitions in source data."""
    if cloud != "aws":
        pytest.skip("sparse_activities table only exists in AWS")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds, end_ds = SPARSE_RANGE
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs[SPARSE_CONF[cloud]], start_ds, end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=2400, interval=45)


@pytest.mark.integration
def test_backfill_no_data(confs, chronon_root, hub_url, cloud):
    """Backfill with dates that have no input data should result in a failed workflow."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs[DERIVATIONS_CONF[cloud]], "1969-01-01", "1969-01-01",
    )
    with pytest.raises(RuntimeError, match="ended with status FAILED"):
        poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)
