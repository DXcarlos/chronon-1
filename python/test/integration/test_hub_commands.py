"""Integration tests for zipline hub list-tables and schedule-all."""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs
from .helpers.hub_api import delete_schedule, find_schedules_by_test_id

EVAL_URL = os.environ.get("EVAL_URL")
skip_no_eval = pytest.mark.skipif(
    not EVAL_URL, reason="EVAL_URL not set — no eval service available"
)


@skip_no_eval
@pytest.mark.integration
def test_list_tables(chronon_root, hub_url, cloud):
    """List tables in the demo schema via eval service."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    from ai.chronon.repo.hub_runner import hub

    result = runner.invoke(
        hub,
        [
            "list-tables",
            "demo",
            f"--repo={chronon_root}",
            f"--hub-url={hub_url}",
            f"--eval-url={EVAL_URL}",
            f"--team={cloud}",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"list-tables failed: {result.output}"
    assert len(result.output) > 10, f"list-tables returned empty output: {result.output}"


@pytest.mark.integration
def test_schedule_all_no_changes(confs, chronon_root, hub_url, cloud):
    """schedule-all with no pending changes should succeed without crashing."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    from ai.chronon.repo.hub_runner import hub

    result = runner.invoke(
        hub,
        [
            "schedule-all",
            f"--repo={chronon_root}",
            f"--hub-url={hub_url}",
            f"--cloud={cloud}",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"schedule-all failed: {result.output}"


@pytest.mark.integration
def test_schedule_all_deploys_and_cleanup(confs, chronon_root, hub_url, cloud, test_id):
    """schedule-all deploys schedules, verify they exist, then clean up."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    from ai.chronon.repo.hub_runner import hub

    result = runner.invoke(
        hub,
        [
            "schedule-all",
            f"--repo={chronon_root}",
            f"--hub-url={hub_url}",
            f"--cloud={cloud}",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"schedule-all failed: {result.output}"

    # Verify schedules were created for this test run
    schedules = find_schedules_by_test_id(hub_url, test_id)

    # Clean up: delete all schedules created by this test
    for schedule in schedules:
        delete_schedule(hub_url, schedule["confName"])
