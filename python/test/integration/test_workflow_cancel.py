"""Integration tests for workflow cancellation.

Exercises cancel of a running backfill and verifies status endpoints remain healthy
after cancellation.
"""

import requests

import pytest
from click.testing import CliRunner

from .helpers.cli import cancel_workflow, compile_configs, submit_backfill
from .helpers.workflow import poll_workflow_until, _get_auth_headers

DEMO_DERIVATIONS = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}


@pytest.mark.integration
def test_cancel_running_backfill(confs, chronon_root, hub_url, cloud):
    """Cancel a running workflow -- should transition to CANCELLED."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_path = confs[DEMO_DERIVATIONS[cloud]]

    # Large date range to ensure the workflow stays RUNNING long enough to cancel
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        conf_path, "2026-01-10", "2026-02-28",
    )

    # Wait until the workflow is actually running
    poll_workflow_until(
        hub_url, workflow_id,
        target_statuses={"RUNNING"},
        timeout=600,
        interval=15,
    )

    cancel_workflow(runner, chronon_root, hub_url, workflow_id, cloud)

    # Poll until terminal -- should be CANCELLED, not FAILED
    data = poll_workflow_until(
        hub_url, workflow_id,
        target_statuses={"CANCELLED"},
        timeout=600,
        interval=15,
    )

    workflow = data.get("workflow", data)
    from .helpers.workflow import WORKFLOW_STATUS
    status_name = WORKFLOW_STATUS.get(workflow.get("status"), "INVALID")
    assert status_name == "CANCELLED", f"Expected CANCELLED, got {status_name}"


@pytest.mark.integration
def test_cancel_status_no_500(confs, chronon_root, hub_url, cloud):
    """After cancellation, workflow status check should return valid response (not 500)."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_path = confs[DEMO_DERIVATIONS[cloud]]

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        conf_path, "2026-01-10", "2026-02-28",
    )

    poll_workflow_until(
        hub_url, workflow_id,
        target_statuses={"RUNNING"},
        timeout=600,
        interval=15,
    )

    cancel_workflow(runner, chronon_root, hub_url, workflow_id, cloud)

    poll_workflow_until(
        hub_url, workflow_id,
        target_statuses={"CANCELLED"},
        timeout=600,
        interval=15,
    )

    # After confirmed cancellation, hit the status endpoint multiple times
    # to verify it returns 200 consistently (no 500 errors)
    url = f"{hub_url}/workflow/v2/{workflow_id}"
    for i in range(3):
        headers = _get_auth_headers()
        resp = requests.get(url, headers=headers)
        assert resp.status_code == 200, (
            f"Status check {i+1}/3 returned {resp.status_code} after cancellation: "
            f"{resp.text}"
        )
        data = resp.json()
        assert "workflow" in data or "status" in data, (
            f"Status check {i+1}/3 returned unexpected body: {data}"
        )
