"""Hub backfill against Crucible cloud warehouse canary confs.

Exercises the same Crucible Hub backfill flow for AWS and Azure. The cloud
specific confs point at the warehouse-backed data source for each deployment:
Databricks Unity Catalog on AWS, Snowflake on Azure.
"""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

# aws_databricks ships canary EMR Serverless wiring in teams.py (PROD compile,
# `compiled/`) and crucible-aws K8sSubmitter overrides in teams.canary.py (canary
# compile, `compiled_canary/`). The deploying CI picks which set is read by
# setting ZIPLINE_COMPILE_ENV. Default "prod" keeps update_canary.yaml untouched.
_COMPILE_ENV = os.environ.get("ZIPLINE_COMPILE_ENV", "prod").strip() or "prod"
_COMPILED_DIR = "compiled" if _COMPILE_ENV == "prod" else f"compiled_{_COMPILE_ENV}"

CRUCIBLE_BACKFILL_CASES = {
    "aws": [
        (
            f"{_COMPILED_DIR}/joins/aws_databricks/demo.pt_v1",
            "2026-02-01",
            "2026-02-05",
        ),
        (
            f"{_COMPILED_DIR}/joins/aws_databricks/demo.sparse_v1",
            "2026-02-01",
            "2026-02-05",
        ),
        (
            f"{_COMPILED_DIR}/joins/aws_databricks/demo.unpartitioned_v1",
            "2026-02-01",
            "2026-02-05",
        ),
        (
            f"{_COMPILED_DIR}/joins/aws_databricks/demo.unpartitioned_sparse_v1",
            "2026-02-01",
            "2026-02-05",
        ),
    ],
    "azure": [
        (
            f"{_COMPILED_DIR}/joins/azure/crucible_stress.training_set__0",
            "2026-03-01",
            "2026-03-03",
        ),
    ],
}

CRUCIBLE_BACKFILL_CASE_VALUES = [
    (cloud, conf_path, start_ds, end_ds)
    for cloud, cases in CRUCIBLE_BACKFILL_CASES.items()
    for conf_path, start_ds, end_ds in cases
]

CRUCIBLE_BACKFILL_CASE_IDS = [
    f"{cloud}:{conf_path.rsplit('/', 1)[-1]}"
    for cloud, conf_path, _, _ in CRUCIBLE_BACKFILL_CASE_VALUES
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "case_cloud,conf_path,start_ds,end_ds",
    CRUCIBLE_BACKFILL_CASE_VALUES,
    ids=CRUCIBLE_BACKFILL_CASE_IDS,
)
def test_backfill_crucible_demo(
    confs, chronon_root, hub_url, cloud, case_cloud, conf_path, start_ds, end_ds
):
    """Multi-day backfill against the cloud-specific Crucible warehouse conf.

    Uses the ``confs`` fixture so each run hits a fresh test_id-scoped conf
    name. Without this, Hub fast-skips on the second run because the output
    Iceberg partitions for the previous run still exist.
    """
    if cloud != case_cloud:
        pytest.skip(f"Crucible backfill case is {case_cloud}-only; cloud={cloud}")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner,
        chronon_root,
        hub_url,
        confs(conf_path),
        start_ds,
        end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)
