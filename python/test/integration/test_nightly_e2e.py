"""Nightly end-to-end integration test.

Exercises the full Zipline pipeline against freshly-provisioned AWS infrastructure:
  1. Compile canary configs
  2. Backfill staging queries to seed data
  3. Backfill GroupBy
  4. Backfill Join (chained after GroupBy)
  5. Verify output tables exist with expected row counts
  6. Cleanup test-specific Glue tables

This test is designed to run in the nightly_e2e GitHub Actions workflow against
infrastructure provisioned from scratch by Terraform.

Usage::

    HUB_URL=http://... CLOUD=aws VERSION=0.9.7 WAREHOUSE_BUCKET=... \\
        python -m pytest test_nightly_e2e.py -v -s --log-cli-level=INFO
"""

import logging
import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.expectations import AthenaValidator
from .helpers.workflow import poll_workflow

logger = logging.getLogger(__name__)

# AWS-specific conf paths used in the ordered test flow.
STAGING_QUERY_USER_ACTIVITIES = "compiled/staging_queries/aws/exports.user_activities__0"
STAGING_QUERY_CHECKOUTS = "compiled/staging_queries/aws/exports.checkouts__0"
STAGING_QUERY_DIM_LISTINGS = "compiled/staging_queries/aws/exports.dim_listings__0"
STAGING_QUERY_DIM_MERCHANTS = "compiled/staging_queries/aws/exports.dim_merchants__0"
GROUP_BY_USER_ACTIVITIES = "compiled/group_bys/aws/user_activities.v1__1"
JOIN_DEMO = "compiled/joins/aws/demo.v1__1"
JOIN_DEMO_DERIVATIONS = "compiled/joins/aws/demo.derivations_v1__2"

# Backfill date range
START_DS = "2025-08-01"
END_DS = "2025-08-01"


@pytest.fixture(scope="module")
def runner():
    return CliRunner()


@pytest.fixture(scope="module")
def warehouse_bucket():
    bucket = os.environ.get("WAREHOUSE_BUCKET")
    assert bucket, "WAREHOUSE_BUCKET env var is required for nightly E2E tests"
    return bucket


@pytest.fixture(scope="module")
def validator(warehouse_bucket):
    database = os.environ.get("AWS_GLUE_DATABASE", "default")
    return AthenaValidator(database=database, warehouse_bucket=warehouse_bucket)


@pytest.mark.nightly
class TestNightlyE2E:
    """Ordered end-to-end test that exercises the full Zipline batch pipeline."""

    def test_01_compile(self, runner, confs, chronon_root):
        """Compile all canary configs from scratch."""
        compile_configs(runner, chronon_root)

    def test_02_seed_staging_queries(self, runner, confs, chronon_root, hub_url):
        """Submit staging query backfills to seed source data into Glue tables."""
        staging_confs = [
            STAGING_QUERY_USER_ACTIVITIES,
            STAGING_QUERY_CHECKOUTS,
            STAGING_QUERY_DIM_LISTINGS,
            STAGING_QUERY_DIM_MERCHANTS,
        ]
        workflow_ids = []
        for conf_key in staging_confs:
            conf_path = confs[conf_key]
            logger.info("Submitting staging query backfill: %s", conf_key)
            wf_id = submit_backfill(
                runner, chronon_root, hub_url, conf_path, START_DS, END_DS,
            )
            workflow_ids.append((conf_key, wf_id))

        for conf_key, wf_id in workflow_ids:
            logger.info("Polling staging query %s (workflow %s)", conf_key, wf_id)
            poll_workflow(hub_url, wf_id, timeout=1800, interval=30)

    def test_03_backfill_group_by(self, runner, confs, chronon_root, hub_url):
        """Backfill the GroupBy, which aggregates over the seeded staging data."""
        conf_path = confs[GROUP_BY_USER_ACTIVITIES]
        logger.info("Submitting GroupBy backfill: %s", GROUP_BY_USER_ACTIVITIES)
        wf_id = submit_backfill(
            runner, chronon_root, hub_url, conf_path, START_DS, END_DS,
        )
        poll_workflow(hub_url, wf_id, timeout=1800, interval=30)

    def test_04_backfill_join(self, runner, confs, chronon_root, hub_url):
        """Backfill the Join (chained after GroupBy) — the core feature table."""
        conf_path = confs[JOIN_DEMO]
        logger.info("Submitting Join backfill: %s", JOIN_DEMO)
        wf_id = submit_backfill(
            runner, chronon_root, hub_url, conf_path, START_DS, END_DS,
        )
        poll_workflow(hub_url, wf_id, timeout=1800, interval=30)

    def test_05_backfill_join_derivations(self, runner, confs, chronon_root, hub_url):
        """Backfill the Join with derivations."""
        conf_path = confs[JOIN_DEMO_DERIVATIONS]
        logger.info("Submitting Join derivations backfill: %s", JOIN_DEMO_DERIVATIONS)
        wf_id = submit_backfill(
            runner, chronon_root, hub_url, conf_path, START_DS, END_DS,
        )
        poll_workflow(hub_url, wf_id, timeout=1800, interval=30)

    def test_06_verify_outputs(self, confs, validator):
        """Verify that output tables exist and have rows."""
        # The test_id is embedded in the table names via the confs fixture.
        # We check that the join output table was populated.
        join_conf = confs[JOIN_DEMO]
        # Table name is derived from the conf name (dots replaced with underscores)
        table_name = join_conf.replace("/", "_").replace(".", "_")
        logger.info("Verifying output table: %s", table_name)
        validator.assert_table_has_rows(table_name, min_rows=1)
