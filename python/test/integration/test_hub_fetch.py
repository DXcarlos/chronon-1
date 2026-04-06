"""Integration tests for zipline hub fetch.

These tests require a running fetcher service. Skip if FETCHER_URL is not set.
The fetcher must have metadata uploaded (via run-adhoc or schedule) for the
confs being fetched.
"""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_hub_fetch

FETCHER_URL = os.environ.get("FETCHER_URL")
skip_no_fetcher = pytest.mark.skipif(
    not FETCHER_URL, reason="FETCHER_URL not set — no fetcher service available"
)

DERIVATIONS_CONF_KEY = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}

GROUP_BY_CONF_KEY = {
    "gcp": "compiled/group_bys/gcp/purchases.v1_test__0",
    "aws": "compiled/group_bys/aws/user_activities.v1__1",
    "azure": "compiled/group_bys/azure/purchases.v1_test__0",
}


@skip_no_fetcher
@pytest.mark.integration
def test_fetch_join_features(confs, chronon_root, cloud):
    """Fetch features for a join conf and verify response contains expected keys."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_key = DERIVATIONS_CONF_KEY.get(cloud)
    conf = confs.get(conf_key)
    if not conf:
        pytest.skip(f"No derivations conf for cloud {cloud}")

    result = submit_hub_fetch(
        runner,
        chronon_root,
        conf,
        fetcher_url=FETCHER_URL,
        keys='{"user_id":"5","listing_id":"1"}',
    )
    assert result.exit_code == 0, f"Fetch failed: {result.output}"
    assert len(result.output) > 10, f"Fetch returned empty output: {result.output}"


@skip_no_fetcher
@pytest.mark.integration
def test_fetch_groupby_features(confs, chronon_root, cloud):
    """Fetch features for a groupby conf."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_key = GROUP_BY_CONF_KEY.get(cloud)
    conf = confs.get(conf_key)
    if not conf:
        pytest.skip(f"No groupby conf for cloud {cloud}")

    result = submit_hub_fetch(
        runner,
        chronon_root,
        conf,
        fetcher_url=FETCHER_URL,
        keys='{"user_id":"5"}',
    )
    assert result.exit_code == 0, f"Fetch failed: {result.output}"


@skip_no_fetcher
@pytest.mark.integration
def test_fetch_schema_only(confs, chronon_root, cloud):
    """Fetch schema for a join conf (no data needed, just metadata)."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_key = DERIVATIONS_CONF_KEY.get(cloud)
    conf = confs.get(conf_key)
    if not conf:
        pytest.skip(f"No conf for cloud {cloud}")

    result = submit_hub_fetch(
        runner,
        chronon_root,
        conf,
        fetcher_url=FETCHER_URL,
        schema=True,
    )
    assert result.exit_code == 0, f"Schema fetch failed: {result.output}"
