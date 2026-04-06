"""Integration tests for config modification sequences: re-runs, in-place changes, and version bumps.

Exercises idempotent re-submission, compile-time rejection of in-place online config
changes, and version-bumped backfills.
"""

import os
import shutil

import pytest
from click.testing import CliRunner

from ai.chronon.repo.zipline import zipline
from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

DEMO_DERIVATIONS = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}

# training_set is not online, so it can be modified without version-bump restrictions.
# Only available on GCP and Azure (AWS canary doesn't have training_set templates).
TRAINING_SET = {
    "gcp": "compiled/joins/gcp/training_set.v1_test__0",
    "azure": "compiled/joins/azure/training_set.v1_test__0",
}

# The demo.py source files per cloud (relative to chronon_root).
DEMO_SOURCE = {
    "gcp": "joins/gcp/demo.py",
    "aws": "joins/aws/demo.py",
    "azure": "joins/azure/demo.py",
}


@pytest.mark.integration
def test_rerun_same_config(confs, chronon_root, hub_url, cloud):
    """Re-submit same backfill with same range -- should succeed (idempotent)."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_path = confs[DEMO_DERIVATIONS[cloud]]

    # First backfill
    workflow_id_1 = submit_backfill(
        runner, chronon_root, hub_url,
        conf_path, "2026-01-10", "2026-01-12",
    )
    poll_workflow(hub_url, workflow_id_1, timeout=1800, interval=45)

    # Re-submit with identical conf and range
    workflow_id_2 = submit_backfill(
        runner, chronon_root, hub_url,
        conf_path, "2026-01-10", "2026-01-12",
    )
    poll_workflow(hub_url, workflow_id_2, timeout=1800, interval=45)


@pytest.mark.integration
def test_in_place_change_blocked(chronon_root, cloud, test_id):
    """Modify online config without version bump -- compile should fail."""
    runner = CliRunner()

    # Initial compile must succeed
    compile_configs(runner, chronon_root)

    demo_path = os.path.join(chronon_root, DEMO_SOURCE[cloud])
    backup_path = demo_path + ".bak"
    shutil.copy2(demo_path, backup_path)

    try:
        # Append a new derivation to the online join (derivations_v1 / derivations_v3)
        with open(demo_path) as f:
            original = f.read()

        # Find the last Derivation block in the derivations join and add a new one.
        # We insert before the final ']' of the derivations list.
        new_derivation = (
            '        Derivation(\n'
            '            name="test_added_col",\n'
            '            expression="listing_id_price_cents + 1"\n'
            '        ),\n'
        )
        # Replace the last occurrence of the catch-all derivation to add ours before it
        modified = original.replace(
            '        Derivation(\n'
            '            name="*",\n'
            '            expression="*"\n'
            '        )\n',
            new_derivation +
            '        Derivation(\n'
            '            name="*",\n'
            '            expression="*"\n'
            '        )\n',
        )

        assert modified != original, "Failed to modify demo.py -- pattern not found"

        with open(demo_path, "w") as f:
            f.write(modified)

        # Re-compile without cleaning (so previous compiled output is still present)
        result = runner.invoke(
            zipline,
            ["compile", f"--chronon-root={chronon_root}", "--force"],
            catch_exceptions=False,
        )
        # The compiler should reject the in-place change
        assert result.exit_code != 0, (
            f"Expected compile to fail for in-place online config change, "
            f"but it succeeded:\n{result.output}"
        )
        assert "cannot be changed in-place" in result.output.lower() or \
               "in-place" in result.output.lower() or \
               "version" in result.output.lower(), (
            f"Expected error about in-place change, got:\n{result.output}"
        )
    finally:
        # Restore original file
        shutil.copy2(backup_path, demo_path)
        if os.path.exists(backup_path):
            os.remove(backup_path)


@pytest.mark.integration
def test_version_bump_backfill(confs, chronon_root, hub_url, cloud, test_id):
    """Bump version and backfill -- should succeed with new table."""
    if cloud not in TRAINING_SET:
        pytest.skip(f"training_set not configured for cloud={cloud}")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    # Backfill the current version
    conf_path = confs[TRAINING_SET[cloud]]
    workflow_id_1 = submit_backfill(
        runner, chronon_root, hub_url,
        conf_path, "2026-01-10", "2026-01-12",
    )
    poll_workflow(hub_url, workflow_id_1, timeout=1800, interval=45)

    # Bump the version in the source file
    # training_set.py has `version=0` on v1_test -- bump it to 1
    ts_source = os.path.join(chronon_root, f"joins/{cloud}/training_set.py")
    # Use the test_id-renamed file if it exists (template system renames files)
    ts_source_tid = os.path.join(chronon_root, f"joins/{cloud}/training_set_{test_id}.py")
    source_path = ts_source_tid if os.path.exists(ts_source_tid) else ts_source
    backup_path = source_path + ".bak"
    shutil.copy2(source_path, backup_path)

    try:
        with open(source_path) as f:
            content = f.read()

        # Bump version=0 to version=1 only for v1_test Join
        # The v1_test Join is the first Join definition, so replace only the first occurrence
        bumped = content.replace("version=0,", "version=1,", 1)
        assert bumped != content, "Failed to bump version in training_set source"

        with open(source_path, "w") as f:
            f.write(bumped)

        # Recompile (clean to pick up the new version)
        compile_configs(runner, chronon_root, clean=True)

        # The new conf path will have __1 instead of __0
        new_conf_path = conf_path.replace("__0", "__1")

        workflow_id_2 = submit_backfill(
            runner, chronon_root, hub_url,
            new_conf_path, "2026-01-10", "2026-01-12",
        )
        poll_workflow(hub_url, workflow_id_2, timeout=1800, interval=45)
    finally:
        shutil.copy2(backup_path, source_path)
        if os.path.exists(backup_path):
            os.remove(backup_path)
