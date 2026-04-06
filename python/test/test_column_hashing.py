"""Tests for semantic hash computation in column_hashing.py."""

import pytest

import gen_thrift.api.ttypes as api
import gen_thrift.common.ttypes as common
from ai.chronon.cli.compile.column_hashing import (
    compute_group_by_columns_hashes,
    compute_join_column_hashes,
)


def _make_event_source(table, selects, time_column="ts_col"):
    """Helper to build a Source wrapping an EventSource with the given table and selects."""
    return api.Source(
        events=api.EventSource(
            table=table,
            query=api.Query(selects=selects, timeColumn=time_column),
        )
    )


def _make_group_by(
    name,
    table,
    keys,
    selects,
    aggregations=None,
    team="test_team",
    version=0,
    derivations=None,
    description=None,
):
    """Build a minimal GroupBy thrift object suitable for hashing."""
    source = _make_event_source(table, selects)
    meta = api.MetaData(name=name, team=team, version=version)
    if description:
        meta.description = description
    return api.GroupBy(
        sources=[source],
        keyColumns=keys,
        aggregations=aggregations,
        metaData=meta,
        derivations=derivations,
    )


def _simple_aggregation(input_column, operation=api.Operation.SUM, windows=None):
    return api.Aggregation(
        inputColumn=input_column,
        operation=operation,
        argMap={},
        windows=windows,
    )


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_groupby_hash_deterministic():
    """Computing the hash twice for the same GroupBy yields identical results."""
    gb = _make_group_by(
        name="test_team.my_group_by__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "amount": "amount"},
        aggregations=[_simple_aggregation("amount")],
    )
    hash1 = compute_group_by_columns_hashes(gb)
    hash2 = compute_group_by_columns_hashes(gb)
    assert hash1 == hash2


# ---------------------------------------------------------------------------
# Sensitivity to semantic changes
# ---------------------------------------------------------------------------


def test_groupby_hash_changes_on_aggregation_add():
    """Adding an aggregation changes the set of output column hashes."""
    gb1 = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "amount": "amount", "count_col": "count_col"},
        aggregations=[_simple_aggregation("amount")],
    )
    gb2 = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "amount": "amount", "count_col": "count_col"},
        aggregations=[
            _simple_aggregation("amount"),
            _simple_aggregation("count_col", operation=api.Operation.COUNT),
        ],
    )
    h1 = compute_group_by_columns_hashes(gb1)
    h2 = compute_group_by_columns_hashes(gb2)
    # gb2 should have an extra column hash that gb1 does not
    assert set(h1.keys()) != set(h2.keys())


def test_groupby_hash_changes_on_source_table():
    """Changing the source table name produces different hashes."""
    common_kwargs = dict(
        name="test_team.gb__0",
        keys=["user_id"],
        selects={"user_id": "user_id", "amount": "amount"},
        aggregations=[_simple_aggregation("amount")],
    )
    gb_a = _make_group_by(table="db.events_v1", **common_kwargs)
    gb_b = _make_group_by(table="db.events_v2", **common_kwargs)

    ha = compute_group_by_columns_hashes(gb_a)
    hb = compute_group_by_columns_hashes(gb_b)
    # The hash for the same output column should differ
    assert ha["amount_sum"] != hb["amount_sum"]


def test_groupby_hash_stable_on_metadata_change():
    """Changing metadata like team or description does not change hashes (metadata is not semantic)."""
    gb1 = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "val": "val"},
        aggregations=[_simple_aggregation("val")],
        team="alpha_team",
        description="original description",
    )
    gb2 = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "val": "val"},
        aggregations=[_simple_aggregation("val")],
        team="beta_team",
        description="completely different description",
    )
    assert compute_group_by_columns_hashes(gb1) == compute_group_by_columns_hashes(gb2)


# ---------------------------------------------------------------------------
# Join hashing
# ---------------------------------------------------------------------------


def _make_join(
    name,
    left_table,
    left_selects,
    join_parts,
    derivations=None,
    use_long_names=False,
):
    left = _make_event_source(left_table, left_selects)
    meta = api.MetaData(name=name, version=0)
    return api.Join(
        left=left,
        joinParts=join_parts,
        metaData=meta,
        derivations=derivations,
        useLongNames=use_long_names,
    )


def test_join_hash_includes_all_parts():
    """Join hash output includes columns from all JoinParts."""
    gb1 = _make_group_by(
        name="test_team.gb1__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "a": "a"},
        aggregations=[_simple_aggregation("a")],
    )
    gb2 = _make_group_by(
        name="test_team.gb2__0",
        table="db.events2",
        keys=["user_id"],
        selects={"user_id": "user_id", "b": "b"},
        aggregations=[_simple_aggregation("b", operation=api.Operation.MAX)],
    )
    jp1 = api.JoinPart(groupBy=gb1)
    jp2 = api.JoinPart(groupBy=gb2)
    join = _make_join(
        name="test_team.my_join__0",
        left_table="db.left_table",
        left_selects={"user_id": "user_id"},
        join_parts=[jp1, jp2],
    )
    hashes = compute_join_column_hashes(join)
    # Should contain columns from both group bys
    col_names = list(hashes.keys())
    has_gb1_col = any("a_sum" in c for c in col_names)
    has_gb2_col = any("b_max" in c for c in col_names)
    assert has_gb1_col, f"Expected column from gb1 in {col_names}"
    assert has_gb2_col, f"Expected column from gb2 in {col_names}"


def test_join_hash_with_prefix():
    """Adding a prefix to a JoinPart changes the output column names (and therefore the hash keys)."""
    gb = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "val": "val"},
        aggregations=[_simple_aggregation("val")],
    )
    jp_no_prefix = api.JoinPart(groupBy=gb)
    jp_with_prefix = api.JoinPart(groupBy=gb, prefix="seller")

    join_no = _make_join(
        name="test_team.j__0",
        left_table="db.left",
        left_selects={"user_id": "user_id"},
        join_parts=[jp_no_prefix],
    )
    join_yes = _make_join(
        name="test_team.j__0",
        left_table="db.left",
        left_selects={"user_id": "user_id"},
        join_parts=[jp_with_prefix],
    )
    h_no = compute_join_column_hashes(join_no)
    h_yes = compute_join_column_hashes(join_yes)
    assert set(h_no.keys()) != set(h_yes.keys()), "Prefix should change output column names"


def test_join_hash_with_derivations():
    """Adding a derivation to a Join changes the hash output."""
    gb = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "val": "val"},
        aggregations=[_simple_aggregation("val")],
    )
    jp = api.JoinPart(groupBy=gb)

    join_base = _make_join(
        name="test_team.j__0",
        left_table="db.left",
        left_selects={"user_id": "user_id"},
        join_parts=[jp],
    )
    join_derived = _make_join(
        name="test_team.j__0",
        left_table="db.left",
        left_selects={"user_id": "user_id"},
        join_parts=[jp],
        derivations=[api.Derivation(name="double_val", expression="user_id_val_sum * 2")],
    )
    h_base = compute_join_column_hashes(join_base)
    h_derived = compute_join_column_hashes(join_derived)
    assert h_base != h_derived


def test_useLongNames_affects_feature_names():
    """useLongNames=True produces different column names than False."""
    gb = _make_group_by(
        name="test_team.gb__0",
        table="db.events",
        keys=["user_id"],
        selects={"user_id": "user_id", "val": "val"},
        aggregations=[_simple_aggregation("val")],
    )
    jp = api.JoinPart(groupBy=gb)

    join_short = _make_join(
        name="test_team.j__0",
        left_table="db.left",
        left_selects={"user_id": "user_id"},
        join_parts=[jp],
        use_long_names=False,
    )
    join_long = _make_join(
        name="test_team.j__0",
        left_table="db.left",
        left_selects={"user_id": "user_id"},
        join_parts=[jp],
        use_long_names=True,
    )
    h_short = compute_join_column_hashes(join_short)
    h_long = compute_join_column_hashes(join_long)
    assert set(h_short.keys()) != set(h_long.keys()), (
        f"useLongNames should change column names: short={set(h_short.keys())}, long={set(h_long.keys())}"
    )
