"""Tests for config construction: GroupBy, Join, StagingQuery, Source."""

from unittest.mock import patch

import pytest

import gen_thrift.api.ttypes as api
import gen_thrift.common.ttypes as common
from ai.chronon.group_by import (
    Aggregation,
    DefaultAggregation,
    Derivation,
    GroupBy,
    Operation,
    TimeUnit,
    Window,
)
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EntitySource, EventSource, JoinSource
from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _event_source(table="db.events", select_map=None, time_column="event_ts"):
    if select_map is None:
        select_map = selects("user_id", "amount")
    return EventSource(table=table, query=Query(selects=select_map, time_column=time_column))


def _entity_source(snapshot_table="db.snapshots", select_map=None):
    if select_map is None:
        select_map = selects("user_id", "status")
    return EntitySource(snapshot_table=snapshot_table, query=Query(selects=select_map))


# ---------------------------------------------------------------------------
# GroupBy construction
# ---------------------------------------------------------------------------


class TestGroupByConstruction:
    def test_groupby_basic_construction(self):
        """Simple GroupBy with EventSource, keys, aggregations creates without error."""
        gb = GroupBy(
            sources=[_event_source()],
            keys=["user_id"],
            aggregations=[Aggregation(input_column="amount", operation=Operation.SUM)],
        )
        assert isinstance(gb, api.GroupBy)
        assert gb.keyColumns == ["user_id"]
        assert len(gb.aggregations) == 1

    def test_aggregation_with_windows(self):
        """Aggregation with multiple window sizes is accepted."""
        agg = Aggregation(
            input_column="amount",
            operation=Operation.SUM,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS)],
        )
        gb = GroupBy(
            sources=[_event_source()],
            keys=["user_id"],
            aggregations=[agg],
        )
        assert len(gb.aggregations[0].windows) == 2

    def test_aggregation_with_string_windows(self):
        """String-format windows like '7d' are normalized."""
        agg = Aggregation(input_column="amount", operation=Operation.SUM, windows=["7d", "30d"])
        gb = GroupBy(
            sources=[_event_source()],
            keys=["user_id"],
            aggregations=[agg],
        )
        assert len(gb.aggregations[0].windows) == 2

    def test_default_aggregation_excludes_keys(self):
        """DefaultAggregation on a source skips key columns and reserved columns."""
        src = _event_source(select_map=selects("user_id", "amount", "score"))
        aggs = DefaultAggregation(
            keys=["user_id"],
            sources=[src],
            operation=Operation.LAST,
        )
        input_cols = [a.inputColumn for a in aggs]
        assert "user_id" not in input_cols
        assert "amount" in input_cols
        assert "score" in input_cols

    def test_groupby_with_derivations(self):
        """GroupBy with Derivation objects creates without error."""
        gb = GroupBy(
            sources=[_event_source()],
            keys=["user_id"],
            aggregations=[Aggregation(input_column="amount", operation=Operation.SUM)],
            derivations=[
                Derivation(name="*", expression="*"),
                Derivation(name="double_amount", expression="amount_sum * 2"),
            ],
        )
        assert gb.derivations is not None
        assert len(gb.derivations) == 2

    def test_groupby_no_aggregations_entity_source(self):
        """GroupBy with aggregations=None is valid for EntitySource without mutations."""
        gb = GroupBy(
            sources=[_entity_source()],
            keys=["user_id"],
            aggregations=None,
        )
        assert gb.aggregations is None


# ---------------------------------------------------------------------------
# JoinPart construction
# ---------------------------------------------------------------------------


class TestJoinPartConstruction:
    def _make_gb(self, keys=None):
        """Create a GroupBy with a pre-set metaData.name to avoid GC module resolution."""
        if keys is None:
            keys = ["user_id"]
        select_map = {k: k for k in keys}
        select_map["amount"] = "amount"
        gb = GroupBy(
            sources=[_event_source(select_map=select_map, time_column="event_ts")],
            keys=keys,
            aggregations=[Aggregation(input_column="amount", operation=Operation.SUM)],
        )
        # Ensure metaData.name is set so JoinPart doesn't need GC module resolution
        gb.metaData.name = "test_team.test_gb__0"
        return gb

    def test_join_part_basic(self):
        """JoinPart with valid GroupBy creates without error."""
        gb = self._make_gb()
        jp = JoinPart(group_by=gb)
        assert isinstance(jp, api.JoinPart)
        assert jp.groupBy is gb

    def test_join_part_key_mapping_valid(self):
        """key_mapping with values matching GroupBy keys is accepted."""
        gb = self._make_gb(keys=["user_id"])
        jp = JoinPart(group_by=gb, key_mapping={"buyer_id": "user_id"})
        assert jp.keyMapping == {"buyer_id": "user_id"}

    def test_join_part_key_mapping_invalid(self):
        """key_mapping with values not in GroupBy keys raises ValueError."""
        gb = self._make_gb(keys=["user_id"])
        with pytest.raises(ValueError, match="Invalid key_mapping"):
            JoinPart(group_by=gb, key_mapping={"buyer_id": "nonexistent_key"})

    def test_join_part_with_prefix(self):
        """JoinPart with prefix set."""
        gb = self._make_gb()
        jp = JoinPart(group_by=gb, prefix="seller")
        assert jp.prefix == "seller"


# ---------------------------------------------------------------------------
# Join construction
# ---------------------------------------------------------------------------


class TestJoinConstruction:
    def _make_join_parts(self):
        gb = GroupBy(
            sources=[_event_source()],
            keys=["user_id"],
            aggregations=[Aggregation(input_column="amount", operation=Operation.SUM)],
        )
        gb.metaData.name = "test_team.test_gb__0"
        return [JoinPart(group_by=gb)]

    def test_join_basic(self):
        """Basic Join construction with left source and right parts."""
        join = Join(
            left=_event_source(),
            right_parts=self._make_join_parts(),
            row_ids=["user_id"],
        )
        assert isinstance(join, api.Join)
        assert len(join.joinParts) == 1

    def test_join_with_derivations(self):
        """Join with derivations creates without error."""
        from ai.chronon.join import Derivation as JoinDerivation

        join = Join(
            left=_event_source(),
            right_parts=self._make_join_parts(),
            row_ids=["user_id"],
            derivations=[JoinDerivation(name="derived_col", expression="user_id_amount_sum * 2")],
        )
        assert join.derivations is not None
        assert len(join.derivations) == 1

    def test_join_row_ids_string_normalized(self):
        """A single string row_id is normalized to a list."""
        join = Join(
            left=_event_source(),
            right_parts=self._make_join_parts(),
            row_ids="user_id",
        )
        assert join.rowIds == ["user_id"]


# ---------------------------------------------------------------------------
# StagingQuery construction
# ---------------------------------------------------------------------------


class TestStagingQueryConstruction:
    def test_staging_query_basic(self):
        """Simple StagingQuery with query and dependencies."""
        sq = StagingQuery(
            query="SELECT * FROM db.my_table WHERE ds = '{{ end_date }}'",
            dependencies=[TableDependency(table="db.my_table", partition_column="ds", offset=0)],
        )
        assert isinstance(sq, api.StagingQuery)
        assert "my_table" in sq.query

    def test_staging_query_template_variables(self):
        """Query with {{ start_date }} and {{ end_date }} is accepted."""
        query_str = (
            "SELECT * FROM db.events "
            "WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'"
        )
        sq = StagingQuery(
            query=query_str,
            dependencies=[TableDependency(table="db.events", partition_column="ds", offset=0)],
        )
        assert "{{ start_date }}" in sq.query
        assert "{{ end_date }}" in sq.query

    def test_staging_query_unparseable_sql(self):
        """Malformed SQL is handled gracefully (no crash) when no tables are detected."""
        # _tables_in_query catches parse errors and returns [], so no dependency check triggers
        sq = StagingQuery(
            query="THIS IS NOT VALID SQL !!@#$%",
        )
        assert isinstance(sq, api.StagingQuery)

    def test_staging_query_engine_types(self):
        """SPARK, BIGQUERY, SNOWFLAKE engine types are all accepted."""
        for engine in [EngineType.SPARK, EngineType.BIGQUERY, EngineType.SNOWFLAKE]:
            sq = StagingQuery(
                query="SELECT 1",
                engine_type=engine,
            )
            assert sq.engineType == engine

    def test_staging_query_with_table_dependency(self):
        """TableDependency objects are converted to thrift properly."""
        dep = TableDependency(table="db.events", partition_column="ds", offset=1)
        sq = StagingQuery(
            query="SELECT * FROM db.events WHERE ds = '{{ end_date }}'",
            dependencies=[dep],
        )
        assert len(sq.tableDependencies) == 1
        assert sq.tableDependencies[0].tableInfo.table == "db.events"


# ---------------------------------------------------------------------------
# Source construction
# ---------------------------------------------------------------------------


class TestSourceConstruction:
    def test_event_source_basic(self):
        """EventSource with table and query returns a Source with events set."""
        src = EventSource(table="db.events", query=Query(selects=selects("user_id", "amount")))
        assert isinstance(src, api.Source)
        assert src.events is not None
        assert src.events.table == "db.events"

    def test_entity_source_basic(self):
        """EntitySource with snapshot_table returns a Source with entities set."""
        src = EntitySource(
            snapshot_table="db.users",
            query=Query(selects=selects("user_id", "status")),
        )
        assert isinstance(src, api.Source)
        assert src.entities is not None
        assert src.entities.snapshotTable == "db.users"

    def test_entity_source_with_mutation(self):
        """EntitySource with mutation_table sets the field correctly."""
        src = EntitySource(
            snapshot_table="db.users",
            query=Query(selects=selects("user_id", "status")),
            mutation_table="db.users_mutations",
        )
        assert src.entities.mutationTable == "db.users_mutations"

    def test_join_source_construction(self):
        """JoinSource wrapping a Join returns a Source with joinSource set."""
        # Build a minimal Join thrift object directly to avoid side effects
        left = _event_source()
        gb = GroupBy(
            sources=[_event_source()],
            keys=["user_id"],
            aggregations=[Aggregation(input_column="amount", operation=Operation.SUM)],
        )
        gb.metaData.name = "test_team.inner_gb__0"
        jp = JoinPart(group_by=gb)
        inner_join = Join(
            left=left,
            right_parts=[jp],
            row_ids=["user_id"],
        )
        inner_join.metaData.name = "test_team.inner_join__0"

        src = JoinSource(
            join=inner_join,
            query=Query(selects=selects("user_id", "amount_sum")),
        )
        assert isinstance(src, api.Source)
        assert src.joinSource is not None
        assert src.joinSource.join is inner_join
