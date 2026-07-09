import gen_thrift.common.ttypes as common
from gen_thrift.api.ttypes import (
    Aggregation,
    BootstrapPart,
    EventSource,
    ExternalPart,
    GroupBy,
    Join,
    JoinPart,
    JoinSource,
    MetaData,
    Operation,
    Query,
    Source,
    StagingQuery,
)

from ai.chronon.cli.compile.conf_validator import ConfValidator


def _make_group_by(name="team.my_gb", online=False, table="test_table", price_expr="price"):
    return GroupBy(
        sources=[
            Source(
                events=EventSource(
                    table=table,
                    query=Query(
                        selects={"user_id": "user_id", "price": price_expr},
                        timeColumn="timestamp",
                    ),
                )
            )
        ],
        keyColumns=["user_id"],
        aggregations=[
            Aggregation(inputColumn="price", operation=Operation.SUM),
        ],
        metaData=MetaData(name=name, online=online),
    )


def _make_join(name="team.my_join", online=False, group_by=None):
    if group_by is None:
        group_by = _make_group_by()
    return Join(
        left=Source(
            events=EventSource(
                table="left_table",
                query=Query(
                    selects={"user_id": "user_id"},
                    timeColumn="timestamp",
                ),
            )
        ),
        joinParts=[JoinPart(groupBy=group_by)],
        metaData=MetaData(name=name, online=online),
    )


def _make_validator(existing_gbs=None, existing_joins=None, existing_staging_queries=None):
    return ConfValidator(
        input_root="/fake",
        output_root="compiled",
        existing_gbs=existing_gbs or {},
        existing_joins=existing_joins or {},
        existing_staging_queries=existing_staging_queries or {},
    )


def _has_online_in_place_error(errors):
    return any("is online and cannot be changed in-place" in str(e) for e in errors)


def _has_time_partitioned_missing_partition_column_error(errors, context=None):
    message = "timePartitioned sources must have partitionColumn set to the timestamp/date column name"
    return any(message in str(e) and (context is None or context in str(e)) for e in errors)


class TestOnlineConfNotChangedInPlace:
    def test_online_group_by_changed_in_place_is_blocked(self):
        old_gb = _make_group_by(online=True, price_expr="price")
        new_gb = _make_group_by(online=True, price_expr="price * 2")
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert _has_online_in_place_error(errors)

    def test_online_group_by_unchanged_passes(self):
        old_gb = _make_group_by(online=True)
        new_gb = _make_group_by(online=True)
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_offline_group_by_changed_in_place_is_allowed(self):
        old_gb = _make_group_by(online=False, price_expr="price")
        new_gb = _make_group_by(online=False, price_expr="price * 2")
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_new_group_by_no_old_object_passes(self):
        new_gb = _make_group_by(online=True)
        validator = _make_validator()

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_online_join_changed_in_place_is_blocked(self):
        old_join = _make_join(online=True)
        gb = _make_group_by(online=True, table="different_table")
        new_join = _make_join(online=True, group_by=gb)
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        errors = validator.validate_obj(new_join)
        assert _has_online_in_place_error(errors)

    def test_online_join_unchanged_passes(self):
        old_join = _make_join(online=True)
        new_join = _make_join(online=True)
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        errors = validator.validate_obj(new_join)
        assert not _has_online_in_place_error(errors)

    def test_online_group_by_metadata_only_change_is_allowed(self):
        """metaData is a skipped field during diff, so metadata-only changes should pass."""
        old_gb = _make_group_by(online=True)
        new_gb = _make_group_by(online=True)
        new_gb.metaData.team = "different_team"
        new_gb.metaData.production = True
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        assert not _has_online_in_place_error(errors)

    def test_chained_online_join_nested_metadata_only_change_is_allowed(self):
        """Deeply nested metaData (e.g. joinSource.join.metaData) should be stripped during diff."""
        inner_join = Join(
            left=Source(
                events=EventSource(
                    table="inner_left",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=_make_group_by(name="team.inner_gb", online=True))],
            metaData=MetaData(name="team.inner_join", online=True),
        )
        gb_with_join_source = GroupBy(
            sources=[
                Source(
                    joinSource=JoinSource(
                        join=inner_join,
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="price", operation=Operation.SUM)],
            metaData=MetaData(name="team.chained_gb", online=True),
        )
        old_join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb_with_join_source)],
            metaData=MetaData(name="team.outer_join", online=True),
        )

        # Build new_join identically, then change only deeply nested metaData
        import copy

        new_join = copy.deepcopy(old_join)
        nested_join = new_join.joinParts[0].groupBy.sources[0].joinSource.join
        nested_join.metaData.customJson = '{"VERSION": "2"}'

        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})
        errors = validator.validate_obj(new_join)
        assert not _has_online_in_place_error(errors)

    def test_online_join_external_parts_only_change_is_allowed(self):
        """onlineExternalParts is a skipped field, so changes to it should not trigger a diff."""
        import copy

        old_join = _make_join(online=True)
        new_join = copy.deepcopy(old_join)
        new_join.onlineExternalParts = [
            ExternalPart(
                source=Source(
                    events=EventSource(
                        table="external_table",
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                ),
                keyMapping={"user_id": "user_id"},
            )
        ]
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        errors = validator.validate_obj(new_join)
        assert not _has_online_in_place_error(errors)

    def test_chained_online_join_nested_external_parts_change_is_allowed(self):
        """onlineExternalParts on a nested joinSource.join should also be stripped during diff."""
        import copy

        inner_join = Join(
            left=Source(
                events=EventSource(
                    table="inner_left",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=_make_group_by(name="team.inner_gb2", online=True))],
            metaData=MetaData(name="team.inner_join2", online=True),
        )
        gb_with_join_source = GroupBy(
            sources=[
                Source(
                    joinSource=JoinSource(
                        join=inner_join,
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="price", operation=Operation.SUM)],
            metaData=MetaData(name="team.chained_gb2", online=True),
        )
        old_join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb_with_join_source)],
            metaData=MetaData(name="team.outer_join2", online=True),
        )

        new_join = copy.deepcopy(old_join)
        nested_join = new_join.joinParts[0].groupBy.sources[0].joinSource.join
        nested_join.onlineExternalParts = [
            ExternalPart(
                source=Source(
                    events=EventSource(
                        table="ext_table",
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                ),
                keyMapping={"user_id": "user_id"},
            )
        ]

        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})
        errors = validator.validate_obj(new_join)
        assert not _has_online_in_place_error(errors)

    def test_validate_all_detects_metadata_change(self):
        """When skipped_fields is empty (validate_all mode), metadata changes ARE detected."""
        import copy

        old_join = _make_join(online=True)
        new_join = copy.deepcopy(old_join)
        new_join.metaData.customJson = '{"VERSION": "2"}'
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        assert not validator._has_diff(new_join, old_join)  # default: skipped
        assert validator._has_diff(new_join, old_join, skipped_fields=[])  # validate_all: detected

    def test_validate_all_detects_external_parts_change(self):
        """When skipped_fields is empty (validate_all mode), onlineExternalParts changes ARE detected."""
        import copy

        old_join = _make_join(online=True)
        new_join = copy.deepcopy(old_join)
        new_join.onlineExternalParts = [
            ExternalPart(
                source=Source(
                    events=EventSource(
                        table="ext_table",
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                ),
                keyMapping={"user_id": "user_id"},
            )
        ]
        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        assert not validator._has_diff(new_join, old_join)  # default: skipped
        assert validator._has_diff(new_join, old_join, skipped_fields=[])  # validate_all: detected

    def test_validate_all_detects_nested_metadata_change(self):
        """When skipped_fields is empty, nested metaData changes ARE detected."""
        import copy

        inner_join = Join(
            left=Source(
                events=EventSource(
                    table="inner_left",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=_make_group_by(name="team.inner_gb3", online=True))],
            metaData=MetaData(name="team.inner_join3", online=True),
        )
        gb_with_join_source = GroupBy(
            sources=[
                Source(
                    joinSource=JoinSource(
                        join=inner_join,
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="price", operation=Operation.SUM)],
            metaData=MetaData(name="team.chained_gb3", online=True),
        )
        old_join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb_with_join_source)],
            metaData=MetaData(name="team.outer_join3", online=True),
        )

        new_join = copy.deepcopy(old_join)
        nested_join = new_join.joinParts[0].groupBy.sources[0].joinSource.join
        nested_join.metaData.customJson = '{"VERSION": "2"}'

        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        assert not validator._has_diff(new_join, old_join)  # default: skipped
        assert validator._has_diff(new_join, old_join, skipped_fields=[])  # validate_all: detected

    def test_validate_all_detects_nested_external_parts_change(self):
        """When skipped_fields is empty, nested onlineExternalParts changes ARE detected."""
        import copy

        inner_join = Join(
            left=Source(
                events=EventSource(
                    table="inner_left",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=_make_group_by(name="team.inner_gb4", online=True))],
            metaData=MetaData(name="team.inner_join4", online=True),
        )
        gb_with_join_source = GroupBy(
            sources=[
                Source(
                    joinSource=JoinSource(
                        join=inner_join,
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="price", operation=Operation.SUM)],
            metaData=MetaData(name="team.chained_gb4", online=True),
        )
        old_join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb_with_join_source)],
            metaData=MetaData(name="team.outer_join4", online=True),
        )

        new_join = copy.deepcopy(old_join)
        nested_join = new_join.joinParts[0].groupBy.sources[0].joinSource.join
        nested_join.onlineExternalParts = [
            ExternalPart(
                source=Source(
                    events=EventSource(
                        table="ext_table",
                        query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                    )
                ),
                keyMapping={"user_id": "user_id"},
            )
        ]

        validator = _make_validator(existing_joins={old_join.metaData.name: old_join})

        assert not validator._has_diff(new_join, old_join)  # default: skipped
        assert validator._has_diff(new_join, old_join, skipped_fields=[])  # validate_all: detected

    def test_error_message_includes_config_name(self):
        old_gb = _make_group_by(name="team.important_gb", online=True, price_expr="price")
        new_gb = _make_group_by(name="team.important_gb", online=True, price_expr="price * 2")
        validator = _make_validator(existing_gbs={old_gb.metaData.name: old_gb})

        errors = validator.validate_obj(new_gb)
        online_errors = [e for e in errors if "is online and cannot be changed in-place" in str(e)]
        assert len(online_errors) == 1
        assert "team.important_gb" in str(online_errors[0])
        assert "GroupBy" in str(online_errors[0])


class TestTimePartitionedValidation:
    # timePartitioned flag is deprecated — column type is detected automatically at runtime.
    # These tests verify that setting timePartitioned without partitionColumn no longer errors.
    def test_group_by_source_time_partitioned_without_partition_column_no_error(self):
        group_by = _make_group_by()
        group_by.sources[0].events.query.timePartitioned = True
        group_by.sources[0].events.query.partitionColumn = None
        validator = _make_validator()

        errors = validator.validate_obj(group_by)
        assert not _has_time_partitioned_missing_partition_column_error(errors)

    def test_join_left_time_partitioned_without_partition_column_no_error(self):
        join = _make_join()
        join.left.events.query.timePartitioned = True
        join.left.events.query.partitionColumn = None
        validator = _make_validator()

        errors = validator.validate_obj(join)
        assert not _has_time_partitioned_missing_partition_column_error(errors)

    def test_join_bootstrap_time_partitioned_without_partition_column_no_error(self):
        join = _make_join()
        bootstrap_query = Query()
        bootstrap_query.timePartitioned = True
        bootstrap_query.partitionColumn = None
        join.bootstrapParts = [
            BootstrapPart(
                table="test.bootstrap_table",
                query=bootstrap_query,
            )
        ]
        validator = _make_validator()

        errors = validator.validate_obj(join)
        assert not _has_time_partitioned_missing_partition_column_error(errors)

    def test_time_partitioned_with_partition_column_passes(self):
        group_by = _make_group_by()
        group_by.sources[0].events.query.timePartitioned = True
        group_by.sources[0].events.query.partitionColumn = "created_at"
        validator = _make_validator()

        errors = validator.validate_obj(group_by)
        assert not _has_time_partitioned_missing_partition_column_error(errors)

    def test_non_time_partitioned_without_partition_column_passes_this_validation(self):
        group_by = _make_group_by()
        validator = _make_validator()

        errors = validator.validate_obj(group_by)
        assert not _has_time_partitioned_missing_partition_column_error(errors)

    def test_downstream_consumer_validates_partition_bounds_against_full_chain_grid(self):
        group_by = _make_group_by()
        source_query = group_by.sources[0].events.query
        source_query.timePartitioned = False
        source_query.startPartition = "2025-11-29"
        source_query.endPartition = "2026-04-12-02-00"

        downstream_consumer = _make_join(group_by=group_by)
        downstream_consumer.metaData.executionInfo = common.ExecutionInfo(
            outputTableInfo=common.TableInfo(
                partitionFormat="yyyy-MM-dd-HH-mm",
                partitionInterval=common.Window(length=3, timeUnit=common.TimeUnit.HOURS),
                partitionOffset=common.Window(length=1, timeUnit=common.TimeUnit.HOURS),
            )
        )

        errors = _make_validator().validate_obj(downstream_consumer)
        messages = "\n".join(str(error) for error in errors)

        assert "join team.my_join's underlying group_by team.my_gb source[0]" in messages
        assert "startPartition '2025-11-29'" in messages
        assert "endPartition '2026-04-12-02-00'" in messages
        assert "yyyy-MM-dd-HH-mm" in messages

    def test_downstream_consumer_accepts_partition_bounds_on_full_chain_grid(self):
        group_by = _make_group_by()
        source_query = group_by.sources[0].events.query
        source_query.timePartitioned = False
        source_query.partitionInterval = common.Window(length=3, timeUnit=common.TimeUnit.HOURS)
        source_query.partitionOffset = common.Window(length=1, timeUnit=common.TimeUnit.HOURS)
        source_query.startPartition = "2025-11-29-01-00"
        source_query.endPartition = "2026-04-12-04-00"

        downstream_consumer = _make_join(group_by=group_by)
        downstream_consumer.metaData.executionInfo = common.ExecutionInfo(
            outputTableInfo=common.TableInfo(
                partitionFormat="yyyy-MM-dd-HH-mm",
                partitionInterval=common.Window(length=3, timeUnit=common.TimeUnit.HOURS),
                partitionOffset=common.Window(length=1, timeUnit=common.TimeUnit.HOURS),
            )
        )

        errors = _make_validator().validate_obj(downstream_consumer)

        assert not any("partition bounds" in str(error) for error in errors)

    def test_downstream_consumer_allows_time_partitioned_partition_bounds(self):
        group_by = _make_group_by()
        source_query = group_by.sources[0].events.query
        source_query.timePartitioned = True
        source_query.startPartition = "2025-11-29"
        source_query.endPartition = "2026-04-12"

        downstream_consumer = _make_join(group_by=group_by)
        downstream_consumer.metaData.executionInfo = common.ExecutionInfo(
            outputTableInfo=common.TableInfo(
                partitionFormat="yyyy-MM-dd-HH-mm",
                partitionInterval=common.Window(length=3, timeUnit=common.TimeUnit.HOURS),
                partitionOffset=common.Window(length=1, timeUnit=common.TimeUnit.HOURS),
            )
        )

        errors = _make_validator().validate_obj(downstream_consumer)

        assert not any("partition bounds" in str(error) for error in errors)
