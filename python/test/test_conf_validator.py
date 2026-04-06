from gen_thrift.api.ttypes import (
    Accuracy,
    Aggregation,
    BootstrapPart,
    Derivation,
    EntitySource,
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
from gen_thrift.common.ttypes import TimeUnit, Window

from ai.chronon.cli.compile.conf_validator import (
    ConfValidator,
    SKIPPED_FIELDS,
    _group_by_has_hourly_windows,
    _source_has_topic,
    detect_feature_name_collisions,
    is_identifier,
)


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


class TestUnboundedEventsUnwindowed:
    def test_unbounded_events_unwindowed_error(self):
        """EventSource with no startPartition + aggregation with no window -> error."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        query=Query(
                            selects={"user_id": "user_id", "val": "val"},
                            timeColumn="ts",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="val", operation=Operation.SUM, windows=None),
            ],
            metaData=MetaData(name="team.unbounded_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert any("unwindowed" in str(e).lower() or "unbounded" in str(e).lower() for e in errors)

    def test_bounded_events_unwindowed_ok(self):
        """EventSource WITH startPartition + unwindowed aggregation is fine."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        query=Query(
                            selects={"user_id": "user_id", "val": "val"},
                            timeColumn="ts",
                            startPartition="2023-01-01",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="val", operation=Operation.SUM, windows=None),
            ],
            metaData=MetaData(name="team.bounded_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert not any("unbounded" in str(e).lower() for e in errors)

    def test_unbounded_events_windowed_ok(self):
        """EventSource with no startPartition but windowed aggregation is fine."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        query=Query(
                            selects={"user_id": "user_id", "val": "val"},
                            timeColumn="ts",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="val",
                    operation=Operation.SUM,
                    windows=[Window(length=7, timeUnit=TimeUnit.DAYS)],
                ),
            ],
            metaData=MetaData(name="team.windowed_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert not any("unbounded" in str(e).lower() for e in errors)

    def test_entity_source_unwindowed_no_error(self):
        """EntitySource (not EventSource) with unwindowed aggregation is fine."""
        gb = GroupBy(
            sources=[
                Source(
                    entities=EntitySource(
                        snapshotTable="db.entities",
                        query=Query(selects={"user_id": "user_id", "val": "val"}),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(inputColumn="val", operation=Operation.SUM, windows=None),
            ],
            metaData=MetaData(name="team.entity_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert not any("unbounded" in str(e).lower() for e in errors)


class TestCumulativeRequiresTimeColumn:
    def test_cumulative_without_time_column_error(self):
        """Cumulative EventSource without timeColumn -> error."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.cumulative",
                        isCumulative=True,
                        query=Query(
                            selects={"user_id": "user_id", "val": "val"},
                            timeColumn=None,
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            metaData=MetaData(name="team.cumul_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert any("timecolumn" in str(e).lower() for e in errors)

    def test_cumulative_with_time_column_ok(self):
        """Cumulative EventSource with timeColumn set is valid."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.cumulative",
                        isCumulative=True,
                        query=Query(
                            selects={"user_id": "user_id", "val": "val"},
                            timeColumn="event_ts",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            metaData=MetaData(name="team.cumul_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert not any("timecolumn" in str(e).lower() for e in errors)


class TestHourlyWindowsBatch:
    def test_hourly_windows_batch_error(self):
        """Daily-refreshed (no topic, non-temporal) GroupBy with hourly windows -> error."""
        gb = GroupBy(
            sources=[
                Source(
                    entities=EntitySource(
                        snapshotTable="db.table",
                        query=Query(selects={"user_id": "user_id", "val": "val"}),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="val",
                    operation=Operation.SUM,
                    windows=[Window(length=1, timeUnit=TimeUnit.HOURS)],
                ),
            ],
            metaData=MetaData(name="team.hourly_batch_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert any("hourly" in str(e).lower() for e in errors)

    def test_hourly_windows_streaming_ok(self):
        """GroupBy with topic (streaming) and hourly windows is fine."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        topic="kafka.topic",
                        query=Query(
                            selects={"user_id": "user_id", "val": "val"},
                            timeColumn="ts",
                        ),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="val",
                    operation=Operation.SUM,
                    windows=[Window(length=1, timeUnit=TimeUnit.HOURS)],
                ),
            ],
            metaData=MetaData(name="team.hourly_streaming_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert not any("hourly" in str(e).lower() for e in errors)

    def test_hourly_windows_temporal_ok(self):
        """GroupBy with TEMPORAL accuracy and hourly windows is fine."""
        gb = GroupBy(
            sources=[
                Source(
                    entities=EntitySource(
                        snapshotTable="db.table",
                        query=Query(selects={"user_id": "user_id", "val": "val"}),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[
                Aggregation(
                    inputColumn="val",
                    operation=Operation.SUM,
                    windows=[Window(length=1, timeUnit=TimeUnit.HOURS)],
                ),
            ],
            accuracy=Accuracy.TEMPORAL,
            metaData=MetaData(name="team.hourly_temporal_gb"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(gb)
        assert not any("hourly" in str(e).lower() for e in errors)


class TestGroupByInOnlineJoin:
    def test_offline_gb_in_online_join_error(self):
        """GroupBy marked offline that appears in an old online join -> error."""
        old_join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=_make_group_by(name="team.offline_gb", online=False))],
            metaData=MetaData(name="team.online_join", online=True),
        )
        validator = _make_validator(existing_joins={"team.online_join": old_join})
        gb = _make_group_by(name="team.offline_gb", online=False)
        errors = validator._validate_group_by(gb)
        assert any("offline" in str(e).lower() and "online" in str(e).lower() for e in errors)

    def test_non_prod_gb_in_prod_join_error(self):
        """GroupBy marked non-production in a production join -> error."""
        old_join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=_make_group_by(name="team.non_prod_gb"))],
            metaData=MetaData(name="team.prod_join", production=True),
        )
        validator = _make_validator(existing_joins={"team.prod_join": old_join})
        gb = _make_group_by(name="team.non_prod_gb")
        gb.metaData.production = False
        errors = validator._validate_group_by(gb)
        assert any("non-production" in str(e).lower() or "non production" in str(e).lower() for e in errors)


class TestJoinValidation:
    def test_online_join_offline_groupby_error(self):
        """Online Join with an offline GroupBy -> error."""
        gb = _make_group_by(name="team.offline_gb", online=False)
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb)],
            metaData=MetaData(name="team.online_join", online=True),
        )
        validator = _make_validator()
        errors = validator.validate_obj(join)
        assert any("offline" in str(e).lower() for e in errors)

    def test_online_join_online_groupby_ok(self):
        """Online Join with an online GroupBy -> no offline error."""
        gb = _make_group_by(name="team.online_gb", online=True)
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb)],
            metaData=MetaData(name="team.online_join", online=True),
        )
        validator = _make_validator()
        errors = validator.validate_obj(join)
        assert not any("offline" in str(e).lower() and "online" in str(e).lower() for e in errors)

    def test_join_key_mapping_left_key_missing(self):
        """key_mapping referencing a column not on left side -> error."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        query=Query(selects={"item_id": "item_id", "val": "val"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["item_id"],
            aggregations=[Aggregation(inputColumn="val", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb1", online=False),
        )
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[
                JoinPart(
                    groupBy=gb,
                    keyMapping={"nonexistent_col": "item_id"},
                ),
            ],
            metaData=MetaData(name="team.join_bad_keys"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(join)
        assert any("key" in str(e).lower() for e in errors)

    def test_join_key_mapping_gb_key_missing(self):
        """key_mapping value referencing non-existent GroupBy key -> error."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        query=Query(selects={"item_id": "item_id", "val": "val"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["item_id"],
            aggregations=[Aggregation(inputColumn="val", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb1", online=False),
        )
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[
                JoinPart(
                    groupBy=gb,
                    keyMapping={"user_id": "wrong_key"},
                ),
            ],
            metaData=MetaData(name="team.join_bad_mapping"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(join)
        assert any("key_mapping" in str(e).lower() or "key" in str(e).lower() for e in errors)

    def test_join_valid_key_mapping_ok(self):
        """Valid key_mapping from left column to GroupBy key -> no key errors."""
        gb = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.events",
                        query=Query(selects={"item_id": "item_id", "val": "val"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["item_id"],
            aggregations=[Aggregation(inputColumn="val", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb1", online=False),
        )
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[
                JoinPart(
                    groupBy=gb,
                    keyMapping={"user_id": "item_id"},
                ),
            ],
            metaData=MetaData(name="team.join_ok"),
        )
        validator = _make_validator()
        errors = validator.validate_obj(join)
        assert not any("missing" in str(e).lower() and "key" in str(e).lower() for e in errors)

    def test_production_join_non_prod_gb_error(self):
        """Production Join with non-production GroupBy that exists in old_objs -> error."""
        gb = _make_group_by(name="team.non_prod_gb")
        gb.metaData.production = False
        join = Join(
            left=Source(
                events=EventSource(
                    table="left_table",
                    query=Query(selects={"user_id": "user_id"}, timeColumn="ts"),
                )
            ),
            joinParts=[JoinPart(groupBy=gb)],
            metaData=MetaData(name="team.prod_join", production=True),
        )
        validator = _make_validator(existing_gbs={"team.non_prod_gb": gb})
        errors = validator.validate_obj(join)
        assert any("non production" in str(e).lower() or "non-production" in str(e).lower() for e in errors)


class TestDerivationValidation:
    def test_derivation_name_conflict(self):
        """Two derivations producing the same output name -> error."""
        validator = _make_validator()
        pre_derived_cols = ["col_a", "col_b"]
        derivations = [
            Derivation(name="output_x", expression="col_a"),
            Derivation(name="output_x", expression="col_b"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert any("conflict" in str(e).lower() or "output_x" in str(e) for e in errors)

    def test_derivation_references_valid_column(self):
        """Derivation referencing existing pre-derived column -> no error."""
        validator = _make_validator()
        pre_derived_cols = ["col_a", "col_b"]
        derivations = [
            Derivation(name="renamed_a", expression="col_a"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert len(errors) == 0

    def test_derivation_references_invalid_column(self):
        """Derivation (identifier) referencing non-existent column -> error."""
        validator = _make_validator()
        pre_derived_cols = ["col_a", "col_b"]
        derivations = [
            Derivation(name="renamed_x", expression="nonexistent_col"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert any("nonexistent_col" in str(e) for e in errors)

    def test_derivation_sql_expression_no_column_check(self):
        """Non-identifier expressions (SQL) are not checked for column existence."""
        validator = _make_validator()
        pre_derived_cols = ["col_a", "col_b"]
        derivations = [
            Derivation(name="computed", expression="col_a + col_b"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert len(errors) == 0

    def test_derivation_wildcard_with_rename(self):
        """Wildcard derivation with rename removes original from derived set."""
        validator = _make_validator()
        pre_derived_cols = ["col_a", "col_b"]
        derivations = [
            Derivation(name="*", expression="*"),
            Derivation(name="renamed_a", expression="col_a"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert len(errors) == 0

    def test_derivation_wildcard_name_conflict_with_existing(self):
        """Wildcard included, then derivation output name same as pre-derived col -> conflict."""
        validator = _make_validator()
        pre_derived_cols = ["col_a", "col_b"]
        # Wildcard expands both col_a and col_b. Then naming a derivation "col_b" conflicts.
        derivations = [
            Derivation(name="*", expression="*"),
            Derivation(name="col_b", expression="col_a + 1"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert any("col_b" in str(e) and "conflict" in str(e).lower() for e in errors)

    def test_derivation_ds_ts_allowed(self):
        """Derivations referencing 'ds' or 'ts' are allowed even if not in pre-derived cols."""
        validator = _make_validator()
        pre_derived_cols = ["col_a"]
        derivations = [
            Derivation(name="date_col", expression="ds"),
            Derivation(name="time_col", expression="ts"),
        ]
        errors = validator._validate_derivations(pre_derived_cols, derivations)
        assert len(errors) == 0


class TestFeatureNameCollisions:
    def test_collision_detected(self):
        """Two GroupBys with same key and same output column name -> collision."""
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.t1",
                        query=Query(selects={"user_id": "user_id", "clicks": "clicks"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="clicks", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb1", version=0),
        )
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.t2",
                        query=Query(selects={"user_id": "user_id", "clicks": "clicks"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="clicks", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb2", version=0),
        )
        result = detect_feature_name_collisions(
            [(gb1, ""), (gb2, "")], "right parts", "team.test_join"
        )
        assert result is not None
        assert "collision" in str(result).lower()

    def test_no_collision_with_prefix(self):
        """Same columns but different prefixes -> no collision."""
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.t1",
                        query=Query(selects={"user_id": "user_id", "clicks": "clicks"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="clicks", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb1", version=0),
        )
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.t2",
                        query=Query(selects={"user_id": "user_id", "clicks": "clicks"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="clicks", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb2", version=0),
        )
        result = detect_feature_name_collisions(
            [(gb1, "prefix_a"), (gb2, "prefix_b")], "right parts", "team.test_join"
        )
        assert result is None

    def test_no_collision_different_columns(self):
        """Different aggregation columns -> no collision."""
        gb1 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.t1",
                        query=Query(selects={"user_id": "user_id", "clicks": "clicks"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="clicks", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb1", version=0),
        )
        gb2 = GroupBy(
            sources=[
                Source(
                    events=EventSource(
                        table="db.t2",
                        query=Query(selects={"user_id": "user_id", "views": "views"}, timeColumn="ts"),
                    )
                )
            ],
            keyColumns=["user_id"],
            aggregations=[Aggregation(inputColumn="views", operation=Operation.SUM)],
            metaData=MetaData(name="team.gb2", version=0),
        )
        result = detect_feature_name_collisions(
            [(gb1, ""), (gb2, "")], "right parts", "team.test_join"
        )
        assert result is None


class TestHelperFunctions:
    def test_is_identifier_valid(self):
        assert is_identifier("foo") is True
        assert is_identifier("_bar") is True
        assert is_identifier("col_123") is True

    def test_is_identifier_invalid(self):
        assert is_identifier("1abc") is False
        assert is_identifier("a + b") is False
        assert is_identifier("") is False

    def test_source_has_topic_events(self):
        src_with_topic = Source(events=EventSource(table="t", topic="kafka.topic", query=Query()))
        src_without_topic = Source(events=EventSource(table="t", query=Query()))
        assert _source_has_topic(src_with_topic) is True
        assert _source_has_topic(src_without_topic) is False

    def test_source_has_topic_entities(self):
        src_with = Source(entities=EntitySource(snapshotTable="t", mutationTopic="topic", query=Query()))
        src_without = Source(entities=EntitySource(snapshotTable="t", query=Query()))
        assert _source_has_topic(src_with) is True
        assert _source_has_topic(src_without) is False

    def test_group_by_has_hourly_windows(self):
        gb_hourly = _make_group_by()
        gb_hourly.aggregations = [
            Aggregation(
                inputColumn="price",
                operation=Operation.SUM,
                windows=[Window(length=1, timeUnit=TimeUnit.HOURS)],
            ),
        ]
        gb_daily = _make_group_by()
        gb_daily.aggregations = [
            Aggregation(
                inputColumn="price",
                operation=Operation.SUM,
                windows=[Window(length=1, timeUnit=TimeUnit.DAYS)],
            ),
        ]
        gb_no_aggs = _make_group_by()
        gb_no_aggs.aggregations = None
        assert _group_by_has_hourly_windows(gb_hourly) is True
        assert _group_by_has_hourly_windows(gb_daily) is False
        assert _group_by_has_hourly_windows(gb_no_aggs) is False


class TestCanSkipMaterialize:
    def test_offline_gb_can_skip(self):
        """Offline GroupBy does not need batch upload -> can skip."""
        gb = _make_group_by(name="team.offline_gb", online=False)
        validator = _make_validator()
        reasons = validator.can_skip_materialize(gb)
        assert len(reasons) > 0

    def test_online_gb_cannot_skip(self):
        """Online GroupBy needs batch upload -> cannot skip."""
        gb = _make_group_by(name="team.online_gb", online=True)
        validator = _make_validator()
        reasons = validator.can_skip_materialize(gb)
        assert len(reasons) == 0
