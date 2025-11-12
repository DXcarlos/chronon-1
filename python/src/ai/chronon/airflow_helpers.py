import json
import math
from typing import OrderedDict

from gen_thrift.api.ttypes import GroupBy, Join
from gen_thrift.common.ttypes import TimeUnit

import ai.chronon.utils as utils
from ai.chronon.constants import (
    AIRFLOW_DEPENDENCIES_KEY,
)


def create_airflow_dependency(table, partition_column, additional_partitions=None, offset=0):
    """
    Create an Airflow dependency object for a table.

    Args:
        table: The table name (with namespace)
        partition_column: The partition column to use (defaults to 'ds')
        additional_partitions: Additional partitions to include in the dependency

    Returns:
        A dictionary with name and spec for the Airflow dependency
    """
    assert (
        partition_column is not None
    ), """Partition column must be provided via the spark.chronon.partition.column 
        config. This can be set as a default in teams.py, or at the individual config level. For example:
        ``` 
        Team(
            conf=ConfigProperties(
                common={
                    "spark.chronon.partition.column": "_test_column",
                }
            )
        )
        ```
        """

    additional_partitions_str = ""
    if additional_partitions:
        additional_partitions_str = "/" + "/".join(additional_partitions)

    return {
        "name": f"wf_{utils.sanitize(table)}_with_offset_{offset}",
        "spec": f"{table}/{partition_column}={{{{ macros.ds_add(ds, {offset}) }}}}{additional_partitions_str}",
    }


def _get_partition_col_from_query(query):
    """Gets partition column from query if available"""
    if query:
        return query.partitionColumn
    return None


def _get_additional_subPartitionsToWaitFor_from_query(query):
    """Gets additional subPartitionsToWaitFor from query if available"""
    if query:
        return query.subPartitionsToWaitFor
    return None


def _get_airflow_deps_from_source(source, partition_column=None):
    """
    Given a source, return a list of Airflow dependencies.

    Args:
        source: The source object (events, entities, or joinSource)
        partition_column: The partition column to use

    Returns:
        A list of Airflow dependency objects
    """
    tables = []
    additional_partitions = None
    # Assumes source has already been normalized
    if source.events:
        tables = [source.events.table]
        # Use partition column from query if available, otherwise use the provided one
        source_partition_column, additional_partitions = (
            _get_partition_col_from_query(source.events.query) or partition_column,
            _get_additional_subPartitionsToWaitFor_from_query(source.events.query),
        )

    elif source.entities:
        # Given the setup of Query, we currently mandate the same partition column for snapshot and mutations tables
        tables = [source.entities.snapshotTable]
        if source.entities.mutationTable:
            tables.append(source.entities.mutationTable)
        source_partition_column, additional_partitions = (
            _get_partition_col_from_query(source.entities.query) or partition_column,
            _get_additional_subPartitionsToWaitFor_from_query(source.entities.query),
        )
    elif source.joinSource:
        # TODO: Handle joinSource -- it doesn't work right now because the metadata isn't set on joinSource at this point
        return []
    else:
        # Unknown source type
        return []

    return [
        create_airflow_dependency(table, source_partition_column, additional_partitions)
        for table in tables
    ]


def extract_default_partition_column(obj):
    try:
        return obj.metaData.executionInfo.conf.common.get("spark.chronon.partition.column")
    except Exception:
        # Error handling occurs in `create_airflow_dependency`
        return None


def _get_distinct_day_windows(group_by):
    windows = []
    aggs = group_by.aggregations
    if aggs:
        for agg in aggs:
            for window in agg.windows:
                time_unit = window.timeUnit
                length = window.length
                if time_unit == TimeUnit.DAYS:
                    windows.append(length)
                elif time_unit == TimeUnit.HOURS:
                    windows.append(math.ceil(length / 24))
                elif time_unit == TimeUnit.MINUTES:
                    windows.append(math.ceil(length / (24 * 60)))
    return set(windows)


def _set_join_deps(join):
    default_partition_col = extract_default_partition_column(join)

    deps = []

    # Handle left source
    left_query = utils.get_query(join.left)
    left_partition_column = _get_partition_col_from_query(left_query) or default_partition_col
    deps.extend(_get_airflow_deps_from_source(join.left, left_partition_column))

    # Handle right parts (join parts)
    if join.joinParts:
        for join_part in join.joinParts:
            if join_part.groupBy and join_part.groupBy.sources:
                for source in join_part.groupBy.sources:
                    source_query = utils.get_query(source)
                    source_partition_column = (
                        _get_partition_col_from_query(source_query) or default_partition_col
                    )
                    deps.extend(_get_airflow_deps_from_source(source, source_partition_column))

    # Update the metadata customJson with dependencies
    _dedupe_and_set_airflow_deps_json(join, deps, AIRFLOW_DEPENDENCIES_KEY)


def _set_group_by_deps(group_by):
    if not group_by.sources:
        return

    default_partition_col = extract_default_partition_column(group_by)

    deps = []

    # Process each source in the group_by
    for source in group_by.sources:
        source_query = utils.get_query(source)
        source_partition_column = (
            _get_partition_col_from_query(source_query) or default_partition_col
        )
        deps.extend(_get_airflow_deps_from_source(source, source_partition_column))

    # Update the metadata customJson with dependencies
    _dedupe_and_set_airflow_deps_json(group_by, deps, AIRFLOW_DEPENDENCIES_KEY)


def _dedupe_and_set_airflow_deps_json(obj, deps, custom_json_key):
    sorted_items = [tuple(sorted(d.items())) for d in deps]
    # Use OrderedDict for re-producible ordering of dependencies
    unique = [OrderedDict(t) for t in sorted_items]
    existing_json = obj.metaData.customJson or "{}"
    json_map = json.loads(existing_json)
    json_map[custom_json_key] = unique
    obj.metaData.customJson = json.dumps(json_map)


def set_airflow_deps(obj):
    """
    Set Airflow dependencies for a Chronon object.

    Args:
        obj: A Join, GroupBy
    """
    # StagingQuery dependency setting is handled directly in object init
    if isinstance(obj, Join):
        _set_join_deps(obj)
    elif isinstance(obj, GroupBy):
        _set_group_by_deps(obj)
