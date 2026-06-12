#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import gen_thrift.common.ttypes as common
from gen_thrift.api import ttypes as api


def event_source(table):
    """
    Sample left join
    """
    return api.Source(
        events=api.EventSource(
            table=table,
            query=api.Query(
                startPartition="2020-04-09",
                selects={
                    "subject": "subject_sql",
                    "event_id": "event_sql",
                },
                timeColumn="CAST(ts AS DOUBLE)",
            ),
        ),
    )


def right_part(source):
    """
    Sample Agg
    """
    return api.JoinPart(
        groupBy=api.GroupBy(
            sources=[source],
            keyColumns=["subject"],
            aggregations=[],
            accuracy=api.Accuracy.SNAPSHOT,
        ),
    )


import pytest
from ai.chronon import join


def test_online_schedule_validation():
    """Test that online_schedule validation works correctly for joins."""
    # Test that online_schedule cannot be set when online=False
    with pytest.raises(ValueError, match="online_schedule cannot be set when online=False"):
        join.Join(
            left=event_source("table"),
            right_parts=[right_part(event_source("table"))],
            version=1,
            row_ids=["id"],
            online=False,
            online_schedule="@daily"  # This should raise an error
        )

    # Test that online_schedule can be None when online=False
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=False,
        online_schedule=None  # This should be fine
    )
    assert j.metaData.executionInfo.onlineSchedule is None

    # Test that online_schedule defaults to @daily when online=True and not specified
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        online_schedule=None  # Should default to @daily
    )
    assert j.metaData.executionInfo.onlineSchedule == "@daily"

    # offline @never disables only offline scheduling; online=True still gets the normal default.
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        offline_schedule="@never",
        online_schedule=None,
    )
    assert j.metaData.executionInfo.offlineSchedule == "@never"
    assert j.metaData.executionInfo.onlineSchedule == "@daily"

    # Test that online_schedule can be explicitly set when online=True
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        offline_schedule="0 2 * * *",
        online_schedule="0 2 * * *"  # Custom schedule
    )
    assert j.metaData.executionInfo.onlineSchedule == "0 2 * * *"

    # Existing daily workflows may use different cron offsets for offline and online jobs.
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        offline_schedule="0 4 * * *",
        online_schedule="0 3 * * *",
    )
    assert j.metaData.executionInfo.onlineSchedule == "0 3 * * *"

    with pytest.raises(ValueError, match="sub-daily partition_interval"):
        join.Join(
            left=event_source("table"),
            right_parts=[right_part(event_source("table"))],
            version=1,
            row_ids=["id"],
            online=True,
            offline_schedule="0 */3 * * *",
            online_schedule="30 */3 * * *",
            partition_interval="3h",
        )

    # Test that @never disables online scheduling even when online=True
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=True,
        online_schedule="@never"
    )
    assert j.metaData.executionInfo.onlineSchedule is None

    # Test that @never is accepted even when online=False
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        online=False,
        online_schedule="@never"
    )
    assert j.metaData.executionInfo.onlineSchedule is None


def test_partition_interval_sets_output_table_info():
    j = join.Join(
        left=event_source("table"),
        right_parts=[right_part(event_source("table"))],
        version=1,
        row_ids=["id"],
        partition_interval="3h",
    )

    table_info = j.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd HH:mm"
    assert table_info.partitionInterval.length == 3
    assert table_info.partitionInterval.timeUnit == common.TimeUnit.HOURS
