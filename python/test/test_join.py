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
            backfillStartDate="2020-04-09",
        ),
    )
