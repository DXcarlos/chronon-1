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

from gen_thrift.api.ttypes import EventSource, Source
from group_bys.quickstart.purchases import purchase_features
from group_bys.quickstart.returns import return_features
from group_bys.quickstart.users import user_features

from ai.chronon.types import Join, JoinPart, Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects(
                "user_id"
            ),  # The primary key used to join various GroupBys together
            time_column="ts",
        ),  # The event time used to compute feature values as-of
    )
)

training_set_join = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=group_by) for group_by in [purchase_features, return_features, user_features]
    ],  # Include the three GroupBys
    version=0,
)

online_training_set_join = Join(
    left=source,
    row_ids=["user_id"],
    right_parts=[
        JoinPart(group_by=group_by) for group_by in [purchase_features, return_features]
    ],  # Include the two online GroupBys
    online=True,
    version=0,
)
