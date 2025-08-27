"""
Sample Chaining Join
"""

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

from group_bys.sample_team.chaining_group_by import chaining_group_by_v1
from sources import test_sources

from ai.chronon.types import Join, JoinPart

v1 = Join(
    left=test_sources.event_source,
    row_ids=["subject", "event"],
    right_parts=[
        JoinPart(
            group_by=chaining_group_by_v1,
            key_mapping={"subject": "user_id"},
        ),
    ],
    online=True,
    check_consistency=True,
    version=0,
)
