"""
Sample Label Join
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

from group_bys.sample_team import (
    event_sample_group_by,
    group_by_with_kwargs,
)
from group_bys.sample_team.label_part_group_by import label_part_group_by
from sources import test_sources

from ai.chronon.types import Join, JoinPart, LabelParts

v1 = Join(
    left=test_sources.event_source,
    row_ids="group_by_subject",
    output_namespace="sample_namespace",
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
        JoinPart(
            group_by=group_by_with_kwargs.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
    ],
    label_part=LabelParts(
        [
            JoinPart(group_by=label_part_group_by),
        ],
        left_start_offset=7,
        left_end_offset=7,
        label_offline_schedule="@weekly",
    ),
    online=False,
    use_long_names=True,
    version=0,
)
