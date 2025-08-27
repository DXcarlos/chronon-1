"""
Sample Join
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
    entity_sample_group_by_from_module,
    event_sample_group_by,
    event_with_derivations,
)
from sources import test_sources

from ai.chronon.join import Derivation, Join, JoinPart

v2 = Join(
    left=test_sources.event_source,
    row_ids="event",
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={'subject': 'group_by_subject'},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={'subject': 'group_by_subject'},
        )
    ],
    derivations=[
        Derivation(
            name="derived_field",
            expression="sample_team_event_sample_group_by_v1_event_sum_7d / sample_team_entity_sample_group_by_from_module_v1_entity_last"
        ),
        Derivation(
            name="*",
            expression="*"
        )
    ],
    version=0,
)


# V2 includes GroupBys with Derivations
v3 = Join(
    left=test_sources.event_source,
    row_ids=["subject"],
    right_parts=[
        JoinPart(
            group_by=event_with_derivations.v1,
        ),
        JoinPart(
            group_by=event_with_derivations.v2,
        ),
    ],
    derivations=[
        Derivation(
            name="derivation_of_derivations",
            expression="subject_x_10 + sum_ratio * 2"
        ),
        Derivation(
            name="*",
            expression="*"
        )
    ],
    use_long_names=False,
    version=0,
)
