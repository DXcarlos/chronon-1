"""
Sample entity temporal accurate backfill (mutation)
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

from group_bys.sample_team import mutation_sample_group_by
from sources import sample_sources

from ai.chronon.types import Join, JoinPart

backfill_mutation_join = Join(
    left=sample_sources.event_source,
    row_ids="subject",
    right_parts=[JoinPart(group_by=mutation_sample_group_by.mutation_features)],
    online=False,
    version=0,
)

