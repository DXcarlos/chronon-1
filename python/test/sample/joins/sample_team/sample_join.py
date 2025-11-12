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

from group_bys.sample_team import sample_group_by, sample_group_by_group_by
from sources import test_sources

from ai.chronon.join import (
    Join,
    JoinPart,
)
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import EnvironmentVariables

v1 = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1)],
    row_ids="place_id",
    table_properties={"config_json": """{"sample_key": "sample_value"}"""},
    output_namespace="sample_namespace",
    env_vars=EnvironmentVariables(
        modeEnvironments={
            RunMode.BACKFILL: {"EXECUTOR_MEMORY": "9G"},
        }
    ),
    online=True,
    version=0,
)

never = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1)],
    row_ids=["s2CellId", "place_id"],
    output_namespace="sample_namespace",
    offline_schedule="@never",
    version=0,
)

group_by_of_group_by = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by_group_by.v1)],
    row_ids="s2CellId",
    output_namespace="sample_namespace",
    version=0,
)

consistency_check = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1)],
    row_ids="place_id",
    output_namespace="sample_namespace",
    check_consistency=True,
    version=0,
)

no_log_flattener = Join(
    left=test_sources.staging_entities,
    right_parts=[JoinPart(group_by=sample_group_by.v1)],
    row_ids=["place_id"],
    output_namespace="sample_namespace",
    sample_percent=0.0,
    version=0,
)
