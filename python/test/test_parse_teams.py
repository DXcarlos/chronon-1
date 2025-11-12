"""
Tests for the parse_teams module.
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
from gen_thrift.api.ttypes import GroupBy, Join, JoinPart, MetaData, Team
from gen_thrift.common.ttypes import ConfigProperties

from ai.chronon.cli.compile import parse_teams
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import EnvironmentVariables, ExecutionInfo


def test_update_metadata_with_existing_output_namespace():
    """Test that update_metadata doesn't override existing outputNamespace."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Test with existing outputNamespace
    existing_namespace = "existing_namespace"
    obj = GroupBy(metaData=MetaData(
        team=team_name,
        name="test.group_by.name",
        outputNamespace=existing_namespace
    ))

    # Call the function
    parse_teams.update_metadata(obj, team_dict)

    # Verify outputNamespace wasn't changed
    assert obj.metaData.outputNamespace == existing_namespace


def test_update_metadata_without_existing_output_namespace():
    """Test that update_metadata sets outputNamespace when not already set."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Test without existing outputNamespace
    obj = GroupBy(metaData=MetaData(
        team=team_name,
        name="test.group_by.name",
    ))

    # Call the function
    parse_teams.update_metadata(obj, team_dict)

    # Verify outputNamespace was set from team
    assert obj.metaData.outputNamespace == "team_namespace"


def test_update_metadata_preserves_join_part_namespace():
    """Test that update_metadata preserves outputNamespace in join parts."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Create a join with join parts that have existing outputNamespace
    join_part_gb = GroupBy(metaData=MetaData(outputNamespace="existing_jp_namespace"))
    join_part = JoinPart(groupBy=join_part_gb)

    # Create the join object
    join = Join(
        metaData=MetaData(
            team=team_name,
            name="test.join.name",
            outputNamespace="join_namespace"
        ),
        joinParts=[join_part]
    )

    # Call the function
    parse_teams.update_metadata(join, team_dict)

    # Verify outputNamespace values were preserved
    assert join.metaData.outputNamespace == "join_namespace"
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "existing_jp_namespace"


def test_update_metadata_sets_missing_join_part_namespace():
    """Test that update_metadata sets outputNamespace for join parts when not set."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }

    # Create a join with join parts that don't have outputNamespace
    join_part_gb = GroupBy(metaData=MetaData())
    join_part = JoinPart(groupBy=join_part_gb)

    # Create the join object
    join = Join(
        metaData=MetaData(
            team=team_name,
            name="test.join.name",
            outputNamespace="join_namespace"
        ),
        joinParts=[join_part]
    )

    # Call the function
    parse_teams.update_metadata(join, team_dict)

    # Verify outputNamespace values were set correctly
    assert join.metaData.outputNamespace == "join_namespace"
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "join_namespace"

def test_merge_team_execution_info():
    """Test that merge_team_execution_info correctly merges team execution info."""
    # Setup
    gcp_team = "gcp"
    team_dict = {
        "default": Team(
            name="default",
            env=EnvironmentVariables(
                common={
                    "common_env_key": "dummy_value",
                    "unique_key_default": "default_value",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_DATAPROC_CLUSTER_NAME": "default-cluster",
                        "backfill-unique-key-default": "default_value",
                    },
                    RunMode.STREAMING: {
                        "GCP_DATAPROC_CLUSTER_NAME": "default-streaming-cluster",
                        "streaming-unique-key-default": "default_value",
                    },
                    RunMode.METASTORE: {
                        "GCP_DATAPROC_CLUSTER_NAME": "default-metastore-cluster",
                        "metastore-unique-key-default": "default_value",
                    },
                },
            ),
            conf=ConfigProperties(
                common={
                    "common_conf_key": "dummy_value",
                    "unique_conf_key_default": "default_value",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.chronon.partition.column": "ds",
                        "backfill-unique-conf-key-default": "default_value",
                    },
                    RunMode.STREAMING: {
                        "streaming-unique-conf-key-default": "default_value",
                    },
                    RunMode.METASTORE: {
                        "metastore-unique-conf-key-default": "default_value",
                    },
                },
            ),
        ),
        gcp_team: Team(
            name=gcp_team,
            env=EnvironmentVariables(
                common={
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_CREATE_DATAPROC": "true",
                        "GCP_DATAPROC_NUM_WORKERS": "2",
                        "GCP_DATAPROC_MASTER_HOST_TYPE": "n2-highmem-8",
                        "GCP_DATAPROC_WORKER_HOST_TYPE": "n2-highmem-4",
                        "ARTIFACT_PREFIX": "some-path",
                    }
                },
            ),
            conf=ConfigProperties(
                common={
                    "spark.chronon.cloud_provider": "gcp",
                    "common_conf_key": "common_conf_value_team",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "backfill-unique-conf-key-gcp": "gcp_value",
                    },
                },
            ),
        ),
    }

    # Create metadata object
    metadata = MetaData(
        team=gcp_team,
        name="test_metadata_for_groupby",
        executionInfo=ExecutionInfo(
            env=EnvironmentVariables(
                common={
                    "common_env_key": "common_value_override_env",
                },
                modeEnvironments={
                    RunMode.BACKFILL: {
                        "GCP_DATAPROC_CLUSTER_NAME": "special-backfill_cluster",
                    },
                    RunMode.STREAMING: {"GCP_DATAPROC_CLUSTER_NAME": "special-streaming_cluster"},
                },
            ),
            conf=ConfigProperties(
                common={
                    "common_conf_key": "common_value_override_conf",
                },
                modeConfigs={
                    RunMode.BACKFILL: {
                        "spark.chronon.partition.column": "notDs",
                    },
                    RunMode.UPLOAD: {
                        "spark.default.parallelism": "1000",
                    },
                },
            ),
        ),
    )

    # Call the function
    parse_teams.merge_team_execution_info(metadata, team_dict, gcp_team)

    actual_execution_info: ExecutionInfo = metadata.executionInfo

    expected_execution_info: ExecutionInfo = ExecutionInfo(
        env=EnvironmentVariables(
            common={
                # metadata common level
                "common_env_key": "common_value_override_env",
                # team common level
                "GCP_PROJECT_ID": "test_project",
                "GCP_REGION": "test_region",
                # default team common level
                "unique_key_default": "default_value",
            },
            modeEnvironments={
                RunMode.BACKFILL: {
                    # metadata common level
                    "common_env_key": "common_value_override_env",
                    # metadata mode level
                    "GCP_DATAPROC_CLUSTER_NAME": "special-backfill_cluster",
                    # team common level
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                    # team mode level
                    "GCP_CREATE_DATAPROC": "true",
                    "GCP_DATAPROC_NUM_WORKERS": "2",
                    "GCP_DATAPROC_MASTER_HOST_TYPE": "n2-highmem-8",
                    "GCP_DATAPROC_WORKER_HOST_TYPE": "n2-highmem-4",
                    "ARTIFACT_PREFIX": "some-path",
                    # default team common level
                    "unique_key_default": "default_value",
                    # default team mode level
                    "backfill-unique-key-default": "default_value",
                },
                RunMode.STREAMING: {
                    # metadata common level
                    "common_env_key": "common_value_override_env",
                    # metadata mode level
                    "GCP_DATAPROC_CLUSTER_NAME": "special-streaming_cluster",
                    # team common level
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                    # default team common level
                    "unique_key_default": "default_value",
                    # default team mode level
                    "streaming-unique-key-default": "default_value",
                },
                RunMode.METASTORE: {
                    # metadata common level
                    "common_env_key": "common_value_override_env",
                    # metadata mode level
                    "GCP_DATAPROC_CLUSTER_NAME": "default-metastore-cluster",
                    # team common level
                    "GCP_PROJECT_ID": "test_project",
                    "GCP_REGION": "test_region",
                    # default team common level
                    "unique_key_default": "default_value",
                    # default team mode level
                    "metastore-unique-key-default": "default_value",
                },
            },
        ),
        conf=ConfigProperties(
            common={
                # metadata common level
                "common_conf_key": "common_value_override_conf",
                # team common level
                "spark.chronon.cloud_provider": "gcp",
                # default team common level
                "unique_conf_key_default": "default_value",
            },
            modeConfigs={
                RunMode.BACKFILL: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # metadata conf level
                    "spark.chronon.partition.column": "notDs",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # team mode level
                    "backfill-unique-conf-key-gcp": "gcp_value",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                    # default team mode level
                    "backfill-unique-conf-key-default": "default_value",
                },
                RunMode.UPLOAD: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # metadata conf level
                    "spark.default.parallelism": "1000",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                },
                RunMode.STREAMING: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                    # default team mode level
                    "streaming-unique-conf-key-default": "default_value",
                },
                RunMode.METASTORE: {
                    # metadata common level
                    "common_conf_key": "common_value_override_conf",
                    # team common level
                    "spark.chronon.cloud_provider": "gcp",
                    # default team common level
                    "unique_conf_key_default": "default_value",
                    # default team mode level
                    "metastore-unique-conf-key-default": "default_value",
                },
            },
        ),
    )

    actual_env: EnvironmentVariables = actual_execution_info.env
    expected_env: EnvironmentVariables = expected_execution_info.env

    assert(actual_env.common == expected_env.common)
    assert(len(actual_env.modeEnvironments) == len(expected_env.modeEnvironments))
    for mode in actual_env.modeEnvironments:
        assert(actual_env.modeEnvironments[mode] == expected_env.modeEnvironments[mode])

    actual_conf: ConfigProperties = actual_execution_info.conf
    expected_conf: ConfigProperties = expected_execution_info.conf

    assert(actual_conf.common == expected_conf.common)
    assert(len(actual_conf.modeConfigs) == len(expected_conf.modeConfigs))
    for mode in actual_conf.modeConfigs:
        assert(actual_conf.modeConfigs[mode] == expected_conf.modeConfigs[mode])

    assert(actual_execution_info == expected_execution_info)
