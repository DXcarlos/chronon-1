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

import json

from gen_thrift.api.ttypes import Operation
from gen_thrift.common.ttypes import TimeUnit

from ai.chronon.repo.entity_register import Entity, EntityRegister, window_to_str_pretty, op_to_str
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource, EntitySource
from ai.chronon.group_by import GroupBy, Aggregation, Operation as AggOperation, TimeUnit as AggTimeUnit, Window
from ai.chronon.join import Join, JoinPart


class TestHelperFunctions:
    """Test helper functions for formatting."""

    def test_window_to_str_pretty_days(self):
        result = window_to_str_pretty(7, TimeUnit.DAYS)
        assert result == "7d"

    def test_window_to_str_pretty_hours(self):
        result = window_to_str_pretty(24, TimeUnit.HOURS)
        assert result == "24h"

    def test_window_to_str_pretty_minutes(self):
        result = window_to_str_pretty(30, TimeUnit.MINUTES)
        assert result == "30m"

    def test_op_to_str_sum(self):
        result = op_to_str(Operation.SUM)
        assert result == "sum"

    def test_op_to_str_count(self):
        result = op_to_str(Operation.COUNT)
        assert result == "count"

    def test_op_to_str_last(self):
        result = op_to_str(Operation.LAST)
        assert result == "last"

    def test_op_to_str_average(self):
        result = op_to_str(Operation.AVERAGE)
        assert result == "average"


class TestEntityIntegration:
    """Integration tests for Entity with Sources."""

    def test_entity_registration_with_entity_mapping(self):
        """Test that entity is registered when source uses entities parameter."""
        # Create entity and register
        user_entity = Entity(
            name="user",
            description="User entity for testing",
            default=["user_id"]
        )
        entity_register = EntityRegister()
        Entity._global_register = entity_register

        # Create source with entity mapping
        source = EventSource(
            table="data.user_events",
            query=Query(
                selects=selects(
                    user_id="user_id",
                    event_type="event_type"
                ),
                time_column="ts"
            ),
            entities={"user_id": user_entity}
        )

        # Verify entity was registered
        assert "user" in entity_register.entity_registrations
        assert entity_register.entity_registrations["user"] == user_entity

        # Verify entity has select registration for the table
        assert "data.user_events" in user_entity.select_registrations
        assert user_entity.select_registrations["data.user_events"].column == "user_id"
        assert user_entity.select_registrations["data.user_events"].expr == "user_id"

        # Create a GroupBy using the entity as a key
        group_by = GroupBy(
            sources=[source],
            keys=[user_entity],
            online=True,
            aggregations=[
                Aggregation(
                    input_column="event_type",
                    operation=AggOperation.COUNT,
                    windows=[Window(length=7, time_unit=AggTimeUnit.DAYS)]
                )
            ]
        )

        # Set the name on the metadata for Join to work
        group_by.metaData.name = "user_event_stats"

        # Verify aggregations are registered for the entity
        assert "data.user_events" in user_entity.aggregation_registrations
        assert len(user_entity.aggregation_registrations["data.user_events"]) == 1

        agg_info = user_entity.aggregation_registrations["data.user_events"][0]
        assert agg_info["groupBy"] == group_by
        assert agg_info["keys"] == ["user_id"]  # Should be the final key column
        assert len(agg_info["aggregations"]) == 1
        assert agg_info["aggregations"][0].inputColumn == "event_type"
        assert agg_info["aggregations"][0].operation == AggOperation.COUNT

        # Create a Join that uses the GroupBy
        join = Join(
            left=source,
            right_parts=[
                JoinPart(
                    group_by=group_by,
                    key_mapping={"user_id": "user_id"}
                )
            ],
            row_ids=None
        )

        # Set the name on the join metadata for export
        join.metaData.name = "user_features_join"

        # Verify feature query is registered for the entity
        assert "data.user_events" in user_entity.feature_query_registrations
        assert len(user_entity.feature_query_registrations["data.user_events"]) == 1

        query_info = user_entity.feature_query_registrations["data.user_events"][0]
        assert query_info["join"] == join
        assert query_info["table"] == "data.user_events"

        # Test export methods
        # Test pretty_print
        result = user_entity.pretty_print()
        assert "user" in result
        assert "User entity for testing" in result
        assert "data.user_events" in result
        assert "event_type" in result
        assert "Feature Queries:" in result
        assert "Join:" in result

        # Test to_json
        json_output = entity_register.to_json()
        json_data = json.loads(json_output)
        assert "entities" in json_data
        assert "user" in json_data["entities"]
        assert json_data["entities"]["user"]["name"] == "user"

        # Verify aggregation info is in JSON
        assert "feature_definitions" in json_data["entities"]["user"]
        assert "data.user_events" in json_data["entities"]["user"]["feature_definitions"]
        feature_def = json_data["entities"]["user"]["feature_definitions"]["data.user_events"]
        assert feature_def["input_column"] == "user_id"
        assert len(feature_def["group_bys"]) == 1
        assert len(feature_def["group_bys"][0]["aggregations"]) == 1
        assert feature_def["group_bys"][0]["aggregations"][0]["operation"] == "count"
        assert feature_def["group_bys"][0]["aggregations"][0]["windows"] == ["7d"]

        # Verify feature query (Join) info is in JSON
        assert "feature_queries" in json_data["entities"]["user"]
        assert "data.user_events" in json_data["entities"]["user"]["feature_queries"]
        assert len(json_data["entities"]["user"]["feature_queries"]["data.user_events"]) == 1

        # Test to_csv
        csv_output = entity_register.to_csv()
        assert "entity,input_column,operation,window,group_by,joins,online" in csv_output
        assert "user" in csv_output
        assert "user_id" in csv_output
        assert "count" in csv_output
        assert "7d" in csv_output
        assert "true" in csv_output  # online=True
        # CSV should include the join in the joins column (though join name might not be set)
        lines = csv_output.strip().split('\n')
        assert len(lines) >= 2  # Header + data

        # Cleanup
        Entity._global_register = None

    def test_entity_registration_with_entity_registry(self):
        """Test that entity is auto-registered when source uses entity_registry parameter."""
        # Create entity with default columns
        listing_entity = Entity(
            name="listing",
            description="Listing entity",
            default=["listing_id", "id_listing"]
        )
        entity_register = EntityRegister()
        entity_register.register_entity(listing_entity)
        Entity._global_register = entity_register

        # Create source with entity_registry - should auto-register based on default columns
        source = EntitySource(
            snapshot_table="data.listings",
            query=Query(
                selects=selects(
                    listing_id="listing_id",
                    title="title",
                    price="price"
                ),
                start_partition="2025-01-01"
            ),
            entity_registry=entity_register
        )

        # Verify entity has select registration for the table
        assert "data.listings" in listing_entity.select_registrations
        assert listing_entity.select_registrations["data.listings"].column == "listing_id"

        # Cleanup
        Entity._global_register = None
