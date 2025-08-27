from joins.sample_team.sample_chaining_join_parent import parent_join

from ai.chronon.types import Accuracy, Aggregation, GroupBy, JoinSource, Operation, Query, selects

chaining_group_by_v1 = GroupBy(
    sources=JoinSource(
        join=parent_join,
        query=Query(
            selects=selects(
                event="event_expr",
                group_by_subject="group_by_expr",
            ),
            start_partition="2023-04-15",
            time_column="ts",
        ),
    ),
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="event", operation=Operation.LAST),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    production=True,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description",
    },
    output_namespace="sample_namespace",
    version=0,
)
