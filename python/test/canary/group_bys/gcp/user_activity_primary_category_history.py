from joins.gcp import user_activity_primary_category

from ai.chronon.group_by import Accuracy, Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
Per-user history of recent event type and primary category pairs from the enriched
activity stream.
"""

source = JoinSource(
    join=user_activity_primary_category.enriched_stream_v1,
    query=Query(
        selects=selects(
            user_id="user_id",
            event_category_pair="concat_ws('|', coalesce(event_type, 'unknown_event'), coalesce(listing_id_primary_category, 'unknown_category'))",
        ),
        time_column="ts",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="event_category_pair",
            operation=Operation.LAST_K(100),
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)],
        ),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    output_namespace="data",
    step_days=7,
    version=1,
)


