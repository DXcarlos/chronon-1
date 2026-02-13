from joins.gcp import user_category_parent

from ai.chronon.group_by import Accuracy, Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
This chained GroupBy aggregates enriched user activity data.
It uses the parent_join (user-activities enriched with listing categories) as input
and aggregates by user to track event_type and primary_category patterns.

The last 100 (event_type, primary_category) pairs are captured over a 7-day window
for downstream ML model input (embeddings).
"""

# JoinSource wraps the parent join
source = JoinSource(
    join=user_category_parent.parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            event_type="event_type",
            primary_category="listing_id_primary_category",  # Prefixed with key name from join
            # Create a struct combining event_type and primary_category for aggregation
            event_category_pair="STRUCT(event_type, listing_id_primary_category as primary_category)"
        ),
        time_column="ts"
    )
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        # Last 100 event_type and primary_category pairs over 7 days
        Aggregation(
            input_column="event_category_pair",
            operation=Operation.LAST_K(100),
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)]
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL,
    version=1
)
