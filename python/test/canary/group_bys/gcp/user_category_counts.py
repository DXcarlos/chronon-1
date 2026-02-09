from joins.gcp import user_category_parent

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
Chained GroupBy that aggregates user activity counts by primary_category.
This uses the user_category_parent Join as a source to count how many times
a user has "viewed" or "purchased" items in each primary_category.

The aggregations are bucketed by primary_category, creating a map of
category -> count for each user.
"""

# Create a JoinSource that wraps the parent join and adds transformations
source = JoinSource(
    join=user_category_parent.parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            event_type="event_type",
            primary_category="listing_id_primary_category",
            # Create binary flags for view and purchase events
            view_event="IF(event_type = 'view', 1, 0)",
            purchase_event="IF(event_type = 'purchase', 1, 0)",
        ),
        time_column="ts"
    )
)

# Define aggregations with buckets by primary_category
window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [7, 14, 30]]

aggregations = [
    # Count views by primary_category - creates map<primary_category, count>
    Aggregation(
        input_column="view_event",
        operation=Operation.SUM,
        windows=window_sizes,
        buckets=["primary_category"]
    ),
    # Count purchases by primary_category - creates map<primary_category, count>
    Aggregation(
        input_column="purchase_event",
        operation=Operation.SUM,
        windows=window_sizes,
        buckets=["primary_category"]
    ),
]

chained_gb = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=aggregations,
    online=True,
    version=0
)
