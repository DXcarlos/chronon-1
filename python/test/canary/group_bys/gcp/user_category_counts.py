from joins.gcp import user_category_parent

from ai.chronon.group_by import Accuracy, Aggregation, GroupBy, Operation
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
Chained GroupBy that aggregates user viewing and purchasing behavior by category.
This GroupBy uses the enriched user activity events (with primary_category)
to count how many times each user has viewed or purchased items in each category.
"""

# Create JoinSource with transformations in the chained GroupBy
source = JoinSource(
    join=user_category_parent.parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            event_type="event_type",
            primary_category="listing_id_primary_category",
            # Transformations done here in the chained GroupBy's source
            is_view="IF(event_type = 'view', 1, 0)",
            is_purchase="IF(event_type = 'purchase', 1, 0)"
        ),
        time_column="ts"
    )
)

chained_gbx = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        # Count views by category - bucketed by primary_category
        Aggregation(
            input_column="is_view",
            operation=Operation.SUM,
            windows=["7d", "30d"],
            buckets=["primary_category"]  # Creates map<category, count>
        ),
        # Count purchases by category - bucketed by primary_category
        Aggregation(
            input_column="is_purchase",
            operation=Operation.SUM,
            windows=["7d", "30d"],
            buckets=["primary_category"]  # Creates map<category, count>
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL
)
