from group_bys.quickstart import dim_listings, user_activities
from staging_queries.quickstart import exports

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

"""
Join combining user activity events with user behavioral features,
listing attributes, and merchant attributes.

Left side: raw user activity events from the local Iceberg table.
"""

source = EventSource(
    table=exports.user_activities.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            row_id="event_id",
        ),
        time_column="event_time_ms",
    ),
)

v1 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=user_activities.v1),
        JoinPart(group_by=dim_listings.v1),
        # Uncomment to also join merchant attributes:
        # JoinPart(group_by=dim_merchants.v1, prefix="merchant_"),
    ],
    online=True,
    version=1,
    output_namespace="quickstart",
    step_days=5,
    enable_stats_compute=True,
)
