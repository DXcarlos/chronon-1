from group_bys.gcp import dim_listings, dim_merchants, user_activities
from staging_queries.gcp import exports

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource
from ai.chronon.utils import get_staging_query_output_table_name

"""
This Join combines user activity events with:
1. User-level behavioral features (from user_activities GroupBy)
2. Listing-level attributes (from dim_listings GroupBy)

Left side: Raw user activity events
Right parts: 
- User behavioral aggregations (keyed by user_id)
- Listing dimension attributes (keyed by listing_id)
"""

# Left side: Raw user activity events from PubSub export
source = EventSource(
    # This will be the BigQuery table that receives the PubSub data
    table=get_staging_query_output_table_name(exports.user_activities, True),
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            row_id="event_id"
        ),
        time_column="event_time_ms",
    ),
)

# Join with user behavioral features and listing attributes
v1 = Join(
    left=source,
    row_ids=["event_id"], # TODO -- kill this once the SPJ API change goes through
    right_parts=[
        # User behavioral features (aggregated over time windows)
        JoinPart(
            group_by=user_activities.v1,
        ),
        # Listing dimension attributes (point-in-time lookup)
        JoinPart(
            group_by=dim_listings.v1,
        ),
        # Listing dimension attributes (point-in-time lookup)
        JoinPart(
            group_by=dim_merchants.v1,
            prefix="merchant_"
        ),
    ],
    version=0,
    online=True,
    output_namespace="data"
)
