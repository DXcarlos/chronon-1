from group_bys.gcp import dim_listings, dim_merchants, user_activities, dim_users
from staging_queries.gcp import exports

from ai.chronon.join import Derivation, Join, JoinPart, ExternalPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource
from external_sources import sift_score

"""
This Join combines user activity events with:
1. User-level behavioral features (from user_activities GroupBy)
2. Listing-level attributes (from dim_listings GroupBy)

Left side: Purchase events
Right parts: 
- User behavioral aggregations (keyed by user_id)
- Listing dimension attributes (keyed by listing_id)
"""

# Left side: Raw user activity events from PubSub export
source = EventSource(
    # This will be the BigQuery table that receives the PubSub data
    table=exports.purchase_events.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
        ),
        time_column="event_time_ms",
    ),
)

# Example Join
checkout_fraud_v2 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(
            group_by=user_activities.v1,
        ),
        # Listing features
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    version=1,
    online=True,
    output_namespace="data",
    step_days=10,
    enable_stats_compute=True,
)


# Updated to user the new user_activities features
checkout_fraud_v2 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(
            group_by=user_activities.v2,
        ),
        # Listing features
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    derivations = [
        Derivation(
            name="ratio_of_purchases_to_views",
            expression="user_id_click_event_sum_1d / user_id_view_event_sum_7d"
        ),
    ],
    version=1,
    online=True,
    output_namespace="data",
    step_days=10,
    enable_stats_compute=True,
)