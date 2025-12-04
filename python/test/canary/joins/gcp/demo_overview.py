from group_bys.gcp import dim_listings, dim_merchants, user_activities
from staging_queries.gcp import exports

from ai.chronon.join import Derivation, Join, JoinPart, ExternalPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource
from external_sources import sift_score

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
v1 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        # User behavioral features (aggregated over time windows)
        JoinPart(
            group_by=user_activities.v1,
        ),
        # Listing features
        JoinPart(
            group_by=dim_listings.v1,
        ),
        # Merchant features
        JoinPart(
            group_by=dim_merchants.v1,
            prefix="merchant_"
        ),
    ],
    version=1,
    online=True,
    output_namespace="data",
    step_days=10,
)

"""
# External API calls
online_external_parts=[
    ExternalPart(sift_score.v0)
],
# Read time transformations
derivations=[
    # Normalize sift score
    Derivation(
        name="sift_score_normalized",
        expression="ip_sift_score / ip_sift_score_avg_30d"
    ),
    # Carry-through of base features
    Derivation(
        name="*",
        expression="*"
    )
],
"""