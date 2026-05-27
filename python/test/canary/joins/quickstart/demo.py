from group_bys.quickstart import dim_listings, dim_merchants, user_activities
from staging_queries.quickstart import exports

from ai.chronon.types import Derivation, EventSource, GroupBy, Join, JoinPart, Query, selects

"""
This Join combines user activity events with:
1. User-level behavioral features (from user_activities GroupBy)
2. Listing-level attributes (from dim_listings GroupBy)
3. Merchant-level attributes (from dim_merchants GroupBy, keyed by merchant_id)

Left side: Raw user activity events from the local Iceberg table.

Note: dim_merchants is keyed by merchant_id. The left source includes merchant_id
(derived from the listing) so the join can resolve merchant attributes.
"""

# Left side: user activity events from the k8s Iceberg table
source = EventSource(
    table=exports.user_activities.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
#            merchant_id="merchant_id",  # needed to key into dim_merchants
            row_id="event_id",
        ),
        time_column="event_time_ms",
    ),
)

# Join with user behavioral features and listing/merchant attributes
event_enrichment = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        # User behavioral features (aggregated over time windows)
        JoinPart(
            group_by=user_activities.user_activity_features,
        ),
        # Listing dimension attributes (point-in-time lookup by listing_id)
        JoinPart(
            group_by=dim_listings.listing_features,
        ),
        # Merchant dimension attributes (point-in-time lookup by merchant_id)
 #       JoinPart(
 #           group_by=dim_merchants.merchant_features,
 #           prefix="merchant_",
 #       ),
    ],
    online=True,
    version=1,
    output_namespace="quickstart",
    step_days=5,
    enable_stats_compute=True,
)

# Join with derivations
derivations_v1 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(
            group_by=dim_listings.listing_features,
        ),
        JoinPart(
            group_by=user_activities.user_activity_features,
        ),
    ],
    derivations=[
        Derivation(
            name="is_listing_heavy",
            expression="IF(listing_id_weight_grams > 1000, 1, 0)"
        ),
        Derivation(
            name="is_item_handmade",
            expression="array_contains(split(listing_id_tags, ','), 'handmade')"
        ),
        Derivation(
            name="price_log",
            expression="log1p(listing_id_price_cents)"
        ),
        Derivation(
            name="price_bucket",
            expression="""
                CASE
                    WHEN listing_id_price_cents < 1000 THEN 0
                    WHEN listing_id_price_cents < 5000 THEN 1
                    WHEN listing_id_price_cents < 10000 THEN 2
                    WHEN listing_id_price_cents < 50000 THEN 3
                    ELSE 4
                END
            """
        ),
        Derivation(
            name="*",
            expression="*"
        ),
    ],
    online=True,
    output_namespace="quickstart",
    version=1,
    step_days=5,
    offline_schedule="0 3 * * *",
    online_schedule="0 3 * * *",
    enable_stats_compute=True,
)
