from staging_queries.gcp import exports
from group_bys.gcp import dim_listings

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource, JoinSource

"""
Parent Join that enriches user activity events with listing information.
This join brings in primary_category from dim_listings to enable downstream
category-based aggregations.
"""

# Left source: user activity events
event_source = EventSource(
    table=exports.user_activities.table,
    topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            event_type="event_type",
            event_id="event_id"
        ),
        time_column="unix_millis(TIMESTAMP(event_time_ms))"
    )
)

# Parent join enriches events with listing dimensions
parent_join = Join(
    left=event_source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=dim_listings.v1)
    ],
    online=True,
    output_namespace="data"
)

# Simple JoinSource - transformations will be done in the chained GroupBy
upstream_join_source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            event_type="event_type",
            # Column from dim_listings comes in as listing_id_primary_category
            primary_category="listing_id_primary_category"
        ),
        time_column="ts"
    )
)
