from staging_queries.gcp import exports
from group_bys.gcp import dim_listings

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

"""
This Join enriches user-activities stream with listing dimension data.
It adds the primary_category field from dim_listings to each user activity event.
This enriched stream will be used as input for downstream aggregations and ML models.
"""

# Left side: User activities stream (with both batch table and streaming topic)
left_source = EventSource(
    table=exports.user_activities.table,
    topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            event_type="event_type",
            event_id="event_id"
        ),
        time_column="event_time_ms"  # Already in milliseconds
    )
)

# Parent join that enriches activities with listing category
parent_join = Join(
    left=left_source,
    row_ids=["event_id"],  # Required for compilation
    right_parts=[
        # Join with dim_listings to get primary_category
        JoinPart(group_by=dim_listings.v1)
    ],
    online=True,
    output_namespace="data",
    version=1
)
