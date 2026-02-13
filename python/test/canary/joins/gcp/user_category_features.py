from staging_queries.gcp import exports
from group_bys.gcp import user_category_aggregates

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

"""
This Join brings together user activity events with the aggregated
event-category patterns (last 100 pairs over 7 days).

This join serves as input for the ModelTransforms that applies
gemini-embedding-001 to generate user behavior embeddings.
"""

left_source = EventSource(
    table=exports.user_activities.table,
    topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
    query=Query(
        selects=selects(
            user_id="user_id",
            event_id="event_id"
        ),
        time_column="event_time_ms"
    )
)

user_category_features_join = Join(
    left=left_source,
    row_ids=["event_id"],
    right_parts=[
        # Include the aggregated event-category patterns
        JoinPart(group_by=user_category_aggregates.v1)
    ],
    online=True,
    output_namespace="data",
    version=1
)
