from group_bys.gcp import user_category_aggregates
from staging_queries.gcp import exports

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

user_category_features_join = Join(
    left=EventSource(
        table=exports.user_activities.table,
        topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
        query=Query(
            selects=selects(
                event_id="event_id",
                user_id="user_id",
            ),
            time_column="event_time_ms",
        ),
    ),
    right_parts=[
        JoinPart(group_by=user_category_aggregates.v1),
    ],
    row_ids=["event_id"],
    online=True,
    output_namespace="data",
    version=1,
)
