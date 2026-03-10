from group_bys.gcp import dim_listings
from staging_queries.gcp import exports

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

parent_join = Join(
    left=EventSource(
        # Batch table is the Iceberg export of the raw BigQuery table.
        # BigQuery (and other external warehouse tables) can't be referenced directly
        # as Spark tables — they must go through a StagingQuery export first.
        table=exports.user_activities.table,
        topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
        query=Query(
            selects=selects(
                event_id="event_id",
                user_id="user_id",
                listing_id="listing_id",
                event_type="event_type",
            ),
            time_column="event_time_ms",
        ),
    ),
    right_parts=[
        JoinPart(group_by=dim_listings.v1),
    ],
    row_ids=["event_id"],
    online=True,
    output_namespace="data",
    version=1,
)
