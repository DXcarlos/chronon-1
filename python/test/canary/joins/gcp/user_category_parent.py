from gen_thrift.api.ttypes import EventSource, Source
from staging_queries.gcp import exports
from group_bys.gcp import dim_listings

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
Parent Join that enriches user activities with listing dimension data in real-time.
This provides the primary_category field needed for downstream aggregations.

This Join uses streaming (PubSub topic) for real-time updates and is used as a
source for chained GroupBys that want to aggregate user behavior by listing
attributes (like primary_category).
"""

# Parent Join: Enrich user activities with listing dimensions (streaming enabled)
parent_join = Join(
    left=Source(
        events=EventSource(
            # BigQuery table for batch/backfill
            table=exports.user_activities.table,
            # PubSub topic for real-time streaming
            topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
            query=Query(
                selects=selects(
                    user_id="user_id",
                    listing_id="listing_id",
                    event_type="event_type",
                    event_id="event_id",
                ),
                time_column="unix_millis(TIMESTAMP(event_time_ms))",
            ),
        )
    ),
    row_ids=["event_id"],
    right_parts=[
        # Join with dim_listings to get primary_category and other listing attributes
        JoinPart(
            group_by=dim_listings.v1,
            key_mapping={"listing_id": "listing_id"}
        )
    ],
    online=True,
    output_namespace="data",
    version=0
)

# JoinSource wraps the parent join for use in downstream chained GroupBys
# This exports the enriched events with both activity and listing dimension fields
upstream_join_source = JoinSource(
    join=parent_join,
    query=Query(
        selects=selects(
            user_id="user_id",
            event_type="event_type",
            # Column from dim_listings GroupBy (prefixed with key name)
            primary_category="listing_id_primary_category",
        ),
        time_column="ts"
    )
)
