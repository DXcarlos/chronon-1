from group_bys.aws import dim_listings, user_activities_sparse
from staging_queries.aws import exports

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

# Left side: sparse user activity events (only 5 partitions out of 20)
source = EventSource(
    table=exports.user_activities_sparse.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            row_id="event_id",
        ),
        time_column="event_time_ms",
    ),
)

v1 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=user_activities_sparse.v1),
        JoinPart(group_by=dim_listings.v1),
    ],
    version=0,
    online=False,
    output_namespace="data",
    step_days=30,
)
