from group_bys.test import bad_key
from staging_queries.test import user_activities

from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource
from ai.chronon.utils import get_staging_query_output_table_name

# Left side: Raw user activity events from PubSub export
source = EventSource(
    # This will be the BigQuery table that receives the PubSub data
    table=get_staging_query_output_table_name(user_activities.v0, True),
    query=Query(
        selects=selects(
            user_id="user_id", # user ID here is a string
            row_id="row_id",
            ds="ds",
        ),
        time_column="ts",
    ),
)

# Join with user behavioral features and listing attributes
v1 = Join(
    left=source,
    row_ids=["row_id"],
    right_parts=[
        # User behavioral features (aggregated over time windows)
        JoinPart(
            group_by=bad_key.v0,
        ),
    ],
    version=0
)
