from group_bys.gcp import purchases

from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects(
                "user_id"
            ),  # The primary key used to join various GroupBys together
            start_partition="2025-07-01",
            time_column="ts",
        ),  # The event time used to compute feature values as-of
    )
)

fraud_detection = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.features)
    ],
    version=0,
)
