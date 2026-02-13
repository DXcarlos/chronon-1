from gen_thrift.api.ttypes import EventSource, Source
from group_bys.test.data import group_by_v1

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
            time_column="ts",
            start_partition="2023-11-01",
        ),  # The event time used to compute feature values as-of
    )
)

v1 = Join(
    left=source,
    right_parts=[JoinPart(group_by=group_by_v1)],
    row_ids="user_id",
    version=0,
)
