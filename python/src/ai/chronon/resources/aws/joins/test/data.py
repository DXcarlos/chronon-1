from group_bys.test.data import purchase_features

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = EventSource(
    table="data.checkouts",
    query=Query(
        selects=selects("user_id"),  # The primary key used to join various GroupBys together
        time_column="ts",
        start_partition="2023-11-01",
    ),
)

training_join = Join(
    left=source,
    right_parts=[JoinPart(group_by=purchase_features)],
    row_ids="user_id",
    version=0,
)
