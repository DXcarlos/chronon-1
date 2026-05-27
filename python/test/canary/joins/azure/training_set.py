from group_bys.azure import purchases

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = EventSource(
    table="data.checkouts",
    query=Query(
        selects=selects(
            "user_id"
        ),  # The primary key used to join various GroupBys together
        time_column="ts",
    ),
)

training_join_test = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.purchase_features_test)
    ],
    version=0,
)

training_join_hub = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.purchase_features_test)
    ],
    version=0,
)

training_join_dev = Join(
    left=source,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=purchases.purchase_features_dev)
    ],
    version=0,
)

source_notds = EventSource(
    table="data.checkouts_notds",
    query=Query(
        selects=selects(
            "user_id"
        ),  # The primary key used to join various GroupBys together
        time_column="ts",
        partition_column="notds",
    ),
)

training_join_test_notds = Join(
    left=source_notds,
    row_ids=["user_id"],
    right_parts=[
        JoinPart(group_by=purchases.purchase_features_test_notds)
    ],
    version=0,
)

training_join_dev_notds = Join(
    left=source_notds,
    row_ids=["user_id"],
    right_parts=[
        JoinPart(group_by=purchases.purchase_features_dev_notds)
    ],
    version=0,
)
