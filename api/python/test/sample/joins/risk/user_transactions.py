from group_bys.risk.merchant_data import merchant_group_by
from group_bys.risk.transaction_events import txn_group_by_merchant, txn_group_by_user
from group_bys.risk.user_data import user_group_by

from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects

source_users = Source(
    events=EventSource(
        table="data.users", query=Query(selects=selects("user_id"), time_column="ts")
    )
)

txn_join = Join(
    left=source_users,
    row_ids="user_id",
    right_parts=[
        JoinPart(group_by=txn_group_by_user, prefix="user"),
        JoinPart(group_by=txn_group_by_merchant, prefix="merchant"),
        JoinPart(group_by=user_group_by, prefix="user"),
        JoinPart(group_by=merchant_group_by, prefix="merchant"),
    ],
    version=0,
)
