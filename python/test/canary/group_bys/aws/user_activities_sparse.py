from gen_thrift.api.ttypes import EventSource, Source

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects

source = Source(
    events=EventSource(
        table="demo.user_activities_raw_sparse",
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                view_event="IF(event_type = 'view', 1, 0)",
                click_event="IF(event_type = 'click', 1, 0)",
                purchase_event="IF(event_type = 'purchase', 1, 0)",
            ),
            time_column="unix_millis(TIMESTAMP(event_time_ms))",
            sparse=True,
        ),
    )
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=False,
    version=1,
    aggregations=[
        Aggregation(input_column="view_event", operation=Operation.SUM, windows=[Window(length=7, time_unit=TimeUnit.DAYS)]),
        Aggregation(input_column="click_event", operation=Operation.SUM, windows=[Window(length=7, time_unit=TimeUnit.DAYS)]),
        Aggregation(input_column="purchase_event", operation=Operation.SUM, windows=[Window(length=7, time_unit=TimeUnit.DAYS)]),
    ],
    step_days=30,
)
