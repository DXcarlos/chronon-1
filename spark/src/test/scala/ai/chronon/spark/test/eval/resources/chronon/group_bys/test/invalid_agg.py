"""
GroupBy aggregating user page activity from the staging query
"""

from ai.chronon.source import EventSource
from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query
from ai.chronon.utils import get_staging_query_output_table_name
from staging_queries.test import user_activities

# GroupBy that aggregates user page activity


source = EventSource(
    table=get_staging_query_output_table_name(user_activities.v0, True),
    query=Query(
        selects=dict(
            user_id="user_id",
            clicks="CAST(click_event as STRING)", # Casting to string to force an invalid input to sum
            purchases="purchase_event",
        ),
        time_column="ts"
    )
)


aggregations = []

window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7]]

# Event type aggregations - Sum and Average over various windows
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in ["clicks", "purchases"]
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.LAST_K(10), windows=window_sizes)
    for col in ["clicks", "purchases"]
])

v0 = GroupBy(
    sources=[source],
    keys=["user_id"],  # Aggregate by user
    online=True,
    version=0,
    aggregations=aggregations,
)
