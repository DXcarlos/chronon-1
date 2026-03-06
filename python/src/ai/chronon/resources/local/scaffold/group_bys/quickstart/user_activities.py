from staging_queries.quickstart import exports

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource
from ai.chronon.types import EnvironmentVariables

"""
GroupBy aggregating user activity metrics from the user_activities table.
Tracks views, clicks, purchases, favorites, and add-to-cart events
with SUM and AVERAGE aggregations over 1d, 7d, 14d, and 30d windows.
"""

source = EventSource(
    table=exports.user_activities.table,
    topic="user_activities_stream",
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            view_event="IF(event_type = 'view', 1, 0)",
            click_event="IF(event_type = 'click', 1, 0)",
            purchase_event="IF(event_type = 'purchase', 1, 0)",
            favorite_event="IF(event_type = 'favorite', 1, 0)",
            add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
            is_mobile="IF(device_type = 'mobile', 1, 0)",
            is_desktop="IF(device_type = 'desktop', 1, 0)",
            user_event_struct="struct(event_type, listing_id, event_time_ms as timestamp)",
        ),
        time_column="event_time_ms",
    ),
)

window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]
event_columns = ["view_event", "click_event", "purchase_event", "favorite_event", "add_to_cart_event"]

aggregations = (
    [Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes) for col in event_columns]
    + [Aggregation(input_column=col, operation=Operation.AVERAGE, windows=window_sizes) for col in event_columns]
    + [Aggregation(input_column="user_event_struct", operation=Operation.LAST_K(128), windows=window_sizes)]
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    version=1,
    aggregations=aggregations,
    step_days=4,
    env_vars=EnvironmentVariables(common={"CHRONON_ONLINE_ARGS": "-Ztasks=1"}),
)
