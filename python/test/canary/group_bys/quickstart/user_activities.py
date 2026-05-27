from staging_queries.quickstart import exports

from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects
from ai.chronon.types import EnvironmentVariables

"""
This GroupBy aggregates user activity metrics from the user_activities table.
It tracks various user behaviors (views, clicks, purchases, favorites, add_to_cart)
with last_k, sum, and average aggregations over multiple time windows.

event_time_ms is already in milliseconds — no conversion needed (unlike GCP/BigQuery).
"""

source = EventSource(
    table=exports.user_activities.table,
    topic="user_activities_stream",
    query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                # Create binary flags for each event type
                view_event="IF(event_type = 'view', 1, 0)",
                click_event="IF(event_type = 'click', 1, 0)",
                purchase_event="IF(event_type = 'purchase', 1, 0)",
                favorite_event="IF(event_type = 'favorite', 1, 0)",
                add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
                # Device type flags
                is_mobile="IF(device_type = 'mobile', 1, 0)",
                is_desktop="IF(device_type = 'desktop', 1, 0)",
                is_tablet="IF(device_type = 'tablet', 1, 0)",
                # Activity struct for last_k tracking
                user_event_struct="struct(event_type, listing_id, event_time_ms as timestamp)",
            ),
            time_column="event_time_ms",  # Already in milliseconds
    ),
)

# Define window sizes for aggregations (1d, 7d, 14d, 30d)
window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]

event_columns = ["view_event", "click_event", "purchase_event", "favorite_event", "add_to_cart_event"]
device_columns = ["is_mobile", "is_desktop", "is_tablet"]

aggregations = []

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.AVERAGE, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in device_columns
])

aggregations.append(
    Aggregation(input_column="user_event_struct", operation=Operation.LAST_K(128), windows=window_sizes)
)

user_activity_features = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    version=1,
    aggregations=aggregations,
    step_days=4,
    env_vars=EnvironmentVariables(
        common={
            "CHRONON_ONLINE_ARGS": "-Ztasks=1",
        }
    ),
)
