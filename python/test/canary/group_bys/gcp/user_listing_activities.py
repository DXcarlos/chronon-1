from gen_thrift.api.ttypes import EventSource, Source
from staging_queries.gcp import exports

from ai.chronon.group_by import Accuracy, Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects

"""
This GroupBy aggregates user-listing pair activity metrics from the user-activities table.
It tracks interactions between specific users and specific listings with features like:
- Event type counts per user-listing pair
- Time since last interaction
- Frequency of interactions
- Device diversity for user-listing pairs
- Recent interaction patterns
"""

source = Source(
    events=EventSource(
        table=exports.user_activities.table,
        topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                # Event time for tracking last interaction
                event_time_ms="event_time_ms",
                # Create binary flags for each event type
                view_event="IF(event_type = 'view', 1, 0)",
                click_event="IF(event_type = 'click', 1, 0)",
                purchase_event="IF(event_type = 'purchase', 1, 0)",
                favorite_event="IF(event_type = 'favorite', 1, 0)",
                add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
                # Device type for diversity tracking
                device_type="device_type",
                # Country for diversity tracking
                country_code="country_code",
                # Interaction struct for last_k tracking
                interaction_struct="STRUCT(event_type, device_type, country_code, unix_millis(TIMESTAMP(event_time_ms)) as timestamp)",
            ),
            time_column="unix_millis(TIMESTAMP(event_time_ms))",
        ),
    )
)

# Define window sizes for aggregations (1d, 7d, 14d, 30d)
window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]

aggregations = [
    # Event type counts - track frequency of each event type per user-listing pair
    Aggregation(input_column="view_event", operation=Operation.SUM, windows=window_sizes),
    Aggregation(input_column="click_event", operation=Operation.SUM, windows=window_sizes),
    Aggregation(input_column="purchase_event", operation=Operation.SUM, windows=window_sizes),
    Aggregation(input_column="favorite_event", operation=Operation.SUM, windows=window_sizes),
    Aggregation(input_column="add_to_cart_event", operation=Operation.SUM, windows=window_sizes),

    # Total interaction frequency - count of all interactions per user-listing pair
    Aggregation(input_column="event_time_ms", operation=Operation.COUNT, windows=window_sizes),

    # Time since last interaction - captures the most recent event time
    Aggregation(input_column="event_time_ms", operation=Operation.LAST, windows=window_sizes),

    # Device diversity - track how many unique devices were used for this user-listing pair
    Aggregation(input_column="device_type", operation=Operation.APPROX_UNIQUE_COUNT_LGK(8), windows=window_sizes),

    # Country diversity - track how many unique countries for this user-listing pair
    Aggregation(input_column="country_code", operation=Operation.APPROX_UNIQUE_COUNT_LGK(8), windows=window_sizes),

    # Recent interaction patterns - keep track of last 10 interactions
    Aggregation(input_column="interaction_struct", operation=Operation.LAST_K(10), windows=window_sizes),
]

v1 = GroupBy(
    sources=[source],
    keys=["user_id", "listing_id"],  # Aggregate by user-listing pairs
    online=True,
    aggregations=aggregations,
    accuracy=Accuracy.TEMPORAL,  # Real-time accuracy for streaming
)
