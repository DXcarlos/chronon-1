from staging_queries.aws import exports

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.source import EventSource

source = EventSource(
    table=exports.sparse_activities.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            activity_id="activity_id",
            activity_type="activity_type",
            region="region",
            platform="platform",
            duration_seconds="duration_seconds",
            # Flags for derived features
            is_error="IF(error_code IS NOT NULL AND error_code != 0, 1, 0)",
            is_mobile="IF(platform = 'mobile', 1, 0)",
            is_long_activity="IF(duration_seconds > 300, 1, 0)",
        ),
        time_column="ts",
    ),
)

windows_1d_7d = [Window(length=d, time_unit=TimeUnit.DAYS) for d in [1, 7]]

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        # Activity volume
        Aggregation(input_column="activity_id", operation=Operation.COUNT, windows=windows_1d_7d),
        # Duration aggregations
        Aggregation(input_column="duration_seconds", operation=Operation.SUM, windows=windows_1d_7d),
        Aggregation(input_column="duration_seconds", operation=Operation.AVERAGE, windows=windows_1d_7d),
        # Error rate signal
        Aggregation(input_column="is_error", operation=Operation.SUM, windows=windows_1d_7d),
        Aggregation(input_column="is_error", operation=Operation.AVERAGE, windows=windows_1d_7d),
        # Platform breakdown
        Aggregation(input_column="is_mobile", operation=Operation.SUM, windows=windows_1d_7d),
        # Long activity count
        Aggregation(input_column="is_long_activity", operation=Operation.SUM, windows=windows_1d_7d),
        # Recent activity types
        Aggregation(input_column="activity_type", operation=Operation.LAST_K(10), windows=windows_1d_7d),
    ],
    online=True,
    version=1,
)
