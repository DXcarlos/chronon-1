from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects

"""
GroupBy over demo.sparse_activities, an AWS Glue/Iceberg table with data
every other day.  Used by integration tests to exercise backfill behaviour
when source partitions are missing for some days in the requested range.
"""

source = EventSource(
    table="demo.sparse_activities",
    query=Query(
        selects=selects(
            user_id="user_id",
            duration_seconds="duration_seconds",
        ),
        time_column="ts",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=False,
    version=0,
    aggregations=[
        Aggregation(
            input_column="duration_seconds",
            operation=Operation.SUM,
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)],
        ),
        Aggregation(
            input_column="duration_seconds",
            operation=Operation.COUNT,
            windows=[Window(length=7, time_unit=TimeUnit.DAYS)],
        ),
    ],
    output_namespace="data",
)
