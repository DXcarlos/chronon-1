from sources.test.data import source_v1

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window

window_sizes = [
    Window(length=day, time_unit=TimeUnit.DAYS) for day in [3, 14, 30]
]  # Define some window sizes to use below

group_by_v1 = GroupBy(
    sources=[source_v1],
    keys=["user_id"],  # We are aggregating by user
    online=True,
    aggregations=[
        Aggregation(
            input_column="purchase_price", operation=Operation.SUM, windows=window_sizes
        ),  # The sum of purchases prices in various windows
        Aggregation(
            input_column="purchase_price", operation=Operation.COUNT, windows=window_sizes
        ),  # The count of purchases in various windows
        Aggregation(
            input_column="purchase_price", operation=Operation.AVERAGE, windows=window_sizes
        ),  # The average purchases by user in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.LAST_K(10),
        ),
    ],
    version=0,
)
