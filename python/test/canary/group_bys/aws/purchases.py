from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""

# This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
source = EventSource(
    table="data.purchases", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
    query=Query(
        selects=selects("user_id","purchase_price"), # Select the fields we care about
        start_partition="2023-11-01",
        time_column="ts") # The event time
)

window_sizes = [Window(length=day, time_unit=TimeUnit.DAYS) for day in [1, 3, 7]] # Define some window sizes to use below

purchase_features_dev = GroupBy(
    sources=[source],
    keys=["user_id"], # We are aggregating by user
    online=True,
    version=0,
    aggregations=[Aggregation(
        input_column="purchase_price",
        operation=Operation.SUM,
        windows=window_sizes
    ), # The sum of purchases prices in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ), # The count of purchases in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ), # The average purchases by user in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.LAST_K(10),
        ),
    ],
)

purchase_features_test = GroupBy(
    sources=[source],
    keys=["user_id"], # We are aggregating by user
    online=True,
    version=0,
    aggregations=[Aggregation(
        input_column="purchase_price",
        operation=Operation.SUM,
        windows=window_sizes
    ), # The sum of purchases prices in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ), # The count of purchases in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ), # The average purchases by user in various windows
        Aggregation(
            input_column="purchase_price",
            operation=Operation.LAST_K(10),
        ),
    ],
)
