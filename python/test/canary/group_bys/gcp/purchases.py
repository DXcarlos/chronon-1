from gen_thrift.api.ttypes import EventSource, Source
from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""

# This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
source = Source(
    events=EventSource(
        table="data.purchases", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
        topic=None, # See the 'returns' GroupBy for an example that has a streaming source configured. In this case, this would be the streaming source topic that can be listened to for realtime events
        query=Query(
            selects=selects("user_id","purchase_price"), # Select the fields we care about
            start_partition="2023-11-01",
            time_column="ts") # The event time
    ))

window_sizes = [Window(length=day, time_unit=TimeUnit.DAYS) for day in [1, 3, 7]] # Define some window sizes to use below


v1_dev = GroupBy(
    backfill_start_date="2023-11-01",
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

v1_test = GroupBy(
    backfill_start_date="2023-11-01",
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

# This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
source_notds = Source(
    events=EventSource(
        table="data.purchases_notds", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
        topic=None, # See the 'returns' GroupBy for an example that has a streaming source configured. In this case, this would be the streaming source topic that can be listened to for realtime events
        query=Query(
            selects=selects("user_id","purchase_price"), # Select the fields we care about
            time_column="ts",
            start_partition="2023-11-01",
            partition_column="notds"
        ) # The event time
    ))

v1_test_notds = GroupBy(
    backfill_start_date="2023-11-01",
    sources=[source_notds],
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

v1_dev_notds = GroupBy(
    backfill_start_date="2023-11-01",
    sources=[source_notds],
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

