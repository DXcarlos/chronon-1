from ai.chronon.types import EventSource, Query, selects

"""
Example: Defining a Chronon Source from a Batch Table

This example demonstrates how to configure a Chronon `Source` from a BigQuery or Hive table,
with a clear event time column and selected fields for downstream feature computation.
"""

purchase_events = EventSource(
    table="data.purchases",  # This points to the log table in the warehouse with historical purchase events, updated in batch daily
    query=Query(
        selects=selects("user_id", "purchase_price"),  # Select the fields we care about
        time_column="ts",
        start_partition="2023-11-01",
    ),
)

# The `purchase_events` object can now be used in a Chronon join or pipeline definition
