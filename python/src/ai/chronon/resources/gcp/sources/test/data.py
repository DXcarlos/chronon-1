from gen_thrift.api.ttypes import EventSource, Source

from ai.chronon.query import Query, selects

"""
Example: Defining a Chronon Source from a Batch Table

This example demonstrates how to configure a Chronon `Source` from a BigQuery or Hive table,
with a clear event time column and selected fields for downstream feature computation.
"""

# Define the EventSource using the batch table and query
# Wrap the EventSource in a Source object

source_v1 = Source(
    events=EventSource(
        table="data.purchases",  # This points to the log table in the warehouse with historical purchase events, updated in batch daily
        topic=None,  # See the 'returns' GroupBy for an example that has a streaming source configured. In this case, this would be the streaming source topic that can be listened to for realtime events
        query=Query(
            selects=selects("user_id", "purchase_price"),  # Select the fields we care about
            time_column="ts",
        ),  # The event time
    )
)

# The `source_v1` object can now be used in a Chronon join or pipeline definition
