"""
The log flattener job requires a schema table to query.
The schema table provides the latest schema based on the schema hash (stored in keyBytes)

This implementation assumes a pubsub bigQuery subscription to create the loggable_response table.
The fields are stored as bytes and such the schema can be decoded by casting to string.
"""
from ai.chronon.types import Accuracy, Aggregation, EventSource, GroupBy, Operation, Query, selects
import staging_queries.azure.partitioned_logging as partitioned_logging

source = EventSource(
    table=partitioned_logging.partitioned_loggable_response.table,
    query=Query(
        selects=selects(
            schema_hash="CAST(keyBytes AS STRING)",
            schema_value="CAST(valueBytes AS STRING)"
        ),
        start_partition="2025-09-23",
        partition_column="ds",
        wheres=["name='SCHEMA_PUBLISH_EVENT'"],
        time_column="ts_millis",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["schema_hash"],
    aggregations=[Aggregation(input_column="schema_value", operation=Operation.LAST)],
    accuracy=Accuracy.SNAPSHOT,
    version=2,
)
