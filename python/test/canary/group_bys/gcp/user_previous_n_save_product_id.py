"""
Chronon GroupBy migrated from Tecton feature: user_previous_n_save_product_id:v2_6_2

Original Tecton Feature:
- Feature view: user_previous_n_save_product_id_v2_6_2
- Source: product_saves_create_batch_v2_2_2 (silver_union.product_saves_create)
- Keys: user_id
- Aggregation: last(10) product IDs over 7-day window
- Transform: Select user_id (int), event_time (timestamp), product_id (string)

Chronon Migration:
- Uses tecton_mock_product_saves_create StagingQuery (transforms demo.user-activities)
- EventSource with batch-only (topic=None)
- LAST_K(10) aggregation on product_id with 7d window
- Keys: ["user_id"]
"""

from ai.chronon.group_by import GroupBy, Aggregation, Operation, Accuracy
from ai.chronon.source import EventSource
from ai.chronon.query import Query, selects

# Import the mock Tecton source
from staging_queries.gcp.tecton_mocks import tecton_mock_product_saves_create

v1 = GroupBy(
    sources=[
        EventSource(
            # Reference the mock staging query's output table
            table=tecton_mock_product_saves_create.table,
            topic=None,  # Batch only (no streaming)
            query=Query(
                selects=selects(
                    "user_id",
                    "product_id"
                ),
                # Tecton timestamp_field: BACKEND_EVENT_TS → created_at in mock
                # Must be in milliseconds since epoch
                time_column="unix_millis(created_at)"
            )
        )
    ],
    keys=["user_id"],
    aggregations=[
        # Tecton: Aggregate last(10) over 7-day window
        # Chronon: LAST_K(10) with 7d window
        Aggregation(
            input_column="product_id",
            operation=Operation.LAST_K(10),
            windows=["7d"]
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL,  # Real-time accuracy
    version=1
)
