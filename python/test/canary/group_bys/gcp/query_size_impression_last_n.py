"""
Chronon GroupBy migrated from Tecton feature: query_size_impression_last_n_batch:v2_8_3

Original Tecton Feature:
- Feature view: query_size_impression_last_n_batch_v2_8_3
- Sources:
  * product_search_create_batch_v2_2_1 (search events)
  * product_create_batch_v2_2_1 (product dimensions)
  * top_n_queries_batch (common queries CSV)
- Keys: query (search query text)
- Aggregation: last(100) sizes over 90-day window
- Complex Transform Pipeline:
  1. Filter searches (organic, non-null fields)
  2. Standardize queries (text processing, join with top queries, impute)
  3. Explode results array to get ranked impressions
  4. Join product size from variants
  5. Remove self-interactions (seller != searcher)
  6. Group by (query, user, date) and get most common size
  7. Aggregate: last 100 sizes over 90 days

Chronon Migration:
- All preprocessing inlined into query_size_impression_preprocessed StagingQuery
- EventSource consumes the preprocessed data
- LAST_K(100) aggregation on size with 90d window
- Keys: ["query"]

Key Challenge Solved:
This feature demonstrates handling complex multi-source Tecton transforms with:
- 3 data sources (searches, products, top queries)
- 9 transformation steps with joins, filters, deduplication
- Array operations (explode, group aggregation)
- Helper function logic (filter_product_search_create, standardise_query,
  get_search_results_with_rank, get_product_size, remove_self_interactions,
  get_array_attribute_with_largest_count_over_group)
- All converted to SQL in a single StagingQuery
"""

from ai.chronon.group_by import GroupBy, Aggregation, Operation, Accuracy
from ai.chronon.source import EventSource
from ai.chronon.query import Query, selects

# Import the comprehensive preprocessing StagingQuery
from staging_queries.gcp.tecton_mocks import query_size_impression_preprocessed

v1 = GroupBy(
    sources=[
        EventSource(
            # Reference the preprocessed data
            table=query_size_impression_preprocessed.table,
            topic=None,  # Batch only
            query=Query(
                selects=selects(
                    "query",  # Search query text (entity key)
                    "size"    # Product size (S/M/L/XL/UNDEFINED)
                ),
                # Tecton timestamp_field: search_timestamp
                time_column="unix_millis(search_timestamp)"
            )
        )
    ],
    keys=["query"],  # Group by search query
    aggregations=[
        # Tecton: Aggregate last(100) over 90-day window
        # Chronon: LAST_K(100) with 90d window
        Aggregation(
            input_column="size",
            operation=Operation.LAST_K(100),
            windows=["90d"]
        )
    ],
    online=True,
    accuracy=Accuracy.TEMPORAL,
    version=1
)
