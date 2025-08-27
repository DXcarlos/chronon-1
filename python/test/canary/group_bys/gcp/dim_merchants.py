from staging_queries.gcp import exports

from ai.chronon.group_by import GroupBy
from ai.chronon.query import Query, selects
from ai.chronon.source import EntitySource
from ai.chronon.utils import get_staging_query_output_table_name

"""
This GroupBy creates a simple passthrough transformation on the dim_listings table.
It selects key columns from the dimension table with no aggregations,
providing a clean interface to listing attributes for joins and feature engineering.
"""

source = EntitySource(
    # BigQuery table written directly by the batch process
    snapshot_table=get_staging_query_output_table_name(exports.dim_merchants, True),
    query=Query(
        selects=selects(
            listing_id="merchant_id",
            primary_category="primary_category",
        ),
        start_partition="2025-01-01"
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["listing_id"],  # Key by listing_id for point lookups
    online=True,
    version=0,
    aggregations=None,  # No aggregations - this is a simple passthrough
)
