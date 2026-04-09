from staging_queries.azure import exports

from ai.chronon.types import EntitySource, GroupBy, Query, selects

"""
This GroupBy creates a simple passthrough transformation on the dim_listings table.
It selects key columns from the dimension table with no aggregations,
providing a clean interface to listing attributes for joins and feature engineering.
"""

source = EntitySource(
    # Snowflake table written directly by the batch process
    snapshot_table=exports.dim_merchants.table,
    query=Query(
        selects=selects(
            listing_id="CAST(merchant_id AS INT)",
            primary_category="primary_category",
        ),
        start_partition="2025-01-01"
    ),
)

v2 = GroupBy(
    sources=[source],
    keys=["listing_id"],  # Key by listing_id for point lookups
    online=True,
    aggregations=None,  # No aggregations - this is a simple passthrough
)

# Time-partitioned variant for sparse partition testing
source_tp = EntitySource(
    snapshot_table=exports.dim_merchants_tp.table,
    query=Query(
        selects=selects(
            listing_id="CAST(merchant_id AS INT)",
            primary_category="primary_category",
        ),
        start_partition="2025-01-01",
        time_partitioned=True,
        partition_column="ds",
    ),
)

v3 = GroupBy(
    sources=[source_tp],
    keys=["listing_id"],
    aggregations=None,
)
