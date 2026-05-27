from staging_queries.quickstart import exports

from ai.chronon.types import EntitySource, GroupBy, Query, selects

"""
This GroupBy creates a simple passthrough on the dim_merchants table.
Provides merchant attributes for point-in-time lookups in joins.
Keyed by merchant_id — joined via the merchant_id field on the left event source.
"""

source = EntitySource(
    snapshot_table=exports.dim_merchants.table,
    query=Query(
        selects=selects(
            merchant_id="merchant_id",
            primary_category="primary_category",
        ),
        start_partition="2023-11-01",
    ),
)

merchant_features = GroupBy(
    sources=[source],
    keys=["merchant_id"],
    online=True,
    version=0,
    aggregations=None,  # Simple passthrough — no aggregations
)
