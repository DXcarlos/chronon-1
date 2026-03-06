from staging_queries.quickstart import exports

from ai.chronon.group_by import GroupBy
from ai.chronon.query import Query, selects
from ai.chronon.source import EntitySource

"""
Passthrough GroupBy on the dim_listings table.
Provides listing attributes for point-in-time lookups in joins.
"""

source = EntitySource(
    snapshot_table=exports.dim_listings.table,
    query=Query(
        selects=selects(
            listing_id="listing_id",
            merchant_id="merchant_id",
            headline="headline",
            brief_description="brief_description",
            price_cents="price_cents",
            currency="currency",
            inventory_count="inventory_count",
            primary_category="primary_category",
            is_active="is_active",
            # Derived features
            is_expensive="IF(price_cents > 10000, 1, 0)",
            is_in_stock="IF(inventory_count > 0, 1, 0)",
        ),
        start_partition="2023-11-01",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    online=True,
    version=0,
    aggregations=None,  # Simple passthrough — no aggregations
)
