from staging_queries.quickstart import exports

from ai.chronon.types import EntitySource, GroupBy, Query, selects

"""
This GroupBy creates a simple passthrough on the dim_listings table.
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
            long_description="long_description",
            price_cents="price_cents",
            currency="currency",
            inventory_count="inventory_count",
            primary_category="primary_category",
            is_active="is_active",
            weight_grams="weight_grams",
            tags="tags",
            # Derived features
            is_expensive="IF(price_cents > 10000, 1, 0)",  # Over $100
            is_in_stock="IF(inventory_count > 0, 1, 0)",
            main_image_path="main_image_path",
            secondary_image_paths="secondary_image_paths",
        ),
        start_partition="2023-11-01",
    ),
)

listing_features = GroupBy(
    sources=[source],
    keys=["listing_id"],
    online=True,
    version=0,
    aggregations=None,  # Simple passthrough — no aggregations
)
