from staging_queries.gcp import exports

from ai.chronon.types import EngineType, StagingQuery, TableDependency

# Three-CTE query: filter active listings, aggregate per-merchant stats, join with merchant dimension.
merchant_listing_summary = StagingQuery(
    query=f"""
WITH active_listings AS (
    SELECT
        listing_id,
        merchant_id,
        primary_category,
        price_cents,
        inventory_count,
        ds
    FROM {exports.dim_listings.table}
    WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
      AND is_active = true
),
merchant_stats AS (
    SELECT
        merchant_id,
        primary_category,
        COUNT(listing_id)    AS active_listing_count,
        SUM(inventory_count) AS total_inventory,
        AVG(price_cents)     AS avg_price_cents,
        ds
    FROM active_listings
    GROUP BY merchant_id, primary_category, ds
),
merchant_dim AS (
    SELECT
        merchant_id,
        primary_category,
        ds
    FROM {exports.dim_merchants.table}
    WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
)
SELECT
    m.merchant_id,
    m.primary_category,
    COALESCE(s.active_listing_count, 0) AS active_listing_count,
    COALESCE(s.total_inventory, 0)      AS total_inventory,
    s.avg_price_cents,
    m.ds
FROM merchant_dim m
LEFT JOIN merchant_stats s
    ON m.merchant_id = s.merchant_id
   AND m.primary_category = s.primary_category
   AND m.ds = s.ds
""",
    output_namespace="data",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table=exports.dim_listings.table, partition_column="ds", start_offset=0, end_offset=0),
        TableDependency(table=exports.dim_merchants.table, partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=0,
    step_days=30,
)

# Two-CTE query: compute category-level price statistics, then label each listing with a price tier.
# The second CTE references the first, exercising chained CTE evaluation.
price_tier_labels = StagingQuery(
    query=f"""
WITH category_price_stats AS (
    SELECT
        primary_category,
        AVG(price_cents)    AS avg_price_cents,
        STDDEV(price_cents) AS stddev_price_cents,
        ds
    FROM {exports.dim_listings.table}
    WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    GROUP BY primary_category, ds
),
labeled_listings AS (
    SELECT
        l.listing_id,
        l.merchant_id,
        l.primary_category,
        l.price_cents,
        l.inventory_count,
        l.is_active,
        l.ds,
        CASE
            WHEN l.price_cents > s.avg_price_cents + 2 * s.stddev_price_cents THEN 'high'
            WHEN l.price_cents < s.avg_price_cents - 2 * s.stddev_price_cents THEN 'low'
            ELSE 'normal'
        END AS price_tier
    FROM {exports.dim_listings.table} l
    JOIN category_price_stats s
        ON l.primary_category = s.primary_category
       AND l.ds = s.ds
    WHERE l.ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
)
SELECT * FROM labeled_listings
""",
    output_namespace="data",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table=exports.dim_listings.table, partition_column="ds", start_offset=0, end_offset=0),
    ],
    version=0,
    step_days=30,
)
