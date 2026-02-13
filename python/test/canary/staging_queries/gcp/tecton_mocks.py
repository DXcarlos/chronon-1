from ai.chronon.staging_query import StagingQuery, TableDependency, EngineType

"""
Mock Tecton source tables using existing demo tables for migration demonstrations.

These StagingQueries transform demo.user-activities and demo.dim_listings to match
Tecton source schemas, allowing us to validate Chronon feature definitions without
needing the actual Tecton source tables.
"""

# Mock for Tecton's product_saves_create table
# Original: silver_union.product_saves_create with user_id, product_id, created_at
# We simulate "saves" using "favorite" events from demo.user-activities
tecton_mock_product_saves_create = StagingQuery(
    query=f"""
    SELECT
        user_id,
        listing_id as product_id,
        TIMESTAMP_MILLIS(event_time_ms) as created_at,
        DATE(_PARTITIONTIME) as ds
    FROM demo.`user-activities`
    WHERE
        TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """,
    output_namespace="data",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table="demo.`user-activities`", partition_column="_PARTITIONTIME", offset=0)
    ],
    version=1
)

# Preprocessing for count_product_likes_batch:v2_2_2
# This combines the logic from Tecton's multi-source transform:
# 1. Get likes from user-activities (event_type='favorite')
# 2. Join with dim_listings to add seller_id
# 3. Remove self-interactions (seller can't like own product)
# 4. Deduplicate by (product_id, sender_id) keeping earliest like
product_likes_preprocessed = StagingQuery(
    query=f"""
    WITH
    -- Step 1: Get likes from user-activities (simulate product_like_create)
    likes AS (
        SELECT
            user_id as sender_id,
            listing_id as product_id,
            TIMESTAMP_MILLIS(event_time_ms) as event_time,
            DATE(_PARTITIONTIME) as ds
        FROM demo.`user-activities`
        WHERE
            TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
            AND event_type = 'favorite'
            AND user_id IS NOT NULL
            AND listing_id IS NOT NULL
    ),

    -- Step 2: Get product dimensions with seller_id (simulate product_create)
    products AS (
        SELECT
            listing_id as product_id,
            merchant_id as seller_id
        FROM demo.`dim_listings`
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    ),

    -- Step 3: Join likes with products to add seller_id (add_seller_id helper)
    likes_with_seller AS (
        SELECT
            l.sender_id,
            l.product_id,
            l.event_time,
            p.seller_id,
            l.ds
        FROM likes l
        LEFT JOIN products p
          ON l.product_id = p.product_id
    ),

    -- Step 4: Remove self-interactions (remove_self_interactions helper)
    -- Seller can't like their own product
    filtered_likes AS (
        SELECT *
        FROM likes_with_seller
        WHERE seller_id IS NOT NULL
          AND sender_id IS NOT NULL
          AND seller_id != CAST(sender_id AS INT64)
    ),

    -- Step 5: Deduplicate by (product_id, sender_id) (drop_duplicates helper)
    -- Keep earliest like per buyer-product pair
    deduped_likes AS (
        SELECT
            sender_id as product_like,  -- Rename for aggregation
            product_id,
            event_time,
            ds,
            ROW_NUMBER() OVER (
                PARTITION BY product_id, sender_id
                ORDER BY event_time ASC
            ) as rn
        FROM filtered_likes
    )

    SELECT
        product_id,
        product_like,
        event_time,
        ds
    FROM deduped_likes
    WHERE rn = 1
    """,
    output_namespace="data",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table="demo.`user-activities`", partition_column="_PARTITIONTIME", offset=0),
        TableDependency(table="demo.`dim_listings`", partition_column="ds", offset=0)
    ],
    version=1
)

# Preprocessing for query_size_impression_last_n_batch:v2_8_3
# This is a complex Tecton feature that combines 3 sources with extensive transformations:
# 1. product_search_create (searches) - mocked from user-activities
# 2. product_create (product dimensions) - mocked from dim_listings with synthetic size
# 3. top_n_queries (common queries) - hardcoded mock data
#
# Tecton transform pipeline:
# - Filter searches (organic, non-null fields)
# - Standardize queries (text processing, join with top queries, impute)
# - Explode results array to get ranked impressions
# - Join product size from product_create
# - Remove self-interactions (seller != searcher)
# - Group by (query, user, date) and get most common size
# - Aggregate: last(100) sizes over 90 days
query_size_impression_preprocessed = StagingQuery(
    query=f"""
    WITH
    -- Step 1: Mock top_n_queries (Tecton's CSV file source)
    -- Contains common search queries for standardization
    top_queries AS (
        SELECT query FROM UNNEST([
            'vintage', 'sneakers', 'denim', 'accessories', 'leather',
            'shoes', 'jacket', 'dress', 'bag', 'jewelry'
        ]) AS query
    ),

    -- Step 2: Mock product_search_create from user-activities
    -- Simulate searches using view/click events
    raw_searches AS (
        SELECT
            event_id as depop_search_id,
            user_id,
            session_id,
            -- Mock query text based on event type
            CASE
                WHEN event_type = 'view' THEN 'vintage'
                WHEN event_type = 'click' THEN 'sneakers'
                WHEN event_type = 'favorite' THEN 'denim'
                ELSE 'accessories'
            END as query_raw,
            TIMESTAMP_MILLIS(event_time_ms) as search_timestamp,
            -- Mock results array (single listing)
            [listing_id] as results,
            1 as page,
            20 as `limit`,
            DATE(_PARTITIONTIME) as ds
        FROM demo.`user-activities`
        WHERE
            TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
            AND listing_id IS NOT NULL
            AND user_id IS NOT NULL
            AND session_id IS NOT NULL
    ),

    -- Step 3: Filter searches (Tecton's filter_product_search_create)
    -- Remove searches with null/empty required fields
    filtered_searches AS (
        SELECT *
        FROM raw_searches
        WHERE
            depop_search_id IS NOT NULL
            AND depop_search_id != ''
            AND session_id IS NOT NULL
            AND page IS NOT NULL
            AND results IS NOT NULL
            AND ARRAY_LENGTH(results) > 0
    ),

    -- Step 4: Standardize queries (Tecton's standardise_product_search_create_query)
    -- Join with top queries, impute missing with 'IMPUTATION-TEXT'
    standardized_searches AS (
        SELECT
            s.*,
            COALESCE(t.query, 'IMPUTATION-TEXT') as query
        FROM filtered_searches s
        LEFT JOIN top_queries t
          ON s.query_raw = t.query
    ),

    -- Step 5: Get search results with rank (Tecton's get_search_results_with_rank)
    -- Explode results array and add rank position
    search_impressions AS (
        SELECT
            depop_search_id,
            user_id,
            session_id,
            query,
            search_timestamp,
            DATE(search_timestamp) as date,
            listing_id as product_id,
            pos as rank_in_page,
            -- Calculate overall rank: (page - 1) * limit + position
            ((page - 1) * `limit`) + pos + 1 as rank,
            ds
        FROM standardized_searches,
        UNNEST(results) AS listing_id WITH OFFSET pos
    ),

    -- Step 6: Get product dimensions with size (Tecton's product_create + get_product_size)
    -- Mock size based on listing_id modulo (S/M/L/XL)
    products_with_size AS (
        SELECT
            listing_id as product_id,
            merchant_id as seller_id,
            -- Mock size data (Tecton extracts from variants field)
            CASE
                WHEN MOD(listing_id, 4) = 0 THEN 'S'
                WHEN MOD(listing_id, 4) = 1 THEN 'M'
                WHEN MOD(listing_id, 4) = 2 THEN 'L'
                ELSE 'XL'
            END as size,
            ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY updated_at_ts ASC) as rn
        FROM demo.`dim_listings`
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    ),

    -- Deduplicate products (keep first create row)
    products_deduped AS (
        SELECT
            product_id,
            seller_id,
            COALESCE(size, 'UNDEFINED') as size
        FROM products_with_size
        WHERE rn = 1
    ),

    -- Step 7: Join size onto search impressions
    impressions_with_size AS (
        SELECT
            i.query,
            i.user_id,
            i.date,
            i.search_timestamp,
            i.product_id,
            i.rank,
            p.size,
            p.seller_id,
            i.ds
        FROM search_impressions i
        LEFT JOIN products_deduped p
          ON i.product_id = p.product_id
    ),

    -- Step 8: Remove self-interactions (Tecton's remove_self_interactions)
    -- Filter out cases where searcher = seller
    filtered_impressions AS (
        SELECT *
        FROM impressions_with_size
        WHERE
            seller_id IS NOT NULL
            AND user_id IS NOT NULL
            AND CAST(user_id AS INT64) != seller_id
    ),

    -- Step 9: Get most common size per (query, user, date)
    -- Tecton's get_array_attribute_with_largest_count_over_group
    size_counts AS (
        SELECT
            query,
            user_id,
            date,
            size,
            COUNT(*) as size_count,
            MAX(search_timestamp) as search_timestamp,
            ds
        FROM filtered_impressions
        GROUP BY query, user_id, date, size, ds
    ),

    -- Deduplicate to get single most common size per group
    -- Break ties by picking earliest alphabetically
    most_common_size AS (
        SELECT
            query,
            user_id,
            date,
            search_timestamp,
            size,
            ds,
            ROW_NUMBER() OVER (
                PARTITION BY query, user_id, date
                ORDER BY size_count DESC, size ASC
            ) as rn
        FROM size_counts
    )

    -- Final output: query, search_timestamp, size
    SELECT
        CAST(query AS STRING) as query,
        CAST(search_timestamp AS TIMESTAMP) as search_timestamp,
        CAST(size AS STRING) as size,
        ds
    FROM most_common_size
    WHERE rn = 1
    """,
    output_namespace="data",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table="demo.`user-activities`", partition_column="_PARTITIONTIME", offset=0),
        TableDependency(table="demo.`dim_listings`", partition_column="ds", offset=0)
    ],
    version=1
)
