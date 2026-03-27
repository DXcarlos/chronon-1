"""
Example: StagingQuery definitions for reading S3 Parquet files into Chronon.

Uses Spark's `setups` SQL to create temporary views over S3 paths, then queries
them with templated date parameters. Covers non-partitioned, Hive-partitioned,
and multi-source join patterns.
"""

from ai.chronon.staging_query import StagingQuery

# --- Non-partitioned parquet ---
# Flat parquet files under an S3 prefix with no partition structure.
# Derives `ds` from a timestamp column and filters in the query.

non_partitioned_setup = """
    CREATE OR REPLACE TEMPORARY VIEW raw_events
    USING parquet
    OPTIONS (path 's3a://my-bucket/data/raw_events/')
"""

non_partitioned_query = """
    SELECT
        user_id,
        event_type,
        event_payload,
        ts,
        DATE(FROM_UNIXTIME(ts / 1000)) AS ds
    FROM raw_events
    WHERE DATE(FROM_UNIXTIME(ts / 1000)) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

# dependencies=[] because the "tables" are temporary views created by setups, not managed tables
non_partitioned = StagingQuery(
    query=non_partitioned_query,
    setups=[non_partitioned_setup],
    output_namespace="data",
    dependencies=[],
)

# --- Hive-partitioned parquet ---
# Parquet files organized as ds=YYYY-MM-DD/ directories.
# Uses `basePath` so Spark discovers partition columns automatically,
# enabling partition pruning in the WHERE clause.

partitioned_setup = """
    CREATE OR REPLACE TEMPORARY VIEW user_activity
    USING parquet
    OPTIONS (
        path 's3a://my-bucket/data/user_activity/',
        basePath 's3a://my-bucket/data/user_activity/'
    )
"""

partitioned_query = """
    SELECT
        user_id,
        action,
        item_id,
        ts,
        ds
    FROM user_activity
    WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

partitioned = StagingQuery(
    query=partitioned_query,
    setups=[partitioned_setup],
    output_namespace="data",
    dependencies=[],
)

# --- Multi-source join ---
# Joins a partitioned event table with a non-partitioned dimension table.
# Each source gets its own setup entry.

multi_source_events_setup = """
    CREATE OR REPLACE TEMPORARY VIEW transactions
    USING parquet
    OPTIONS (
        path 's3a://my-bucket/data/transactions/',
        basePath 's3a://my-bucket/data/transactions/'
    )
"""

multi_source_dim_setup = """
    CREATE OR REPLACE TEMPORARY VIEW product_catalog
    USING parquet
    OPTIONS (path 's3a://my-bucket/data/product_catalog/')
"""

multi_source_query = """
    SELECT
        t.user_id,
        t.product_id,
        t.amount,
        p.category,
        p.brand,
        t.ts,
        t.ds
    FROM transactions t
    JOIN product_catalog p ON t.product_id = p.product_id
    WHERE t.ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

multi_source = StagingQuery(
    query=multi_source_query,
    setups=[multi_source_events_setup, multi_source_dim_setup],
    output_namespace="data",
    dependencies=[],
)
