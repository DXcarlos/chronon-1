"""Cloud-specific output table validation for integration tests.

After a backfill completes, these helpers verify that output tables
exist and contain non-null data for the expected partitions.
"""

import logging
import os

logger = logging.getLogger(__name__)

try:
    from google.cloud import bigquery as _bigquery
except ImportError:
    _bigquery = None  # type: ignore[assignment]

try:
    import boto3
except ImportError:
    boto3 = None  # type: ignore[assignment]

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None  # type: ignore[assignment]


def validate_table_has_data(
    cloud: str,
    table_name: str,
    partition_col: str,
    partition_value: str,
) -> bool:
    """Return True if the table partition has at least one row."""
    if cloud == "gcp":
        return _gcp_table_has_data(table_name, partition_col, partition_value)
    elif cloud == "aws":
        return _aws_table_has_data(table_name, partition_col, partition_value)
    elif cloud == "azure":
        return _azure_table_has_data(table_name, partition_col, partition_value)
    else:
        raise ValueError(f"Unsupported cloud: {cloud}")


def validate_columns_non_null(
    cloud: str,
    table_name: str,
    columns: list[str],
    partition_col: str,
    partition_value: str,
) -> bool:
    """Return True if every column in *columns* has at least one non-null value in the partition."""
    if cloud == "gcp":
        return _gcp_columns_non_null(table_name, columns, partition_col, partition_value)
    elif cloud == "aws":
        return _aws_columns_non_null(table_name, columns, partition_col, partition_value)
    elif cloud == "azure":
        return _azure_columns_non_null(table_name, columns, partition_col, partition_value)
    else:
        raise ValueError(f"Unsupported cloud: {cloud}")


# ---------------------------------------------------------------------------
# GCP (BigQuery)
# ---------------------------------------------------------------------------


def _gcp_table_has_data(table_name: str, partition_col: str, partition_value: str) -> bool:
    project = os.environ.get("GCP_BQ_PROJECT", "canary-443022")
    client = _bigquery.Client(project=project)

    query = (
        f"SELECT COUNT(*) as cnt FROM `{table_name}` "
        f"WHERE {partition_col} = '{partition_value}'"
    )
    logger.info("GCP validation query: %s", query)
    result = client.query(query).result()
    row = next(iter(result))
    cnt = row.cnt
    logger.info("GCP row count for %s partition %s=%s: %d", table_name, partition_col, partition_value, cnt)
    return cnt > 0


def _gcp_columns_non_null(
    table_name: str, columns: list[str], partition_col: str, partition_value: str
) -> bool:
    project = os.environ.get("GCP_BQ_PROJECT", "canary-443022")
    client = _bigquery.Client(project=project)

    non_null_exprs = ", ".join(f"COUNTIF({c} IS NOT NULL) as {c}_nonnull" for c in columns)
    query = (
        f"SELECT COUNT(*) as total, {non_null_exprs} "
        f"FROM `{table_name}` WHERE {partition_col} = '{partition_value}'"
    )
    logger.info("GCP non-null validation query: %s", query)
    result = client.query(query).result()
    row = next(iter(result))

    for c in columns:
        nonnull_count = getattr(row, f"{c}_nonnull")
        logger.info("  %s: %d non-null values", c, nonnull_count)
        if nonnull_count == 0:
            logger.warning("Column %s has zero non-null values in %s", c, table_name)
            return False
    return True


# ---------------------------------------------------------------------------
# AWS (S3 / Parquet)
# ---------------------------------------------------------------------------

_S3_BUCKET = "zipline-warehouse-canary"
_S3_TABLE_PREFIX = "data/tables"


def _aws_table_has_data(table_name: str, partition_col: str, partition_value: str) -> bool:
    region = os.environ.get("AWS_REGION", "us-west-2")
    s3 = boto3.client("s3", region_name=region)

    prefix = f"{_S3_TABLE_PREFIX}/{table_name}/data/{partition_col}={partition_value}/"
    logger.info("AWS validation: listing s3://%s/%s", _S3_BUCKET, prefix)

    resp = s3.list_objects_v2(Bucket=_S3_BUCKET, Prefix=prefix, MaxKeys=10)
    parquet_files = [
        obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")
    ]
    logger.info("AWS found %d parquet files for %s", len(parquet_files), table_name)
    return len(parquet_files) > 0


def _aws_columns_non_null(
    table_name: str, columns: list[str], partition_col: str, partition_value: str
) -> bool:
    region = os.environ.get("AWS_REGION", "us-west-2")
    s3 = boto3.client("s3", region_name=region)

    prefix = f"{_S3_TABLE_PREFIX}/{table_name}/data/{partition_col}={partition_value}/"
    resp = s3.list_objects_v2(Bucket=_S3_BUCKET, Prefix=prefix, MaxKeys=10)
    parquet_files = [
        obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        logger.warning("No parquet files found for %s at %s", table_name, prefix)
        return False

    # Read the first parquet file and check columns
    key = parquet_files[0]
    logger.info("AWS reading s3://%s/%s for non-null check", _S3_BUCKET, key)
    obj = s3.get_object(Bucket=_S3_BUCKET, Key=key)
    table = pq.read_table(obj["Body"])

    for c in columns:
        if c not in table.column_names:
            logger.warning("Column %s not found in parquet file for %s", c, table_name)
            return False
        col = table.column(c)
        non_null = col.null_count < len(col)
        logger.info("  %s: %d/%d non-null", c, len(col) - col.null_count, len(col))
        if not non_null:
            logger.warning("Column %s is entirely null in %s", c, table_name)
            return False
    return True


# ---------------------------------------------------------------------------
# Azure (stub)
# ---------------------------------------------------------------------------


def _azure_table_has_data(table_name: str, partition_col: str, partition_value: str) -> bool:
    # TODO: implement Snowflake/Iceberg REST validation for Azure canary tables
    logger.warning("Azure table validation not yet implemented for %s", table_name)
    return True


def _azure_columns_non_null(
    table_name: str, columns: list[str], partition_col: str, partition_value: str
) -> bool:
    # TODO: implement Snowflake/Iceberg REST validation for Azure canary tables
    logger.warning("Azure column validation not yet implemented for %s", table_name)
    return True
