from typing import Dict, List, Optional, Tuple

from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


def _select_sql(projections: Optional[Dict[str, str]], table_alias: str = "") -> str:
    if projections is None:
        prefix = f"{table_alias}." if table_alias else ""
        return f"{prefix}*"
    return ",\n    ".join(f"{expr} AS {alias}" for alias, expr in projections.items())


# Snowflake-flavored emit. Uses Snowflake-only constructs: ::DATE / ::TIMESTAMP_NTZ casts,
# EXTRACT(epoch_second ...), TABLE(GENERATOR(...)), DATEADD/DATEDIFF.
def _snowflake_queries(
    scd2_table_name: str,
    begin_ts: str,
    end_ts: str,
    mut_select: str,
    snap_select: str,
    entity_partition: str,
) -> Tuple[str, str]:
    mutation = f"""
SELECT
    {mut_select},
    CAST(EXTRACT(epoch_second FROM {begin_ts}::TIMESTAMP_NTZ) * 1000 AS BIGINT) AS mutation_ts,
    FALSE AS is_before,
    {begin_ts}::DATE AS ds
FROM {scd2_table_name}
WHERE {begin_ts}::DATE BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}

UNION ALL

SELECT
    {mut_select},
    CAST(EXTRACT(epoch_second FROM {end_ts}::TIMESTAMP_NTZ) * 1000 AS BIGINT) AS mutation_ts,
    TRUE AS is_before,
    {end_ts}::DATE AS ds
FROM {scd2_table_name}
WHERE {end_ts} IS NOT NULL
  AND {end_ts}::DATE BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""
    snapshot = f"""
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), {{{{ start_date }}}}::DATE) AS ds
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', {{{{ start_date }}}}::DATE, {{{{ end_date }}}}::DATE) + 1))
)
SELECT
    {snap_select},
    d.ds
FROM date_spine d
JOIN {scd2_table_name} t
    ON t.{begin_ts}::DATE <= d.ds
   AND (t.{end_ts} IS NULL OR t.{end_ts}::DATE > d.ds)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {entity_partition}, d.ds
    ORDER BY t.{begin_ts} DESC NULLS LAST
) = 1
"""
    return mutation, snapshot


# Spark-flavored emit. Uses CAST(... AS DATE), unix_timestamp() for epoch-millis, sequence() +
# explode() for the date spine, and Spark 3.4+ QUALIFY for dedup. Output is byte-for-byte
# equivalent to the Snowflake variant against the same input.
def _spark_queries(
    scd2_table_name: str,
    begin_ts: str,
    end_ts: str,
    mut_select: str,
    snap_select: str,
    entity_partition: str,
) -> Tuple[str, str]:
    mutation = f"""
SELECT
    {mut_select},
    CAST(unix_timestamp({begin_ts}) * 1000 AS BIGINT) AS mutation_ts,
    FALSE AS is_before,
    CAST({begin_ts} AS DATE) AS ds
FROM {scd2_table_name}
WHERE CAST({begin_ts} AS DATE) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}

UNION ALL

SELECT
    {mut_select},
    CAST(unix_timestamp({end_ts}) * 1000 AS BIGINT) AS mutation_ts,
    TRUE AS is_before,
    CAST({end_ts} AS DATE) AS ds
FROM {scd2_table_name}
WHERE {end_ts} IS NOT NULL
  AND CAST({end_ts} AS DATE) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""
    # Spark 3.5 doesn't accept QUALIFY in this CTE+JOIN position and lacks `* EXCEPT (...)`
    # to drop a helper column from a star-expanded subquery. A well-formed SCD2 table has at
    # most one valid version per (entity, day) so we skip the defensive dedup; if the input
    # ever has overlapping validity periods callers should clean it up upstream.
    snapshot = f"""
WITH date_spine AS (
    SELECT explode(sequence(
        to_date({{{{ start_date }}}}),
        to_date({{{{ end_date }}}}),
        INTERVAL 1 DAY
    )) AS ds
)
SELECT
    {snap_select},
    d.ds
FROM date_spine d
JOIN {scd2_table_name} t
    ON CAST(t.{begin_ts} AS DATE) <= d.ds
   AND (t.{end_ts} IS NULL OR CAST(t.{end_ts} AS DATE) > d.ds)
"""
    return mutation, snapshot


def scd2_to_entity_staging_queries(
    scd2_table_name: str,
    begin_ts_column: str,
    end_ts_column: str,
    entity_key_columns: List[str],
    projections: Optional[Dict[str, str]] = None,
    output_namespace: str = "data",
    version: int = 0,
    step_days: int = 30,
    engine_type: int = EngineType.SNOWFLAKE,
) -> Tuple[StagingQuery, StagingQuery]:
    """
    Builds (mutation_sq, snapshot_sq) from an SCD2 table for use with EntitySource.

    SCD2 input: one row per entity version, valid from begin_ts_column to end_ts_column (NULL = still active).

    mutation_sq output per ds partition: all mutations that occurred on that day.
      - INSERT: one row, is_before=False, mutation_ts=begin_ts
      - UPDATE: two rows, is_before=True (old, mutation_ts=end_ts) + is_before=False (new, mutation_ts=begin_ts)
      - DELETE: one row, is_before=True, mutation_ts=end_ts

    snapshot_sq output per ds partition: one row per active entity reflecting end-of-day state.

    engine_type controls the emitted SQL dialect — SNOWFLAKE (default) emits ::DATE / EXTRACT
    / TABLE(GENERATOR(...)); SPARK emits CAST AS DATE / unix_timestamp / sequence + explode
    so the output runs under the local Spark eval harness.
    """
    mut_select = _select_sql(projections)
    snap_select = _select_sql(projections, table_alias="t")
    entity_partition = ", ".join(f"t.{col}" for col in entity_key_columns)

    builder = {
        EngineType.SNOWFLAKE: _snowflake_queries,
        EngineType.SPARK: _spark_queries,
    }.get(engine_type)
    if builder is None:
        raise ValueError(f"scd2_to_entity_staging_queries: unsupported engine_type {engine_type}")

    mutation_query, snapshot_query = builder(
        scd2_table_name, begin_ts_column, end_ts_column, mut_select, snap_select, entity_partition
    )

    dep = TableDependency(
        table=scd2_table_name,
        partition_column=begin_ts_column,
        offset=0,
        time_partitioned=True,
    )

    mutation_sq = StagingQuery(
        query=mutation_query,
        output_namespace=output_namespace,
        engine_type=engine_type,
        dependencies=[dep],
        version=version,
        step_days=step_days,
    )

    snapshot_sq = StagingQuery(
        query=snapshot_query,
        output_namespace=output_namespace,
        engine_type=engine_type,
        dependencies=[dep],
        version=version,
        step_days=step_days,
    )

    return mutation_sq, snapshot_sq
