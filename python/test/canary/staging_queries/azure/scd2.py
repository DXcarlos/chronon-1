from typing import Dict, List, Optional, Tuple

from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


def _select_sql(projections: Optional[Dict[str, str]], table_alias: str = "") -> str:
    if projections is None:
        prefix = f"{table_alias}." if table_alias else ""
        return f"{prefix}*"
    return ",\n    ".join(f"{expr} AS {alias}" for alias, expr in projections.items())


def scd2_to_entity_staging_queries(
    scd2_table_name: str,
    begin_ts_column: str,
    end_ts_column: str,
    entity_key_columns: List[str],
    projections: Optional[Dict[str, str]] = None,
    output_namespace: str = "data",
    version: int = 0,
    step_days: int = 30,
) -> Tuple[StagingQuery, StagingQuery]:
    """
    Builds (mutation_sq, snapshot_sq) from an SCD2 table for use with EntitySource.

    SCD2 input: one row per entity version, valid from begin_ts_column to end_ts_column (NULL = still active).

    mutation_sq output per ds partition: all mutations that occurred on that day.
      - INSERT: one row, is_before=False, mutation_ts=begin_ts
      - UPDATE: two rows, is_before=True (old, mutation_ts=end_ts) + is_before=False (new, mutation_ts=begin_ts)
      - DELETE: one row, is_before=True, mutation_ts=end_ts

    snapshot_sq output per ds partition: one row per active entity reflecting end-of-day state.
    """
    mut_select = _select_sql(projections)
    snap_select = _select_sql(projections, table_alias="t")
    entity_partition = ", ".join(f"t.{col}" for col in entity_key_columns)

    mutation_query = f"""
SELECT
    {mut_select},
    CAST(EXTRACT(epoch_second FROM {begin_ts_column}::TIMESTAMP_NTZ) * 1000 AS BIGINT) AS mutation_ts,
    FALSE AS is_before,
    {begin_ts_column}::DATE AS ds
FROM {scd2_table_name}
WHERE {begin_ts_column}::DATE BETWEEN '{{{{ start_date }}}}' AND '{{{{ end_date }}}}'

UNION ALL

SELECT
    {mut_select},
    CAST(EXTRACT(epoch_second FROM {end_ts_column}::TIMESTAMP_NTZ) * 1000 AS BIGINT) AS mutation_ts,
    TRUE AS is_before,
    {end_ts_column}::DATE AS ds
FROM {scd2_table_name}
WHERE {end_ts_column} IS NOT NULL
  AND {end_ts_column}::DATE BETWEEN '{{{{ start_date }}}}' AND '{{{{ end_date }}}}'
"""

    snapshot_query = f"""
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '{{{{ start_date }}}}'::DATE) AS ds
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', '{{{{ start_date }}}}'::DATE, '{{{{ end_date }}}}'::DATE) + 1))
)
SELECT
    {snap_select},
    d.ds
FROM date_spine d
JOIN {scd2_table_name} t
    ON t.{begin_ts_column}::DATE <= d.ds
   AND (t.{end_ts_column} IS NULL OR t.{end_ts_column}::DATE > d.ds)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {entity_partition}, d.ds
    ORDER BY t.{begin_ts_column} DESC NULLS LAST
) = 1
"""

    dep = TableDependency(
        table=scd2_table_name,
        partition_column=begin_ts_column,
        offset=0,
        time_partitioned=True,
    )

    mutation_sq = StagingQuery(
        query=mutation_query,
        output_namespace=output_namespace,
        engine_type=EngineType.SNOWFLAKE,
        dependencies=[dep],
        version=version,
        step_days=step_days,
    )

    snapshot_sq = StagingQuery(
        query=snapshot_query,
        output_namespace=output_namespace,
        engine_type=EngineType.SNOWFLAKE,
        dependencies=[dep],
        version=version,
        step_days=step_days,
    )

    return mutation_sq, snapshot_sq
