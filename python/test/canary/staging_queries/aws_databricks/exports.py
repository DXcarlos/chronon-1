from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

dim_listings = StagingQuery(
    query="""
    SELECT
        *, date_format(CAST(updated_at_ts AS DATE), 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_nop
    WHERE
    CAST(updated_at_ts AS DATE) BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_nop", partition_column="ds", offset=0)
    ],
    version=1,
)
