from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

dim_listings = StagingQuery(
    query="""
    SELECT
        *
    FROM workspace.poc.dim_listings_v2
    WHERE
    DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_v2", partition_column="updated_at_ts", offset=0)
    ],
    version=2,
)
