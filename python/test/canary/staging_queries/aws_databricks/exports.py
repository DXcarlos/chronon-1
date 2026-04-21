from ai.chronon.types import EngineType, StagingQuery, TableDependency

dim_listings = StagingQuery(
    query="""
    SELECT
        *
    FROM workspace.poc.dim_listings
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    version=0,
)

dim_listings_b = StagingQuery(
    query="""
    SELECT
        *
    FROM main.`schema-with-dashes`.`example-table`
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="main.`schema-with-dashes`.`example-table`", partition_column="ds", offset=0)
    ],
    version=0,
)

dim_listings_non_partitioned = StagingQuery(
    query="""
    SELECT
        *, DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_nop
    WHERE
    DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_nop", partition_column="updated_at_ts", time_partitioned=True, offset=0)
    ],
    version=0,
)
