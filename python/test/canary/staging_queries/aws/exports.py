from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "ds"):
    spark_export_sql = f"""
    SELECT
        *
    FROM demo.{table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """

    return StagingQuery(
        query=spark_export_sql,
        output_namespace="data",
        engine_type=EngineType.SPARK,
        dependencies=[
            TableDependency(table=f"demo.{table}", partition_column=partition_column, offset=0)
        ],
        version=0,
        step_days=30,
    )


def get_native_partition_export(table: str, partition_column: str):
    native_partition_sql = f"""
    SELECT
        *,
        DATE_FORMAT({partition_column}, 'yyyy-MM-dd') as ds
    FROM demo.{table}
    WHERE
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=native_partition_sql,
        output_namespace="data",
        engine_type=EngineType.SPARK,
        dependencies=[
            TableDependency(table=f"demo.{table}", partition_column=partition_column, offset=0)
        ],
        version=0,
        step_days=30,
    )


user_activities = get_native_partition_export("user_activities", "event_time")
checkouts = get_native_partition_export("checkouts", "ts")
dim_listings = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")

# Sparse input table — only has partitions on 01-01, 01-05, 01-10, 01-15, 01-20
# Used to test backfill with missing input partitions
user_activities_sparse = StagingQuery(
    query="""
    SELECT
        *
    FROM demo.user_activities_raw_sparse
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="data",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="demo.user_activities_raw_sparse", partition_column="ds", offset=0, sparse=True)
    ],
    version=1,
    step_days=30,
)
