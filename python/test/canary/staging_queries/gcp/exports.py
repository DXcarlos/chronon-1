from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "_PARTITIONTIME", version=0):
    bigquery_export_sql = f"""
    SELECT 
        * 
    FROM demo.`{table}`
    WHERE 
    TIMESTAMP_TRUNC({partition_column}, DAY) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """


    return StagingQuery(
        query=bigquery_export_sql,
        output_namespace="data",
        engine_type=EngineType.BIGQUERY,
        dependencies=[
            TableDependency(table=f"demo.`{table}`", partition_column=partition_column, offset=0)
        ],
        version=0,
        step_days=30,
    )


def get_native_partition_export(table: str, partition_column: str, version=0):
    native_partition_sql = f"""
    SELECT 
        *,
        TIMESTAMP_TRUNC({partition_column}, DAY) as ds
    FROM demo.`{table}`
    WHERE 
    {partition_column} BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=native_partition_sql,
        output_namespace="data",
        engine_type=EngineType.BIGQUERY,
        dependencies=[
            TableDependency(table=f"demo.`{table}`", partition_column=partition_column, offset=0)
        ],
        version=0,
        step_days=30,
    )



user_activities = get_native_partition_export("user-activities", "_PARTITIONTIME")
user_activities_new = get_native_partition_export("user-activities", "_PARTITIONTIME", version=3)
checkouts = get_native_partition_export("checkouts", "_PARTITIONTIME")
dim_listings = get_select_star_export("dim_listings", "ds")
dim_listings_new = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")
