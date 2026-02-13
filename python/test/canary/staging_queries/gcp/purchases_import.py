from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

v1 = StagingQuery(
    query="SELECT * FROM data.purchases WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}",
    engine_type=EngineType.BIGQUERY,
    output_namespace="data",
    dependencies=[
        TableDependency(table="data.purchases", partition_column="ds", offset=0)
    ],
    version=0,
)
