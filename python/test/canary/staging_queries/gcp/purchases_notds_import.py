from gen_thrift.common.ttypes import ConfigProperties

from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency

v1 = StagingQuery(
    query="SELECT * FROM data.purchases_notds WHERE notds BETWEEN {{ start_date }} AND {{ end_date }}",
    engine_type=EngineType.BIGQUERY,
    output_namespace="data",
    conf=ConfigProperties(common={"spark.chronon.partition.column": "notds"}),
    dependencies=[
        TableDependency(table="data.purchases_notds", partition_column="notds", offset=0)
    ],
    version=0,
)
