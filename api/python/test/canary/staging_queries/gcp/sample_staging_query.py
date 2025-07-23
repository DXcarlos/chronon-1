from joins.gcp import training_set

from ai.chronon.staging_query import StagingQuery, TableDependency
from ai.chronon.utils import get_join_output_table_name

query = f"""
SELECT
    *
FROM {get_join_output_table_name(training_set.v1_test, True)}
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

v1 = StagingQuery(
    query=query,
    start_partition="2020-03-01",
    setups=[
        "CREATE TEMPORARY FUNCTION S2_CELL AS 'com.sample.hive.udf.S2CellId'",
    ],
    name="sample_staging_query",
    output_namespace="sample_namespace",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    dependencies=[
        TableDependency(table=get_join_output_table_name(training_set.v1_test, True), partition_column="ds", additional_partitions=["_HR=23:00"], offset=1)
    ],
    version=0,
)
