from ai.chronon.staging_query import StagingQuery, TableDependency

query = """
SELECT
    id_listing,
    place_id,
    S2_CELL(lat, lng, 12) AS s2CellId,
    impressed_unique_count_1d,
    viewed_unique_count_1d,
    ds
FROM sample_namespace.sample_table
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
        TableDependency(table="sample_namespace.sample_table", partition_column="ds", additional_partitions=["_HR=23:00"], offset=1)
    ],
)
