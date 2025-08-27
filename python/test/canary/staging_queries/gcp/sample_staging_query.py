from joins.gcp import training_set

from ai.chronon.staging_query import EngineType, Import, StagingQuery, TableDependency
from ai.chronon.utils import get_join_output_table_name, get_staging_query_output_table_name


def get_staging_query(category_name):
    query = f"""
        SELECT
            *,
            '{category_name}' as category_name
        FROM {get_join_output_table_name(training_set.v1_test, True)}
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """
    return StagingQuery(
        query=query,
        output_namespace="data",
        table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
        dependencies=[
            TableDependency(table=get_join_output_table_name(training_set.v1_test, True), partition_column="ds", offset=1)
        ],
        version=0,
    )

cart = get_staging_query("cart")
user = get_staging_query("user")
item = get_staging_query("item")
order = get_staging_query("order")
payment = get_staging_query("payment")
shipping = get_staging_query("shipping")


def terminal_query(staging_queries):
    full_query =  "\nUNION ALL\n".join([f"""SELECT
                *
            FROM {get_staging_query_output_table_name(staging_query, True)}
            WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}""" for staging_query in staging_queries])
    return full_query


terminal = StagingQuery(
    query=terminal_query([cart, user, item, order, payment, shipping]),
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    output_namespace="data",
    dependencies=[
        TableDependency(table=get_staging_query_output_table_name(cart, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(user, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(item, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(order, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(payment, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(shipping, True), partition_column="ds", offset=1),
    ],
    version=0,
)

purchases_labels = StagingQuery(
    query=f"""
SELECT 
    *,
    case when rand() < 0.5 then 0 else 1 end as label
FROM {get_join_output_table_name(training_set.v1_test, True)}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
""",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    output_namespace="data",
    dependencies=[
        TableDependency(table=get_join_output_table_name(training_set.v1_test, True), partition_column="ds", offset=0),
    ],
    version=0,
)

query_hub = f"""
SELECT
    *
FROM {get_join_output_table_name(training_set.v1_hub, True)}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""

v1_hub = StagingQuery(
    query=query_hub,
    output_namespace="data",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    dependencies=[
        TableDependency(table=get_join_output_table_name(training_set.v1_hub, True), partition_column="ds", offset=1)
    ],
    version=0,
)

bigquery_import_query = f"""
SELECT
    *
FROM {get_join_output_table_name(training_set.v1_hub, True)}
WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
"""

v1_bigquery_import = Import(
    query=bigquery_import_query,
    engine_type=EngineType.BIGQUERY,
    output_namespace="data",
    dependencies=[
        TableDependency(table=get_join_output_table_name(training_set.v1_hub, True), partition_column="ds", offset=0)
    ],
    version=0,

)
