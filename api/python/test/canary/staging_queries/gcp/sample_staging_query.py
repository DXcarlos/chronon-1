from joins.gcp import training_set

from ai.chronon.staging_query import StagingQuery, TableDependency
from ai.chronon.utils import get_join_output_table_name, get_staging_query_output_table_name


def get_staging_query(category_name, additional_filters = None, version = 1):
    additional_filters_str = f"AND {additional_filters}" if additional_filters else ""
    query = f"""
        SELECT
            user_id,
            user_id_purchase_price_count_1d,
            user_id_purchase_price_count_3d,
            user_id_purchase_price_count_7d,
            user_id_purchase_price_average_1d,
            user_id_purchase_price_average_3d,
            user_id_purchase_price_average_7d,
            user_id_purchase_price_last10,
            '{category_name}' as category_name
        FROM {get_join_output_table_name(training_set.fraud_detection, True)}
        WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
        {additional_filters_str}
    """
    return StagingQuery(
        query=query,
        start_partition="2025-07-01",
        name="sample_staging_query",
        output_namespace="data",
        table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
        dependencies=[
            TableDependency(table=get_join_output_table_name(training_set.fraud_detection, True), partition_column="ds", offset=1)
        ],
        version=version,
    )

cart = get_staging_query("cart")
user = get_staging_query("user")
payment = get_staging_query("payment") # "user_id_purchase_price_sum_7d > 0"


def terminal_query(staging_queries):
    full_query =  "\nUNION ALL\n".join([f"""SELECT
            user_id,
            user_id_purchase_price_count_1d,
            user_id_purchase_price_count_3d,
            user_id_purchase_price_count_7d,
            user_id_purchase_price_average_1d,
            user_id_purchase_price_average_3d,
            user_id_purchase_price_average_7d,
            user_id_purchase_price_last10,
            FROM {get_staging_query_output_table_name(staging_query, True)}
            WHERE ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}""" for staging_query in staging_queries])
    return full_query


fraud_detection_labels = StagingQuery(
    query=terminal_query([cart, user, payment]),
    start_partition="2025-07-01",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    name="fraud_detection_labels",
    output_namespace="data",
    dependencies=[
        TableDependency(table=get_staging_query_output_table_name(cart, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(user, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(payment, True), partition_column="ds", offset=1),
    ],
    version=4,
)
