from gen_thrift.api.ttypes import EventSource, Source

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects
from ai.chronon.types import ConfigProperties, EnvironmentVariables

source = Source(
    events=EventSource(
        table="demo.user_activities_raw",
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                view_event="IF(event_type = 'view', 1, 0)",
                click_event="IF(event_type = 'click', 1, 0)",
                purchase_event="IF(event_type = 'purchase', 1, 0)",
                favorite_event="IF(event_type = 'favorite', 1, 0)",
                add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
                is_mobile="IF(device_type = 'mobile', 1, 0)",
                is_desktop="IF(device_type = 'desktop', 1, 0)",
                is_tablet="IF(device_type = 'tablet', 1, 0)",
                user_event_struct="STRUCT(event_type, listing_id, unix_millis(TIMESTAMP(event_time_ms)) as timestamp)",
            ),
            time_column="event_time_ms",
        ),
    )
)

window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]

event_columns = ["view_event", "click_event", "purchase_event", "favorite_event", "add_to_cart_event"]
device_columns = ["is_mobile", "is_desktop", "is_tablet"]
last_k_columns = ["user_event_struct"]

aggregations = []

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.AVERAGE, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in device_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.LAST_K(128), windows=window_sizes)
    for col in last_k_columns
])

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    version=1,
    aggregations=aggregations,
    step_days=30,
    conf=ConfigProperties(
        common={
    # Hive Metastore
    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    # 2. Iceberg Wrapper: Configures the "spark_catalog" wrapper to use Glue for metadata
    "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",

    # 3. Iceberg Extensions: Required for Spark to handle the catalog correctly
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

    # 4. Your existing warehouse configs
    "spark.sql.catalog.spark_catalog.warehouse": "s3://zipline-warehouse/data/tables/",
    "spark.sql.catalog.default_iceberg.warehouse": "s3://zipline-warehouse/data/tables/",
    "spark.sql.catalog.default_iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        }
    ),
    env_vars=EnvironmentVariables(
        common={
            "CHRONON_ONLINE_ARGS": "-Ztasks=1",
        }
    ),
)
