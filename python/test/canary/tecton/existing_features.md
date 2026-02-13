# Search Reranker Feature Samples (User, Query, Product)

This doc captures 3 user, 3 query, and 3 product features used by the search reranker feature services.
All entries are batch feature views, and all are `mode="pyspark"` as defined in their decorators.

Feature services referenced:

- Query: `feature_store/search/feature_services/query_services/reranking_feature_service_query_2_8_3.py`
- User: `feature_store/shared/reranker_all_features/user_services/reranking_feature_service_userid_v2_12_3.py`
- Product: `feature_store/shared/reranker_all_features/product_services/reranking_feature_service_product_v2_12_7.py`

## User Features

### user_previous_n_add_to_bag_product_id:v2_6_1

- Feature view: `feature_store/search/features/user_previous_n_add_to_bag_product_id_v2_6_1.py`
  (`user_previous_n_add_to_bag_product_id_v2_6_1`)
- Feature service: `feature_store/shared/reranker_all_features/user_services/reranking_feature_service_userid_v2_12_3.py`
- Data sources:
  - `data_sources/tracking_add_item_to_bag_action_v2_2_1.py`
- Data source code + delta table:

#### tracking_add_item_to_bag_action_batch_v2_2_1

```python
import tecton
from common import defaults

tracking_add_item_to_bag_action_batch_v2_2_1 = tecton.BatchSource(
    name="tracking_add_item_to_bag_action_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="tracking_add_item_to_bag_action",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.TRACKING_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.tracking_add_item_to_bag_action`
Schema:

- user_id: int
- product_id: int
- created_at: timestamp
  BatchSource timestamp_field: `defaults.TRACKING_EVENT_TS`
- Transform summary:
  - Explodes `product_ids`, selects `(user_id, event_timestamp, product_id)`.
  - Aggregates `last(10)` product ids over a 7-day window.
- Transform code (pyspark):

```python
def user_previous_n_add_to_bag_product_id_v2_6_1(tracking_add_item_to_bag_action):
    from pyspark.sql import functions as f

    tracking_add_item_to_bag_action = tracking_add_item_to_bag_action.select(
        "*",
        f.explode("product_ids").alias("product_id"),
    )

    tracking_add_item_to_bag_action = tracking_add_item_to_bag_action.select(
        "user_id",
        f.col(TRACKING_EVENT_TS).alias("event_timestamp"),
        f.col("product_id").cast("string"),
    )

    return tracking_add_item_to_bag_action
```

### user_previous_n_save_product_id:v2_6_2

- Feature view: `feature_store/search/features/user_previous_n_save_product_id_v2_6_2.py`
  (`user_previous_n_save_product_id_v2_6_2`)
- Feature service: `feature_store/shared/reranker_all_features/user_services/reranking_feature_service_userid_v2_12_3.py`
- Data sources:
  - `data_sources/product_saves_create_v2_2_2.py`
- Data source code + delta table:

#### product_saves_create_batch_v2_2_2

```python
import tecton
from common import defaults

product_saves_create_batch_v2_2_2 = tecton.BatchSource(
    name="product_saves_create_batch_v2_2_2",
    batch_config=tecton.HiveConfig(
        table="product_saves_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_saves_create`
Schema:

- user_id: int
- product_id: int
- created_at: timestamp
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`
- Transform summary:
  - Selects `(user_id, event_time, product_id)` from saves.
  - Aggregates `last(10)` product ids over a 7-day window.
- Transform code (pyspark):

```python
def user_previous_n_save_product_id_v2_6_2(product_saves_create):
    from pyspark.sql import functions as f

    product_saves_create = product_saves_create.select(
        f.col("user_id").cast("int"),
        f.col(BACKEND_EVENT_TS).alias("event_time").cast("timestamp"),
        f.col("product_id").cast("string"),
    )

    return product_saves_create
```

### user_price_history_views:v2_6_2

- Feature view: `feature_store/search/features/user_price_history_views_v2_6_2.py` (`user_price_history_views_v2_6_2`)
- Feature service: `feature_store/shared/reranker_all_features/user_services/reranking_feature_service_userid_v2_12_3.py`
- Data sources:
  - `data_sources/tracking_product_view_v2_2_1.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
- Data source code + delta table:

#### tracking_product_view_batch_v2_2_1

```python
import tecton
from common import defaults

tracking_product_view_batch_v2_2_1 = tecton.BatchSource(
    name="tracking_product_view_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="tracking_product_view_by_product_id",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.TRACKING_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.tracking_product_view_by_product_id`
Schema:

- user_id: int
- product_id: int
- event_timestamp: timestamp
- price_amount: double
  BatchSource timestamp_field: `defaults.TRACKING_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`
- Transform summary:
  - De-duplicate product creates per `product_id`, join `price_amount` onto user views, and emit per-view price events.
  - Aggregates mean price over 30-day and 365-day windows.
- Transform code (pyspark):

```python
def user_price_history_views_v2_6_2(
    tracking_product_view,
    product_create,
):
    from pyspark.sql import functions as f

    pc_cols = ["product_id", "price_amount", BACKEND_EVENT_TS]
    product_create = product_create.select(*pc_cols)
    product_create = drop_duplicates(
        df=product_create,
        order_by=BACKEND_EVENT_TS,
        order_descending=False,  # keep first create row
        keep_instance=1,
        partition_by="product_id",
    ).drop(BACKEND_EVENT_TS)

    tracking_product_view = tracking_product_view.join(
        product_create,
        on="product_id",
        how="left",
    )

    tracking_product_view = tracking_product_view.select(
        f.col(TRACKING_EVENT_TS).alias("event_timestamp"),
        f.col("price_amount").cast("double"),
        "user_id",
    )

    return tracking_product_view
```

## Query Features

### query_size_impression_last_n_batch:v2_8_3

- Feature view: `feature_store/search/features/query_features/query_size_impression_last_n_v2_8_3.py`
  (`query_size_impression_last_n_batch_v2_8_3`)
- Feature service: `feature_store/search/feature_services/query_services/reranking_feature_service_query_2_8_3.py`
- Data sources:
  - `data_sources/product_search_create_v2_2_1.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
  - `data_sources/top_queries_60_percent_of_searches_2022_monthly.py` (unfiltered)
- Data source code + delta table:

#### product_search_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_search_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_search_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_search_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_search_create`
Schema:

- depop_search_id: string
- user_id: int
- what: string
- backend_event_ts: timestamp
- results: array<int>
- page: int
- limit: int
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### top_n_queries_batch

```python
from tecton import FileConfig, BatchSource
from common.environment import tecton_environment

FILE_URI = (
    f"s3://us-east-1-{tecton_environment.short_name}-svcs-ml-platform/"
    "search_reranking/tecton_file_data_sources/.../"
    "top_stemmed_queries_60_percent_of_searches_monthly_2022_09_01_2023_09_15.csv"
)

schema = ...

top_n_queries_ds = FileConfig(uri=FILE_URI, file_format="csv", schema_override=schema)

top_n_queries_batch = BatchSource(
    name="top_stemmed_queries_60_percent_of_searches_monthly_2022_09_01_2023_09_15",
    batch_config=top_n_queries_ds,
)
```

Underlying storage: S3 CSV at `FILE_URI` (not a Delta table)

- Transform summary:
  - Build ranked search results with standardized queries, join product size, remove self-interactions.
  - Select most common size per (query, user, date) and aggregate `last(100)` over 90 days.
- Transform code (pyspark):

```python
def query_size_impression_last_n_batch_v2_8_3(product_search_create, product_create, top_n_queries):
    from pyspark.sql import functions as F

    top_n_queries = top_n_queries.withColumnRenamed("query_text", "query")

    # get processed product search create
    product_search_create = product_search_create.filter(
        ((F.col("search_type") == "organic") | F.col("search_type").isNull()),
    )
    product_search_create = filter_product_search_create(product_search_create=product_search_create)
    product_search_create = standardise_product_search_create_query(
        product_search_create=product_search_create,
        top_n_queries=top_n_queries,
        impute_query=True,
        imputation_text="IMPUTATION-TEXT",
    )
    search_results = get_search_results_with_rank(result_page=product_search_create)
    search_results = search_results.withColumnRenamed(BACKEND_EVENT_TS, "search_timestamp").withColumn(
        "date",
        F.to_date(F.col("search_timestamp")),
    )

    # add size col to product create
    product_create = product_create.withColumnRenamed("user", "seller_id")
    product_create = get_product_size(
        input_table=product_create,
        variants_col="variants",
        variant_set_col="variant_set_id",
    )
    product_create = product_create.withColumn(
        "size",
        F.when(F.col("size").isNull(), F.lit(None)).otherwise(
            F.col("size"),
        ),
    )

    # join size onto search results
    product_create = common_drop_duplicates(
        df=product_create,
        order_by=BACKEND_EVENT_TS,
        keep_max_value=False,
        duplicate_col_1="product_id",
    )

    search_results = search_results.join(
        product_create.select("product_id", "size", "seller_id"),
        on="product_id",
        how="left",
    ).fillna("UNDEFINED", ["size"])
    search_results = remove_self_interactions(
        df=search_results,
        user_1_col="seller_id",
        user_2_col="user_id",
    )  # NOTE: If seller id is missing from a row it will be removed

    # get the brand with the largest count for each (query, user, date) group
    size_unique_value_counts_per_user_query = get_array_attribute_with_largest_count_over_group(
        input_table=search_results,
        attribute_column="size",
        groupBy_col_1="query",
        groupBy_col_2="user_id",
        groupBy_col_3="date",
    )

    size_unique_value_counts_per_user_query = size_unique_value_counts_per_user_query.select(
        F.col("query").cast("string"),
        F.col("search_timestamp").alias("event_time").cast("timestamp"),
        F.col("size").cast("string"),
    )

    return size_unique_value_counts_per_user_query
```

### query_brand_impression_last_n_batch:v2_8_3

- Feature view: `feature_store/search/features/query_features/query_brand_impression_last_n_v2_8_3.py`
  (`query_brand_impression_last_n_batch_v2_8_3`)
- Feature service: `feature_store/search/feature_services/query_services/reranking_feature_service_query_2_8_3.py`
- Data sources:
  - `data_sources/product_search_create_v2_2_1.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
  - `data_sources/top_queries_60_percent_of_searches_2022_monthly.py` (unfiltered)
- Data source code + delta table:

#### product_search_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_search_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_search_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_search_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_search_create`
Schema:

- depop_search_id: string
- user_id: int
- what: string
- backend_event_ts: timestamp
- results: array<int>
- page: int
- limit: int
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### top_n_queries_batch

```python
from tecton import FileConfig, BatchSource
from common.environment import tecton_environment

FILE_URI = (
    f"s3://us-east-1-{tecton_environment.short_name}-svcs-ml-platform/"
    "search_reranking/tecton_file_data_sources/.../"
    "top_stemmed_queries_60_percent_of_searches_monthly_2022_09_01_2023_09_15.csv"
)

schema = ...

top_n_queries_ds = FileConfig(uri=FILE_URI, file_format="csv", schema_override=schema)

top_n_queries_batch = BatchSource(
    name="top_stemmed_queries_60_percent_of_searches_monthly_2022_09_01_2023_09_15",
    batch_config=top_n_queries_ds,
)
```

Underlying storage: S3 CSV at `FILE_URI` (not a Delta table)

- Transform summary:
  - Build ranked search results with standardized queries, join product brand, and remove self-interactions.
  - Compute most common brand per (query, user, date), then aggregate `last(100)` over a 30-day window.
- Transform code (pyspark):

```python
def query_brand_impression_last_n_batch_v2_8_3(product_search_create, product_create, top_n_queries):
    from pyspark.sql import functions as F

    top_n_queries = top_n_queries.withColumnRenamed("query_text", "query")

    # get processed product search create
    product_search_create = product_search_create.filter(
        ((F.col("search_type") == "organic") | F.col("search_type").isNull()),
    )
    product_search_create = filter_product_search_create(product_search_create=product_search_create)
    product_search_create = standardise_product_search_create_query(
        product_search_create=product_search_create,
        top_n_queries=top_n_queries,
        impute_query=True,
        imputation_text="IMPUTATION-TEXT",
    )
    search_results = get_search_results_with_rank(result_page=product_search_create)
    search_results = search_results.withColumnRenamed(BACKEND_EVENT_TS, "search_timestamp").withColumn(
        "date",
        F.to_date(F.col("search_timestamp")),
    )

    # join brand onto search results
    product_create = product_create.withColumnRenamed("user", "seller_id")
    product_create = common_drop_duplicates(
        df=product_create,
        order_by=BACKEND_EVENT_TS,
        keep_max_value=False,
        duplicate_col_1="product_id",
    )
    search_results = search_results.join(
        product_create.select("product_id", "brand", "seller_id"),
        on="product_id",
        how="left",
    ).fillna(-1, ["brand"])
    search_results = remove_self_interactions(
        df=search_results,
        user_1_col="seller_id",
        user_2_col="user_id",
    )  # NOTE: If seller id is missing from a row it will be removed

    # get the brand with the largest count for each (query, user, date) triplet
    most_common_brand_per_day_user_query = get_attribute_with_largest_count_over_group(
        input_table=search_results,
        attribute_column="brand",
        groupBy_col_1="query",
        groupBy_col_2="user_id",
        groupBy_col2_3="date",
    )

    most_common_brand_per_day_user_query = most_common_brand_per_day_user_query.select(
        F.col("query").cast("string"),
        F.col("search_timestamp").alias("event_time").cast("timestamp"),
        F.col("brand").cast("string"),
    )

    return most_common_brand_per_day_user_query
```

### query_price_liked_mean_std_batch:v2_8_4

- Feature view: `feature_store/search/features/query_features/query_price_liked_mean_std_v2_8_4.py`
  (`query_price_liked_mean_std_batch_v2_8_4`)
- Feature service: `feature_store/search/feature_services/query_services/reranking_feature_service_query_2_8_3.py`
- Data sources:
  - `data_sources/product_search_create_v2_2_1.py`
  - `data_sources/product_like_create_v2_2_1.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
  - `data_sources/top_queries_60_percent_of_searches_2022_monthly.py` (unfiltered)
- Data source code + delta table:

#### product_search_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_search_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_search_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_search_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_search_create`
Schema:

- depop_search_id: string
- user_id: int
- what: string
- backend_event_ts: timestamp
- results: array<int>
- page: int
- limit: int
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### product_like_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_like_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_like_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_like_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_like_create`
Schema:

- sender_id: int
- product_id: int
- created_at: timestamp
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### top_n_queries_batch

```python
from tecton import FileConfig, BatchSource
from common.environment import tecton_environment

FILE_URI = (
    f"s3://us-east-1-{tecton_environment.short_name}-svcs-ml-platform/"
    "search_reranking/tecton_file_data_sources/.../"
    "top_stemmed_queries_60_percent_of_searches_monthly_2022_09_01_2023_09_15.csv"
)

schema = ...

top_n_queries_ds = FileConfig(uri=FILE_URI, file_format="csv", schema_override=schema)

top_n_queries_batch = BatchSource(
    name="top_stemmed_queries_60_percent_of_searches_monthly_2022_09_01_2023_09_15",
    batch_config=top_n_queries_ds,
)
```

Underlying storage: S3 CSV at `FILE_URI` (not a Delta table)

- Transform summary:
  - Join likes onto search results, keep only liked impressions, then attach product prices and filter outliers.
  - Aggregates mean and population stddev of liked prices over a 30-day window.
- Transform code (pyspark):

```python
def query_price_liked_mean_std_batch_v2_8_4(
    product_search_create,
    product_like_create,
    product_create,
    top_n_queries,
):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    top_n_queries = top_n_queries.withColumnRenamed("query_text", "query")

    # get processed product search create
    product_search_create = product_search_create.filter(
        ((F.col("search_type") == "organic") | F.col("search_type").isNull()),
    )
    product_search_create = filter_product_search_create(product_search_create=product_search_create)
    product_search_create = standardise_product_search_create_query(
        product_search_create=product_search_create,
        top_n_queries=top_n_queries,
        impute_query=True,
        imputation_text="IMPUTATION-TEXT",
    )
    search_results = get_search_results_with_rank(result_page=product_search_create)
    search_results = search_results.withColumnRenamed(BACKEND_EVENT_TS, "search_timestamp")

    # process product_like_create
    product_like_create = remove_self_interactions(
        df=product_like_create,
        user_1_col="sender_id",  # this is the user who liked the product
        user_2_col="user_id",  # this is the user who created the product
    )  # NOTE: If seller id is missing from a row it will be removed
    product_likes = product_like_create.drop("user_id").withColumnRenamed(
        "sender_id",
        "user_id",
    )
    # add likes to search results
    search_results = join_interaction(
        search_data=search_results,
        interaction_table=product_likes,
        output_col="search_like",
        upper_bound_attribution_window_minutes=20,
        join_keys=["user_id", "product_id"],
        interaction_cols=[],
        interaction_table_timestamp_col=BACKEND_EVENT_TS,
    ).withColumn("search_impression", F.lit(1))
    search_results = search_results.filter(
        F.col("search_like") == 1,
    )  # keep only results with a like

    # join price_amount onto search results
    product_create = product_create.withColumnRenamed("user", "seller_id")
    product_create = common_drop_duplicates(
        df=product_create,
        order_by=BACKEND_EVENT_TS,
        keep_max_value=False,
        duplicate_col_1="product_id",
    )
    search_results = search_results.join(
        product_create.select("product_id", "price_amount", "seller_id"),
        on="product_id",
        how="left",
    ).fillna(float("nan"), ["price_amount"])

    search_results = remove_self_interactions(
        df=search_results,
        user_1_col="seller_id",
        user_2_col="user_id",
    )  # NOTE: If seller id is missing from a row it will be removed

    # remove outliers
    search_results = search_results.filter(F.col("price_amount") < 1000).filter(F.col("price_amount") > 0)

    search_results = search_results.select(
        F.col("query").cast("string"),
        F.col("price_amount").cast(T.DoubleType()).alias("liked_price"),
        F.col("search_timestamp").alias("event_time").cast("timestamp"),
    )

    return search_results
```

## Product Features

### count_product_likes_batch:v2_2_2

- Feature view: `feature_store/search/features/product_likes_v2_2_2.py` (`count_product_likes_batch_v2_2_2`)
- Feature service: `feature_store/shared/reranker_all_features/product_services/reranking_feature_service_product_v2_12_7.py`
- Data sources:
  - `data_sources/product_like_create_v2_2_1.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
- Data source code + delta table:

#### product_like_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_like_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_like_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_like_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_like_create`
Schema:

- sender_id: int
- product_id: int
- created_at: timestamp
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`
- Transform summary:
  - Attach seller id to likes, remove self-interactions, de-duplicate buyer-product pairs.
  - Aggregates unique-liker counts over 6-day and 30-day windows.
- Transform code (pyspark):

```python
def count_product_likes_batch_v2_2_2(
    product_like_create,
    product_create,
):
    from pyspark.sql import functions as f

    product_create = product_create.withColumnRenamed("user", "seller_id")
    product_likes = add_seller_id(
        df_to_add_seller_to=product_like_create,
        df_with_seller_id=product_create,
    )

    product_likes = remove_self_interactions(
        df=product_likes,
        user_1_col="seller_id",
        user_2_col="sender_id",
    )

    product_likes = drop_duplicates(
        df=product_likes,
        order_by=BACKEND_EVENT_TS,
        order_descending=False,
        keep_instance=1,
        partition_by_1="product_id",
        partition_by_2="sender_id",
    )  # if there are multiple product-buyer pairs, i.e. a buyer liked the item
    #  multiple times. Choose the most recent like as the only liked.

    product_likes = product_likes.withColumnRenamed("sender_id", "product_like")

    product_likes = product_likes.select(
        f.col(BACKEND_EVENT_TS).alias("event_time"),
        "product_id",
        "product_like",
    )

    return product_likes
```

### count_product_saves_batch:v2_2_3

- Feature view: `feature_store/search/features/product_saves_v2_2_3.py` (`count_product_saves_batch_v2_2_3`)
- Feature service: `feature_store/shared/reranker_all_features/product_services/reranking_feature_service_product_v2_12_7.py`
- Data sources:
  - `data_sources/product_saves_create_v2_2_2.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
- Data source code + delta table:

#### product_saves_create_batch_v2_2_2

```python
import tecton
from common import defaults

product_saves_create_batch_v2_2_2 = tecton.BatchSource(
    name="product_saves_create_batch_v2_2_2",
    batch_config=tecton.HiveConfig(
        table="product_saves_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_saves_create`
Schema:

- user_id: int
- product_id: int
- created_at: timestamp
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`
- Transform summary:
  - Attach seller id to saves, remove self-interactions, de-duplicate buyer-product pairs.
  - Aggregates save counts over 6-day and 30-day windows.
- Transform code (pyspark):

```python
def count_product_saves_batch_v2_2_3(
    product_saves_create,
    product_create,
):
    from pyspark.sql import functions as f

    product_create = product_create.withColumnRenamed("user", "seller_id")

    product_saves = add_seller_id(
        df_to_add_seller_to=product_saves_create,
        df_with_seller_id=product_create,
    )

    product_saves = remove_self_interactions(
        df=product_saves,
        user_1_col="seller_id",
        user_2_col="user_id",
    )

    product_saves = drop_duplicates(
        df=product_saves,
        order_by=BACKEND_EVENT_TS,
        order_descending=0,
        keep_instance=1,
        partition_by_1="product_id",
        partition_by_2="user_id",
    )  # if there are multiple product-buyer pairs, i.e. a buyer liked the item
    #  multiple times. Choose the most recent like as the only liked.

    product_saves = product_saves.withColumn("user_id", f.col("user_id").cast("int"))
    product_saves = product_saves.withColumnRenamed("user_id", "product_save")

    product_saves = product_saves.select(
        f.col(BACKEND_EVENT_TS).alias("event_time"),
        f.col("product_id").cast("int"),
        "product_save",
    )

    return product_saves
```

### count_product_add_to_bag_batch:v2_2_3

- Feature view: `feature_store/search/features/product_add_to_bag_v2_2_3.py`
  (`count_product_add_to_bag_batch_v2_2_3`)
- Feature service: `feature_store/shared/reranker_all_features/product_services/reranking_feature_service_product_v2_12_7.py`
- Data sources:
  - `data_sources/tracking_add_item_to_bag_action_v2_2_1.py`
  - `data_sources/product_create_v2_2_1.py` (unfiltered)
- Data source code + delta table:

#### tracking_add_item_to_bag_action_batch_v2_2_1

```python
import tecton
from common import defaults

tracking_add_item_to_bag_action_batch_v2_2_1 = tecton.BatchSource(
    name="tracking_add_item_to_bag_action_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="tracking_add_item_to_bag_action",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.TRACKING_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.tracking_add_item_to_bag_action`
Schema:

- user_id: int
- product_id: int
- created_at: timestamp
  BatchSource timestamp_field: `defaults.TRACKING_EVENT_TS`

#### product_create_batch_v2_2_1

```python
import tecton
from common import defaults

product_create_batch_v2_2_1 = tecton.BatchSource(
    name="product_create_batch_v2_2_1",
    batch_config=tecton.HiveConfig(
        table="product_create",
        database=defaults.DATALAKE_SILVER_UNION_DATABASE,
        timestamp_field=defaults.BACKEND_EVENT_TS,
        data_delay=defaults.DATA_DELAY_HOURS,
    ),
    ...,
)
```

Delta table: `defaults.DATALAKE_SILVER_UNION_DATABASE.product_create`
Schema:

- product_id: int
- price_amount: double
  BatchSource timestamp_field: `defaults.BACKEND_EVENT_TS`
- Transform summary:
  - Explode product_ids, attach seller id, remove self-interactions, and de-duplicate buyer-product pairs.
  - Aggregates add-to-bag counts over 6-day and 30-day windows.
- Transform code (pyspark):

```python
def count_product_add_to_bag_batch_v2_2_3(tracking_add_item_to_bag_action, product_create):
    from pyspark.sql import functions as f

    tracking_add_item_to_bag_action = tracking_add_item_to_bag_action.select(
        f.col(TRACKING_EVENT_TS),
        f.col("user_id"),
        f.col("product_ids"),
    )

    product_add_to_bag = tracking_add_item_to_bag_action.withColumn(
        "product_id",
        f.explode("product_ids"),
    )  # product_ids is always of length 1

    product_create = product_create.withColumnRenamed("user", "seller_id")

    product_add_to_bag = add_seller_id(
        df_to_add_seller_to=product_add_to_bag,
        df_with_seller_id=product_create,
    )

    product_add_to_bag = remove_self_interactions(
        df=product_add_to_bag,
        user_1_col="seller_id",
        user_2_col="user_id",
    )

    product_add_to_bag = drop_duplicates(
        df=product_add_to_bag,
        order_by=TRACKING_EVENT_TS,
        order_descending=False,
        keep_instance=1,
        partition_by_1="product_id",
        partition_by_2="user_id",
    )

    product_add_to_bag = product_add_to_bag.withColumnRenamed("user_id", "product_add_to_bag")

    product_add_to_bag = product_add_to_bag.select(
        f.col(TRACKING_EVENT_TS).alias("event_timestamp").cast("timestamp"),
        f.col("product_id").cast("int"),
        f.col("product_add_to_bag").cast("long"),
    )

    return product_add_to_bag
```

## Helper Functions (Referenced in Transforms)

### `common/transformations.py`

#### common_drop_duplicates

```python
def common_drop_duplicates(
    df,
    order_by: str,
    keep_max_value: bool = True,
    tiebreak_col=None,
    **duplicate_col: str,
):
    """
    Drop duplicate rows. Groups where all duplicate_col keys are equal are
    sorted by the order_by key in either descending or ascending order
    (based on keep_max_value bool), then the first row is kept for each group.
    Args:
        df: spark dataframe
        order_by: column to order by
        keep_max_value: whether to keep the largest or smallest value of order_by
            col.
        tiebreak_col: column to use to break ties if there are multiple rows with
            the same value for order_by col. If None, a column called "tiebreak"
            is created and used.
        duplicate_col: columns to use to identify duplicate rows. Can be given
            any name as long as they don't clash with args already in use. for
            example, use duplicate_col_1, duplicate_col_2 etc.
    Returns:
        df: spark dataframe with duplicate rows dropped.
    """
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    if tiebreak_col is None:
        df = df.withColumn("tiebreak", F.monotonically_increasing_id())
    else:
        df = df.withColumn("tiebreak", F.col(tiebreak_col))

    if keep_max_value:
        window = Window.partitionBy(*duplicate_col.values()).orderBy(F.col(order_by).desc(), F.col("tiebreak").desc())
    else:
        window = Window.partitionBy(*duplicate_col.values()).orderBy(F.col(order_by).asc(), F.col("tiebreak").desc())

    df = df.withColumn("rank", F.rank().over(window)).filter(F.col("rank") == 1).drop("rank", "tiebreak")
    return df
```

#### drop_duplicates

```python
def drop_duplicates(
    df,
    order_by: str,
    order_descending: bool = True,
    keep_instance: int = 1,
    **partition_by: str,
):
    """Drop duplicate rows. Groups where all partition_by keys are equal are
    sorted by the order_by key in either descending or ascending order, then the
    keep_instanceth row is kept for each group.
    Partition_by args can be given any name as long as they don't clash with
    args already in use. for example, use partition_by_1, partition_by_2 etc."""

    from pyspark.sql.window import Window
    from pyspark.sql import functions as f

    if order_descending:
        window = Window.partitionBy(*partition_by.values()).orderBy(
            f.desc(order_by),
        )
    else:
        window = Window.partitionBy(*partition_by.values()).orderBy(
            f.asc(order_by),
        )

    df = df.withColumn("rn", f.row_number().over(window))
    deduped_df = df.filter(f.col("rn") == keep_instance).drop("rn")

    return deduped_df
```

#### add_seller_id

```python
def add_seller_id(df_to_add_seller_to, df_with_seller_id):
    """set of operations to add seller_id column to df_to_add_seller_to by
    joining on product_id. product_id column must be present in
    df_to_add_seller_to. Note that duplicate product_id-seller_id pairs will be
    dropped (the most recent will be kept)."""

    from pyspark.sql.window import Window
    from pyspark.sql import functions as f

    # first get seller-product mapping
    # drop_duplicates
    window = Window.partitionBy(["seller_id", "product_id"]).orderBy(
        f.desc("created_date"),  # which one is chosen doesn't matter, I guess newer is more likely to be correct?
    )
    df_with_seller_id = df_with_seller_id.withColumn("rn", f.row_number().over(window))  # number rows by
    # order_by param
    df_with_seller_id = df_with_seller_id.filter(f.col("rn") == 1).drop("rn")  # keep just
    # the keep_instanceth instance

    # select_cols
    product_seller_mapping = df_with_seller_id.select("seller_id", "product_id")

    # add seller id
    df_with_sellerid = df_to_add_seller_to.join(product_seller_mapping, on="product_id", how="left")

    return df_with_sellerid
```

#### remove_self_interactions

```python
def remove_self_interactions(
    df,
    user_1_col: str,
    user_2_col: str,
):
    """remove rows where the value of user_1_col == user_2_col."""
    from pyspark.sql import functions as f

    df_no_self_int = df.filter(f.col(user_1_col) != f.col(user_2_col))
    # be aware that if either col is null that that row will also be dropped as
    # null is undefined in SQL

    return df_no_self_int
```

#### process_text (shortened)

```python
def process_text(
    input_df,
    input_col: str,
    output_col: str,
):
    from pyspark.sql import functions as F
    from nltk.stem.snowball import SnowballStemmer

    snowball = SnowballStemmer("english")
    # lower-case, remove punctuation/whitespace, stem via snowball
    ...
```

#### get_product_size

```python
def get_product_size(
    input_table,
    variants_col: str,
    variant_set_col: str,
    null_fill_value: str = "UNDEFINED",
):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    input_table = (
        input_table.withColumn(
            "variant_ids",
            F.map_keys(F.from_json(variants_col, T.MapType(T.StringType(), T.LongType()))),
        )
        .withColumn(
            "variant_ids",
            F.col("variant_ids").cast(T.ArrayType(T.LongType())),
        )
        .withColumn(
            "size",
            F.udf(get_unique_size_strings_udf, T.ArrayType(T.StringType()))(
                variant_set_col,
                "variant_ids",
            ),
        )
    )
    input_table = input_table.fillna(value=null_fill_value, subset=["size"])

    return input_table
```

#### get_unique_size_strings_udf

```python
def get_unique_size_strings_udf(variant_set_id, variant_ids):
    if not variant_ids:
        return None
    return [f"{variant_set_id}.{variant_id}" for variant_id in variant_ids]
```

### `feature_store/search/utils/generic_search_transformations.py`

#### filter_psc_country_duplicates (shortened)

```python
def filter_psc_country_duplicates(
    product_search_df,
):
    from pyspark.sql import functions as F

    product_search_df = product_search_df.filter(~F.col("filters").contains("rest_of_world_search"))
    return product_search_df
```

#### filter_product_search_create

```python
def filter_product_search_create(product_search_create):
    from pyspark.sql import functions as F

    product_search_create = product_search_create.filter(
        (~F.col("depop_search_id").isNull())
        & (~F.col("session_id").isNull())
        & (F.col("depop_search_id") != "")
        & (~F.col("page").isNull())
        & (~F.col("results").isNull())
        & (F.col("results") != F.array()),
    )

    product_search_create = filter_psc_country_duplicates(product_search_df=product_search_create)
    return product_search_create
```

#### standardise_product_search_create_query

```python
def standardise_product_search_create_query(
    product_search_create,
    top_n_queries,
    impute_query: bool = True,
    imputation_text: str = "IMPUTATION-TEXT",
):
    from pyspark.sql import functions as F

    product_search_create = process_text(
        input_df=product_search_create,
        input_col="what",
        output_col="query",
    )

    if impute_query:
        top_n_queries = top_n_queries.withColumn("query_placeholder", F.col("query"))
        product_search_create = product_search_create.join(top_n_queries, on="query", how="left")
        product_search_create = product_search_create.withColumn(
            "query",
            F.when(F.col("query_placeholder").isNull(), F.lit(imputation_text)).otherwise(F.col("query_placeholder")),
        ).drop("query_placeholder")
    else:
        product_search_create = product_search_create.join(top_n_queries, on="query", how="inner")

    return product_search_create
```

#### get_search_results_with_rank

```python
def get_search_results_with_rank(result_page):
    """
    From a spark dataframe with search results, add a rank column which is the
    position of the product_id in the results array. The rank is calculated
    based on the page and limit fields. The rank is 1-indexed.
    NOTE: there is an assumption in here that the limit field will never change
    within a search. If it does then it could give in incorrect order, e.g.
    if page 2 has a longer limit, then the rank positions for page 2 will be
    too high, and page 3 items would come before page 2.
    However, seems very unlikely
    web: hardcoded to 24 always
    ios: hardcoded to 28 always.
    Args:
        result_page: spark dataframe with search results. Must contain "page",
            "limit" and "results" columns. "results" column must be an array
            of product_ids.
    Returns:
        result_page: spark dataframe with "rank" and "product_id" columns added.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    results = result_page.select(
        "*",
        F.posexplode_outer(
            "results",
        ).alias("rank_in_page", "product_id"),
    )

    window_spec = Window.partitionBy("depop_search_id").orderBy(F.asc("page"), F.asc("rank_in_page"))

    results = results.withColumn(
        "rank",
        F.row_number().over(window_spec),
    ).drop("rank_in_page", "results")

    return results
```

#### join_interaction (shortened)

```python
def join_interaction(
    search_data,
    interaction_table,
    output_col: str,
    upper_bound_attribution_window_minutes: int,
    interaction_cols,
    join_keys,
    interaction_table_timestamp_col: str,
):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    upper_bound_attribution_window_seconds = upper_bound_attribution_window_minutes * 60
    search_data = search_data.join(
        interaction_table.repartition(*join_keys).select(*join_keys, *interaction_cols, interaction_table_timestamp_col),
        on=join_keys,
        how="left",
    )

    search_data = (
        search_data.withColumn(
            f"{output_col}_timestamp_unix",
            F.unix_timestamp(F.to_timestamp(interaction_table_timestamp_col)),
        )
        .withColumn("search_timestamp_unix", F.unix_timestamp("search_timestamp"))
        .withColumn(
            f"{output_col}_time_difference",
            F.col(f"{output_col}_timestamp_unix") - F.col("search_timestamp_unix"),
        )
        .withColumn(
            output_col,
            F.when(
                (F.col(f"{output_col}_time_difference") < 0)
                | (F.col(f"{output_col}_time_difference") > upper_bound_attribution_window_seconds)
                | (F.col(f"{output_col}_time_difference").isNull()),
                F.lit(0),
            ).otherwise(F.lit(1)),
        )
        .drop("search_timestamp_unix", f"{output_col}_timestamp_unix")
    )

    window_spec = (
        Window()
        .partitionBy(["depop_search_id", "product_id", "rank"])
        .orderBy(F.col(output_col).desc(), F.col(f"{output_col}_time_difference").asc())
    )
    return (
        search_data.withColumn("nth_interaction", F.row_number().over(window_spec))
        .filter(F.col("nth_interaction") == 1)
        .drop("nth_interaction")
    )
```

### `feature_store/search/utils/query_specific_transformations.py`

#### get_attribute_with_largest_count_over_group

```python
def get_attribute_with_largest_count_over_group(input_table, attribute_column: str, **groupBy_cols: str):
    """
    Function to get the attribute value with the largest count over a group.
    If there are multiple attribute values with the same count, the one with the
    largest count over the groupBy_cols is kept.

    Args:
        input_table: Spark dataframe containing the attribute column
        attribute_column: Name of the attribute column
    Returns:
        Spark dataframe with the attribute query affinity
    """
    import pyspark.sql.functions as F

    attribute_count_col = f"{attribute_column}_count"

    attribute_value_counts = input_table.groupBy(attribute_column, *groupBy_cols.values()).agg(
        F.count(attribute_column).alias(attribute_count_col),
        F.last("search_timestamp").alias("search_timestamp"),
    )
    attribute_value_counts = common_drop_duplicates(
        df=attribute_value_counts,
        order_by=attribute_count_col,
        keep_max_value=True,
        tiebreak_col=attribute_column,
        **groupBy_cols,
    )

    return attribute_value_counts
```

#### get_array_attribute_with_largest_count_over_group

```python
def get_array_attribute_with_largest_count_over_group(input_table, attribute_column: str, **groupBy_cols: str):
    """
    Function to get the attribute value with the largest count over a group.
    If there are multiple attribute values with the same count, the one with the
    largest count over the groupBy_cols is kept.

    Args:
        input_table: Spark dataframe containing the attribute column
        attribute_column: Name of the attribute column
    Returns:
        Spark dataframe with the attribute query affinity
    """
    import pyspark.sql.functions as F

    input_table_exploded = input_table.withColumn(
        attribute_column,
        F.explode(F.col(attribute_column)),
    )

    attribute_value_counts = get_attribute_with_largest_count_over_group(
        input_table_exploded,
        attribute_column,
        **groupBy_cols,
    )

    return attribute_value_counts
```