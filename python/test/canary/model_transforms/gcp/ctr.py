from joins.gcp import demo
from models.gcp import click_through_rate

# Create a ModelTransforms where we enrich the demo join fields with the ctr model score
from ai.chronon.model import ModelTransforms
from ai.chronon.source import JoinSource

from ai.chronon.data_types import DataType

source = JoinSource(join=demo.ctr_features_v1)

# [{"user_id": "user_1"}]
v1 = ModelTransforms(
    sources=[source], # noticed that this source is used in both ModelTransform and Models
    models=[click_through_rate.ctr_model],
    # include relevant pass through fields from the source / join lookup
    passthrough_fields=["user_id", "listing_id", "user_id_click_event_average_7d", "listing_id_price_cents", "price_log", "price_bucket"],
    version=1,
    output_namespace="data",
    key_fields=[
        ("user_id_click_event_average_7d", DataType.DOUBLE),
        ("listing_id_price_cents", DataType.LONG),
        ("price_log", DataType.DOUBLE),
        ("price_bucket", DataType.INT)
    ]
)


source2 = JoinSource(join=demo.ctr_features_v2_predemo1)

v2 = ModelTransforms(
    sources=[source2], # noticed that this source is used in both ModelTransform and Models
    models=[click_through_rate.ctr_model_v2predemo],
    # include relevant pass through fields from the source / join lookup
    passthrough_fields=["user_id", "listing_id", "user_id_click_event_average_7d", "listing_id_price_cents", "price_log", "price_bucket"],
    version=1,
    output_namespace="data",
    key_fields=[
        ("user_id_click_event_average_7d", DataType.DOUBLE),
        ("listing_id_price_cents", DataType.LONG),
        ("price_log", DataType.DOUBLE),
        ("price_bucket", DataType.INT)
    ]
)
