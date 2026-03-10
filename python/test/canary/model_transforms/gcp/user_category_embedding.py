from joins.gcp import user_category_features
from models.gcp import user_category_embedding

from ai.chronon.data_types import DataType
from ai.chronon.model import ModelTransforms
from ai.chronon.query import Query
from ai.chronon.source import JoinSource

v1 = ModelTransforms(
    sources=[
        JoinSource(
            join=user_category_features.user_category_features_join,
            query=Query(
                wheres=["user_id_event_category_pair_last100_7d IS NOT NULL"],
            ),
        )
    ],
    models=[user_category_embedding.v1],
    passthrough_fields=["user_id"],
    key_fields=[
        ("user_id_event_category_pair_last100_7d", DataType.LIST(DataType.STRING)),
    ],
    version=1,
    output_namespace="data",
)
