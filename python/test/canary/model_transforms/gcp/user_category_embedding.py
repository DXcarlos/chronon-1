from joins.gcp import user_category_features
from models.gcp import user_category_embedding

from ai.chronon.model import ModelTransforms
from ai.chronon.query import Query
from ai.chronon.source import JoinSource
from ai.chronon.data_types import DataType

"""
This ModelTransforms applies the gemini-embedding-001 model to user activity patterns.

It takes the last 100 (event_type, primary_category) pairs from a 7-day window,
converts them to a concatenated string, and generates semantic embeddings.

Output: user_id with corresponding behavioral embedding vector
"""

source = JoinSource(
    join=user_category_features.user_category_features_join,
    query=Query(
        # Filter out users with no activity data
        wheres=["user_id_event_category_pair_last100_7d IS NOT NULL AND SIZE(user_id_event_category_pair_last100_7d) > 0"]
    )
)

v1 = ModelTransforms(
    sources=[source],
    models=[user_category_embedding.user_category_embedding_model],
    # Pass through user_id and the raw activity patterns alongside the embedding
    passthrough_fields=["user_id", "user_id_event_category_pair_last100_7d"],
    # Input field to the model (the array of event-category pairs)
    key_fields=[
        ("user_id_event_category_pair_last100_7d", DataType.LIST(
            DataType.STRUCT("event_category_pair",
                ("event_type", DataType.STRING),
                ("primary_category", DataType.STRING)
            )
        ))
    ],
    version=1,
    output_namespace="data"
)
