from ai.chronon.data_types import DataType
from ai.chronon.model import InferenceSpec, Model, ModelBackend

statistics = DataType.STRUCT("statistics", ("truncated", DataType.BOOLEAN), ("token_count", DataType.INT))
values = DataType.LIST(DataType.DOUBLE)
embeddings = DataType.STRUCT("embeddings", ("statistics", statistics), ("values", values))

v1 = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",
        },
    ),
    # Combine the last 100 event:category pairs into a single text string for embedding.
    # user_id_event_category_pair_last100_7d is the LAST_K(100) column from user_category_aggregates,
    # prefixed with the join key name by the wrapper join.
    input_mapping={
        "instance": "named_struct('content', array_join(user_id_event_category_pair_last100_7d, ', '))",
    },
    output_mapping={
        "user_category_embedding": "gcp_user_category_embedding_v1__1__embeddings.values",
    },
    value_fields=[
        ("embeddings", embeddings),
    ],
)
