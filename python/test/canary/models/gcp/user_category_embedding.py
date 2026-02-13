from ai.chronon.model import Model, ModelBackend, InferenceSpec
from ai.chronon.data_types import DataType

"""
This model takes user activity patterns (event_type and primary_category pairs)
and generates embeddings using Vertex AI's gemini-embedding-001 model.

The input is a concatenated string of event-category pairs over a 7-day window,
which is then converted to a vector embedding for semantic similarity and ML applications.
"""

# Define the output schema for gemini-embedding-001
# Based on: https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings
statistics = DataType.STRUCT("statistics", ("truncated", DataType.BOOLEAN), ("token_count", DataType.INT))
values = DataType.LIST(DataType.DOUBLE)
embeddings = DataType.STRUCT("embeddings", ("statistics", statistics), ("values", values))

user_category_embedding_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",
        }
    ),
    # Convert the array of event-category pairs into a concatenated string
    # Example: [STRUCT('view', 'electronics'), STRUCT('click', 'books')]
    #       -> "view:electronics; click:books"
    input_mapping={
        "instance": """named_struct('content',
            array_join(
                transform(
                    user_id_event_category_pair_last100_7d,
                    x -> concat(x.event_type, ':', x.primary_category)
                ),
                '; '
            )
        )""",
    },
    output_mapping={
        "user_category_embedding": "gcp_user_category_embedding_user_category_embedding_model__1__embeddings.values"
    },
    value_fields=[
        ("embeddings", embeddings),
    ]
)
