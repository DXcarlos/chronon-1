from joins.gcp import demo

from ai.chronon.model import Model, ModelBackend, InferenceSpec, ModelTransforms
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource

"""
This model takes the user activity sequence from the demo search_v0 join and transforms it
into a user embedding that captures behavioral patterns, preferences, and risk signals.
"""

source = JoinSource(join=demo.search_v0)

activities_v0 = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",
        }
    ),
    input_mapping={
        "instance": "user_id_user_event_struct_last128_30d",
        "parameters": "CAST(map() AS MAP<STRING, STRING>)"
    },
    output_mapping={
        "user_sequence_embedding": "predictions[0].embeddings.values"
    }
)

# Create user_sequence model transforms
v1 = ModelTransforms(
    sources=[source],
    models=[activities_v0],
    passthrough_fields=["user_id"],
    version=0,
    output_namespace="models"
)
