from joins.gcp import demo

from ai.chronon.model import Model, ModelBackend, InferenceSpec, ModelTransforms
from ai.chronon.query import Query, selects
from ai.chronon.source import JoinSource
from ai.chronon.data_types import DataType

"""
This model takes some of the listing related fields from the demo join and uses that
to build up a couple of listing related embeddings
"""

statistics = DataType.STRUCT("statistics", ("truncated", DataType.BOOLEAN), ("token_count", DataType.INT) )
values = DataType.LIST(DataType.DOUBLE)
embeddings = DataType.STRUCT("embeddings", ("statistics", statistics), ("values", values))

item_description_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",
        }
    ),
    input_mapping={
        "instance": "named_struct('content', concat_ws('; ', listing_id_headline, listing_id_long_description))",
    },
    output_mapping={
        "item_embedding": "gcp_listing_item_description_model__1__embeddings.values"
    },
    # captures the schema of the model output as documented in:
    # https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings
    value_fields=[
        ("embeddings", embeddings),
    ]
)

# This model is currently un-used but shows how to create an image embedding from a GCS path
item_img_model = Model(
    version="001",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "multimodalembedding",
            "model_type": "publisher",
            "version": "001",
            "dimension": "512"
        }
    ),
    input_mapping={
        "instance": "named_struct('image', named_struct('gcsUri', listing_id_main_image_path), 'text','')",
    },
    output_mapping={
         "image_embedding": "gcp_listing_item_img_model__001__imageEmbedding"
    },
    # captures the schema of the model output as documented in (differs from the text embedding response):
    # https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-multimodal-embeddings
    value_fields=[
        ("imageEmbedding", values),
        ("textEmbedding", values)
    ]
)

