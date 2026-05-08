from joins.gcp import demo

from ai.chronon.types import ModelRuntime, JoinSource, Model, ModelBackend, Inference, Query, selects
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
    runtime=ModelRuntime(
        backend=ModelBackend.VERTEXAI,
        params={
            "model_name": "gemini-embedding-001",
            "model_type": "publisher",
        }
    ),
    inputs={
        "instance": "named_struct('content', concat_ws('; ', listing_id_headline, listing_id_long_description))",
    },
    outputs={
        "item_embedding": "gcp_listing_item_description_model__1__embeddings.values"
    },
    # captures the schema of the model output as documented in:
    # https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings
    output_fields=[
        ("embeddings", embeddings),
    ]
)

# This model is currently un-used but shows how to create an image embedding from a GCS path
item_img_model = Model(
    version="001",
    runtime=ModelRuntime(
        backend=ModelBackend.VERTEXAI,
        params={
            "model_name": "multimodalembedding",
            "model_type": "publisher",
            "version": "001",
            "dimension": "512"
        }
    ),
    inputs={
        "instance": "named_struct('image', named_struct('gcsUri', listing_id_main_image_path), 'text','')",
    },
    outputs={
         "image_embedding": "gcp_listing_item_img_model__001__imageEmbedding"
    },
    # captures the schema of the model output as documented in (differs from the text embedding response):
    # https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-multimodal-embeddings
    output_fields=[
        ("imageEmbedding", values),
        ("textEmbedding", values)
    ]
)

