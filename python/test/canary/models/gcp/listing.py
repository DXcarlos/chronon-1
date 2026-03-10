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

# Generates a short product description from long_description using Gemini text generation.
# Input instance format follows Gemini's contents API (role/parts structure).
# value_fields mirrors the Gemini predict response schema:
#   predictions[i].candidates[0].content.parts[0].text
# Verify against: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/gemini
part = DataType.STRUCT("part", ("text", DataType.STRING))
content_inner = DataType.STRUCT("content_inner", ("parts", DataType.LIST(part)), ("role", DataType.STRING))
candidate = DataType.STRUCT("candidate", ("content", content_inner), ("finishReason", DataType.STRING))

listing_short_description_model = Model(
    version="1",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "gemini-1.5-flash-001",
            "model_type": "publisher",
            "maxOutputTokens": "128",
            "temperature": "0.4",
        },
    ),
    input_mapping={
        "instance": "named_struct('contents', array(named_struct('role', 'user', 'parts', array(named_struct('text', CONCAT('Write a concise short product description (2-3 sentences) for an online store listing. Long description: ', listing_id_long_description))))))",
    },
    output_mapping={
        "short_description": "gcp_listing_listing_short_description_model__1__candidates[0].content.parts[0].text",
    },
    value_fields=[
        ("candidates", DataType.LIST(candidate)),
    ],
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

