from joins.aws import demo

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
        backend=ModelBackend.SAGEMAKER,
        params={
            "model_name": "amazon-titan-embed-text-v1",
            "model_type": "bedrock",
        }
    ),
    inputs={
        "instance": "named_struct('content', concat_ws('; ', listing_id_headline, listing_id_long_description))",
    },
    outputs={
        "item_embedding": "aws_listing_item_description_model__1__embeddings.values"
    },
    # captures the schema of the model output
    output_fields=[
        ("embeddings", embeddings),
    ]
)

# This model is currently un-used but shows how to create an image embedding from an S3 path
item_img_model = Model(
    version="001",
    runtime=ModelRuntime(
        backend=ModelBackend.SAGEMAKER,
        params={
            "model_name": "amazon-multimodal-embedding",
            "model_type": "bedrock",
            "dimension": "512"
        }
    ),
    inputs={
        "instance": "named_struct('image', named_struct('s3Uri', listing_id_main_image_path), 'text','')",
    },
    outputs={
         "image_embedding": "aws_listing_item_img_model__001__imageEmbedding"
    },
    # captures the schema of the model output
    output_fields=[
        ("imageEmbedding", values),
        ("textEmbedding", values)
    ]
)
