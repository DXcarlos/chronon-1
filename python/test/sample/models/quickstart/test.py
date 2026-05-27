from gen_thrift.api.ttypes import EventSource, Source

from ai.chronon.types import InferenceSpec, Model, ModelBackend, ModelTransforms, Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects("user_id"),
            time_column="ts",
        ),
    )
)

model = Model(
    version="1.0",
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={"model_type": "xgboost"}
    )
)

test_model_transforms = ModelTransforms(
    sources=[source],
    models=[model],
    passthrough_fields=["user_id"],
    version=1
)
