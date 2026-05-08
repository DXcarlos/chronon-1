import gen_thrift.api.ttypes as ttypes
from ai.chronon.types import (
    DataType,
    EventSource,
    Infer,
    Inference,
    Model,
    Query,
    Resources,
    Serve,
    Train,
)


def test_model_authoring_names_compile_to_canonical_thrift_fields():
    training_source = EventSource(table="warehouse.labels", query=Query())

    model = Model(
        version="1.0",
        inputs={"instance": "named_struct('clicks_7d', clicks_7d)"},
        outputs={"ctr": "ctr_model__score"},
        output_fields=[("score", DataType.DOUBLE)],
        artifact_uri="s3://models/ctr",
        train=Train(
            data=training_source,
            image="repo/ctr-trainer:v1",
            entrypoint="trainer.train",
            resources=Resources(min_replica_count=1, max_replica_count=1, machine_type="cpu-small"),
            params={"max_depth": "4"},
        ),
        serve=Serve(
            image="repo/ctr-server:v1",
            endpoint="ctr",
            infer_route="/infer",
            resources=Resources(min_replica_count=1, max_replica_count=3, machine_type="cpu-small"),
            env={"MODEL_NAME": "ctr"},
        ),
    )

    assert model.inputs == {"instance": "named_struct('clicks_7d', clicks_7d)"}
    assert model.outputs == {"ctr": "ctr_model__score"}
    assert model.artifactUri == "s3://models/ctr"
    assert model.train.data.events.table == "warehouse.labels"
    assert model.train.entrypoint == "trainer.train"
    assert model.train.params == {"max_depth": "4"}
    assert model.train.resources.machineType == "cpu-small"
    assert model.serve.container.image == "repo/ctr-server:v1"
    assert model.serve.container.inferRoute == "/infer"
    assert model.serve.container.env == {"MODEL_NAME": "ctr"}
    assert model.serve.endpoint.endpointName == "ctr"
    assert model.serve.resources.maxReplicaCount == 3


def test_inference_names_compile_to_canonical_thrift():
    model = Model(version="1.0")
    model.metaData.name = "test_team.ctr"
    feature_source = EventSource(table="warehouse.features", query=Query())

    inference = Inference(
        features=feature_source,
        models=[model],
        version=1,
        passthrough=["user_id"],
    )
    inferred = Infer(
        features=feature_source,
        models=[model],
        version=1,
        passthrough=["user_id"],
    )

    assert isinstance(inference, ttypes.Inference)
    assert isinstance(inferred, ttypes.Inference)
    assert inference.features[0].events.table == "warehouse.features"
    assert inference.models[0].metaData.name == "test_team.ctr"
    assert inference.passthrough == ["user_id"]
    assert inferred.passthrough == ["user_id"]
