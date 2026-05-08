from staging_queries.aws import ctr_labels

from ai.chronon.types import Serve, DeploymentStrategyType, EndpointConfig, EventSource, ModelRuntime, JoinSource, Model, ModelBackend, Inference, Query, Resources, RolloutStrategy, ServingContainerConfig, TimeUnit, Train, Window, selects

from ai.chronon.data_types import DataType

"""
This model takes into account features computed as part of the demo.derivations_v1 join
and uses it as an input to predict click_through_rate.
"""

label_source = EventSource(table=ctr_labels.v1.table, query = Query(
    selects= selects(
        user_id_click_event_average_7d="user_id_click_event_average_7d",
        listing_price_cents="listing_id_price_cents",
        price_log="price_log",
        price_bucket="price_bucket",
        label="label",
        ds="ds"
    ),
    start_partition="2025-07-01",
))

ctr_model = Model(
    version="1.0",
    runtime=ModelRuntime(
        backend=ModelBackend.SAGEMAKER,
        params={
            "model_name": "test_ctr_model",
            "model_type": "custom",
        }
    ),
    inputs={
        "instance": "named_struct('user_id_click_event_average_7d', user_id_click_event_average_7d, 'listing_price_cents', listing_id_price_cents, "
        "'price_log', price_log, 'price_bucket', price_bucket)",
    },
    outputs={
        "ctr": "score"
    },
    # captures the schema of the model output
    output_fields=[
        ("score", DataType.DOUBLE),
    ],
    artifact_uri="s3://zipline-warehouse-models",
    # Model build is expected to be in - s3://zipline-warehouse-models/builds/test_ctr_model-1.0.tar.gz
    train=Train(
        data=label_source,
        window=Window(length=1, time_unit=TimeUnit.DAYS),
        schedule="@daily",
        image="763104351884.dkr.ecr.us-east-1.amazonaws.com/xgboost-training:latest",
        # Py module coordinates are optional unless they differ from the default
        entrypoint="trainer.train",
        resources=Resources(
            min_replica_count=1,
            max_replica_count=1,
            machine_type="ml.m5.xlarge"
        ),
        params={
            "n-samples": "1000",
            "max-depth": "4",
            "eta": "0.1",
            "num-boost-round": "50"
        }
    ),
    serve=Serve(
        container=ServingContainerConfig(
            image="763104351884.dkr.ecr.us-east-1.amazonaws.com/xgboost-inference:latest"
        ),
        endpoint=EndpointConfig(
            endpoint_name="test_ctr_model"
        ),
        resources=Resources(
            min_replica_count=3,
            max_replica_count=10,
            machine_type="ml.m5.xlarge"
        ),
        rollout=RolloutStrategy(
            # More sophisticated deployment strategies (e.g. blue/green) and
            # gradual traffic ramps are possible as well
            rollout_type=DeploymentStrategyType.IMMEDIATE,
        )
    )
)
