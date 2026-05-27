from staging_queries.gcp import ctr_labels

from ai.chronon.types import DeploymentSpec, DeploymentStrategyType, EndpointConfig, EventSource, InferenceSpec, JoinSource, Model, ModelBackend, ModelTransforms, Query, ResourceConfig, RolloutStrategy, ServingContainerConfig, TimeUnit, TrainingSpec, Window, selects

from ai.chronon.data_types import DataType

"""
This model takes into account features computed as part of the demo.derivations_v1 join
and uses it as an input to predict click_through_rate.
"""

label_source = EventSource(table=ctr_labels.ctr_labels.table, query = Query(
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
    inference_spec=InferenceSpec(
        model_backend=ModelBackend.VERTEXAI,
        model_backend_params={
            "model_name": "test_ctr_model",
            "model_type": "custom",
        }
    ),
    input_mapping={
        "instance": "named_struct('user_id_click_event_average_7d', user_id_click_event_average_7d, 'listing_price_cents', listing_id_price_cents, "
        "'price_log', price_log, 'price_bucket', price_bucket)",
    },
    output_mapping={
        "ctr": "gcp_click_through_rate_ctr_model__1_0__score"
    },
    # captures the schema of the model output
    value_fields=[
        ("score", DataType.DOUBLE),
    ],
    model_artifact_base_uri="gs://zipline-warehouse-models",
    # Model build is expected to be in - gs://zipline-warehouse-models/builds/test_ctr_model-1.0.tar.gz
    training_conf=TrainingSpec(
        training_data_source=label_source,
        training_data_window=Window(length=1, time_unit=TimeUnit.DAYS),
        schedule="@daily",
        image="us-docker.pkg.dev/vertex-ai/training/xgboost-cpu.2-1:latest",
        # Py module coordinates are optional unless they differ from the default
        # Vertex expectation of trainer.train
        python_module="trainer.train",
        resource_config=ResourceConfig(
            min_replica_count=1,
            max_replica_count=1,
            machine_type="n1-standard-4"
        ),
        job_configs={
            "max-depth": "4",
            "eta": "0.1",
            "num-boost-round": "50"
        }
    ),
    deployment_conf=DeploymentSpec(
        # We use a custom built container to help tweak the model input / outputs compared to the
        # pre-built containers
        container_config=ServingContainerConfig(
            image="us-central1-docker.pkg.dev/canary-443022/canary-images/ctr-predictor:v1"
        ),
        endpoint_config=EndpointConfig(
            endpoint_name="test_ctr_model"
        ),
        resource_config=ResourceConfig(
            min_replica_count=1,
            max_replica_count=3,
            machine_type="n1-standard-4"
        ),
        rollout_strategy=RolloutStrategy(
            # More sophisticated deployment strategies (e.g. blue/green) and
            # gradual traffic ramps are possible as well
            rollout_type=DeploymentStrategyType.IMMEDIATE,
        )
    )
)
