from collections.abc import Sequence
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import gen_thrift.api.ttypes as ttypes
import gen_thrift.common.ttypes as common
from ai.chronon import utils
from ai.chronon import windows as window_utils
from ai.chronon.data_types import DataType, FieldsType
from ai.chronon.utils import ANY_SOURCE_TYPE, normalize_source, normalize_sources


def _as_sources(features):
    if features is None:
        return None
    if isinstance(features, Sequence) and not isinstance(features, (str, bytes)):
        return features
    return [features]


class ModelBackend:
    VERTEXAI = ttypes.ModelBackend.VertexAI
    SAGEMAKER = ttypes.ModelBackend.SageMaker


class DeploymentStrategyType:
    # deploys the model in a blue-green fashion (~2x capacity) to another endpoint and gradually ramps traffic
    BLUE_GREEN = ttypes.DeploymentStrategyType.BLUE_GREEN

    # deploys the model in a rolling manner by gradually scaling down existing instances and scaling up new instances
    ROLLING = ttypes.DeploymentStrategyType.ROLLING

    # deploys the model immediately to the endpoint without any traffic ramping
    IMMEDIATE = ttypes.DeploymentStrategyType.IMMEDIATE


@dataclass
class Resources:
    min_replica_count: Optional[int] = None
    max_replica_count: Optional[int] = None
    machine_type: Optional[str] = None

    def to_thrift(self):
        return ttypes.Resources(
            minReplicaCount=self.min_replica_count,
            maxReplicaCount=self.max_replica_count,
            machineType=self.machine_type,
        )


@dataclass
class ModelRuntime:
    backend: Optional[ModelBackend] = None
    params: Optional[Dict[str, str]] = None
    resources: Optional[Resources] = None

    def to_thrift(self):
        return ttypes.ModelRuntime(
            backend=self.backend,
            params=self.params,
            resources=self.resources.to_thrift() if self.resources else None,
        )


@dataclass
class Train:
    # TODO: may want to try to support staging query as a training data source.
    data: Optional[ANY_SOURCE_TYPE] = None
    window: Optional[Union[common.Window, str]] = None
    schedule: Optional[str] = None
    image: Optional[str] = None
    entrypoint: Optional[str] = None
    resources: Optional[Resources] = None
    params: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.Train(
            data=normalize_source(self.data) if self.data else None,
            window=window_utils.normalize_window(self.window) if self.window else None,
            schedule=self.schedule,
            image=self.image,
            entrypoint=self.entrypoint,
            resources=self.resources.to_thrift() if self.resources else None,
            params=self.params,
        )


@dataclass
class ServingContainerConfig:
    image: Optional[str] = None
    health_route: Optional[str] = None
    infer_route: Optional[str] = None
    env: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.ServingContainerConfig(
            image=self.image,
            healthRoute=self.health_route,
            inferRoute=self.infer_route,
            env=self.env,
        )


@dataclass
class EndpointConfig:
    endpoint_name: Optional[str] = None
    additional_configs: Optional[Dict[str, str]] = None

    def to_thrift(self):
        return ttypes.EndpointConfig(
            endpointName=self.endpoint_name,
            additionalConfigs=self.additional_configs,
        )


def _endpoint_config(endpoint):
    if endpoint is None:
        return None
    if isinstance(endpoint, EndpointConfig):
        return endpoint
    return EndpointConfig(endpoint_name=endpoint)


@dataclass
class Metric:
    name: Optional[str] = None
    threshold: Optional[float] = None

    def to_thrift(self):
        return ttypes.Metric(
            name=self.name,
            threshold=self.threshold,
        )


@dataclass
class RolloutStrategy:
    rollout_type: Optional[DeploymentStrategyType] = None
    validation_traffic_percent_ramps: Optional[List[int]] = None
    validation_traffic_duration_mins: Optional[List[int]] = None
    rollout_metric_thresholds: Optional[List[Metric]] = None

    def to_thrift(self):
        return ttypes.RolloutStrategy(
            rolloutType=self.rollout_type,
            validationTrafficPercentRamps=self.validation_traffic_percent_ramps,
            validationTrafficDurationMins=self.validation_traffic_duration_mins,
            rolloutMetricThresholds=[m.to_thrift() for m in self.rollout_metric_thresholds]
            if self.rollout_metric_thresholds
            else None,
        )


@dataclass
class Serve:
    container: Optional[ServingContainerConfig] = None
    endpoint: Optional[Union[str, EndpointConfig]] = None
    resources: Optional[Resources] = None
    rollout: Optional[RolloutStrategy] = None
    image: Optional[str] = None
    health_route: Optional[str] = None
    infer_route: Optional[str] = None
    env: Optional[Dict[str, str]] = None

    def to_thrift(self):
        container = self.container
        if container is None and any(
            value is not None for value in (self.image, self.health_route, self.infer_route, self.env)
        ):
            container = ServingContainerConfig(
                image=self.image,
                health_route=self.health_route,
                infer_route=self.infer_route,
                env=self.env,
            )

        return ttypes.Serve(
            container=container.to_thrift() if container else None,
            endpoint=_endpoint_config(self.endpoint).to_thrift() if self.endpoint else None,
            resources=self.resources.to_thrift() if self.resources else None,
            rollout=self.rollout.to_thrift() if self.rollout else None,
        )


def Model(
    version: str,
    runtime: Optional[ModelRuntime] = None,
    inputs: Optional[Dict[str, str]] = None,
    outputs: Optional[Dict[str, str]] = None,
    output_fields: Optional[FieldsType] = None,
    artifact_uri: Optional[str] = None,
    train: Optional[Train] = None,
    serve: Optional[Serve] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> ttypes.Model:
    """
    Creates a Model object for ML model training, deployment, and inference.

    :param version:
        Version string for the model configuration.
    :param runtime:
        Backend and runtime details needed to call the model.
    :param inputs:
        Spark SQL expressions that build model inputs from feature columns.
    :param outputs:
        Spark SQL expressions that map model outputs to Chronon columns.
    :param output_fields:
        List of tuples of (field_name, DataType) defining the model output schema.
    :param artifact_uri:
        Base URI where trained model artifacts are stored.
    :param train:
        Training job configuration.
    :param serve:
        Serving deployment configuration.
    """
    team = utils._get_team_from_caller()

    assert isinstance(version, str), f"Version must be a string, but found {type(version).__name__}"

    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=version,
    )

    return ttypes.Model(
        metaData=meta_data,
        runtime=runtime.to_thrift() if runtime else None,
        inputs=inputs,
        outputs=outputs,
        outputSchema=DataType.STRUCT("model_output_schema", *output_fields) if output_fields else None,
        artifactUri=artifact_uri,
        train=train.to_thrift() if train else None,
        serve=serve.to_thrift() if serve else None,
    )


def _get_inference_output_table_name(inference: ttypes.Inference, full_name: bool = False):
    """Generate output table name for Inference."""
    return utils._ensure_name_and_get_output_table(
        inference, ttypes.Inference, "inferences", full_name
    )


def Inference(
    features: Union[ANY_SOURCE_TYPE, Sequence[ANY_SOURCE_TYPE]],
    models: List[ttypes.Model],
    version: int,
    passthrough: Optional[List[str]] = None,
    key_fields: Optional[FieldsType] = None,
    output_namespace: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> ttypes.Inference:
    """
    Creates an inference config from feature sources and one or more models.
    """
    team = utils._get_team_from_caller()

    if models:
        for model in models:
            if not model.metaData.name:
                utils.__set_name(model, ttypes.Model, "models")

    meta_data = ttypes.MetaData(
        outputNamespace=output_namespace,
        team=team,
        tags=tags,
        tableProperties=table_properties,
        version=str(version),
    )

    inference = ttypes.Inference(
        features=normalize_sources(_as_sources(features)),
        models=models,
        passthrough=passthrough,
        metaData=meta_data,
        keySchema=DataType.STRUCT("inference_key_schema", *key_fields) if key_fields else None,
    )

    inference.__class__.table = property(
        lambda self: _get_inference_output_table_name(self, full_name=True)
    )

    return inference


Infer = Inference
