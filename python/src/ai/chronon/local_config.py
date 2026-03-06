"""Dataclasses and loader for zipline-stack.yaml."""
from dataclasses import dataclass, field
from typing import Optional

import yaml

_VALID_ONLINE_STORE_TYPES = {"redis", "local_kv"}
_VALID_OBSERVABILITY_TYPES = {"none", "local", "gcp", "aws", "azure"}
_VALID_PULL_POLICIES = {"IfNotPresent", "Always", "Never"}


@dataclass
class ClusterConfig:
    name: str = "chronon"
    type: str = "kind"


@dataclass
class OnlineStoreConfig:
    type: str = "redis"
    host: str = "redis"
    port: int = 6379


@dataclass
class ObservabilityConfig:
    type: str = "local"
    gcp_project: Optional[str] = None
    gcp_credentials_secret: Optional[str] = None
    aws_region: Optional[str] = None
    aws_credentials_secret: Optional[str] = None
    azure_connection_string_secret: Optional[str] = None


@dataclass
class ImagesConfig:
    registry: str = "local"
    tag: str = "latest"
    pull_policy: str = "IfNotPresent"


@dataclass
class StackConfig:
    version: str = "1"
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    online_store: OnlineStoreConfig = field(default_factory=OnlineStoreConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    images: ImagesConfig = field(default_factory=ImagesConfig)


def load_config(path: str) -> StackConfig:
    with open(path) as f:
        data = yaml.safe_load(f) or {}

    cluster_data = data.get("cluster", {})
    online_store_data = data.get("online_store", {})
    observability_data = data.get("observability", {})
    images_data = data.get("images", {})

    online_store_type = online_store_data.get("type", "redis")
    if online_store_type not in _VALID_ONLINE_STORE_TYPES:
        raise ValueError(
            f"online_store.type must be one of {sorted(_VALID_ONLINE_STORE_TYPES)}, got {online_store_type!r}"
        )

    obs_type = observability_data.get("type", "local")
    if obs_type not in _VALID_OBSERVABILITY_TYPES:
        raise ValueError(
            f"observability.type must be one of {sorted(_VALID_OBSERVABILITY_TYPES)}, got {obs_type!r}"
        )

    pull_policy = images_data.get("pull_policy", "IfNotPresent")
    if pull_policy not in _VALID_PULL_POLICIES:
        raise ValueError(
            f"images.pull_policy must be one of {sorted(_VALID_PULL_POLICIES)}, got {pull_policy!r}"
        )

    return StackConfig(
        version=str(data.get("version", "1")),
        cluster=ClusterConfig(
            name=cluster_data.get("name", "chronon"),
            type=cluster_data.get("type", "kind"),
        ),
        online_store=OnlineStoreConfig(
            type=online_store_type,
            host=online_store_data.get("host", "redis"),
            port=int(online_store_data.get("port", 6379)),
        ),
        observability=ObservabilityConfig(
            type=obs_type,
            gcp_project=observability_data.get("gcp_project"),
            gcp_credentials_secret=observability_data.get("gcp_credentials_secret"),
            aws_region=observability_data.get("aws_region"),
            aws_credentials_secret=observability_data.get("aws_credentials_secret"),
            azure_connection_string_secret=observability_data.get("azure_connection_string_secret"),
        ),
        images=ImagesConfig(
            registry=images_data.get("registry", "local"),
            tag=images_data.get("tag", "latest"),
            pull_policy=pull_policy,
        ),
    )
