# zipline admin k8s — Context

## Commands

```
zipline admin init k8s
zipline admin generate [--config PATH] [--output-dir PATH] [--overwrite] [--check-tools]
```

## User flow

```bash
zipline admin init k8s          # writes zipline-stack.yaml + zipline/ scaffold
# edit zipline-stack.yaml
zipline admin generate           # writes k8s-local/

# Deploy (any cluster)
kubectl apply -k k8s-local/kustomize/overlays/quickstart   # remote registry
kubectl apply -k k8s-local/kustomize/overlays/local-dev    # local images

# If using kind
kind create cluster --name chronon --config k8s-local/kind/cluster-config.yaml
kind load docker-image chronon-spark-k8s:latest chronon-spark-base:latest chronon-frontend:latest chronon-fetcher-k8s:latest --name chronon
kubectl apply -k k8s-local/kustomize/overlays/local-dev

# Chronon configs
zipline compile
pip install zipline-seedkit && zipline-seedkit   # seed test data
```

## Key files

| File | Purpose |
|---|---|
| `python/src/ai/chronon/local_config.py` | `StackConfig` dataclasses + `load_config()` |
| `python/src/ai/chronon/repo/local_env.py` | `generate` command + all rendering logic |
| `python/src/ai/chronon/repo/init.py` | `init k8s` — writes stack YAML + scaffold |
| `python/src/ai/chronon/repo/admin.py` | registers `generate` and `init` under `admin` |
| `python/src/ai/chronon/resources/local/` | templates + static YAMLs + scaffold |
| `python/requirements/base.in` | PyYAML, Jinja2 added |
| `python/package.mill` | `[tool.setuptools.package-data]` added to include non-py resources |

## `zipline-stack.yaml` schema

```yaml
version: "1"
cluster:
  name: chronon       # kind cluster name
  type: kind
online_store:
  type: redis         # redis | local_kv
  host: redis
  port: 6379
observability:
  type: local         # none | local | gcp | aws | azure
  # gcp: gcp_project, gcp_credentials_secret
  # aws: aws_region, aws_credentials_secret
  # azure: azure_connection_string_secret
images:
  registry: local     # local | ghcr.io/zipline-ai | <custom>
  tag: latest
  pull_policy: IfNotPresent
```

## Resources layout

```
resources/local/
├── zipline-stack.yaml
├── scaffold/                          # copied to ./zipline/ by init k8s
│   ├── teams.py                       # quickstart team; default = quickstart
│   ├── group_bys/quickstart/{user_activities,dim_listings,dim_merchants}.py
│   ├── joins/quickstart/demo.py
│   └── staging_queries/quickstart/exports.py
├── kind/
│   └── cluster-config.yaml.j2        # var: cluster_name
└── kustomize/
    ├── base/
    │   ├── kustomization.yaml.j2      # vars: online_store, otel_file
    │   ├── hub.yaml.j2                # vars: online_class, spark_image, redis_host, pull_policy
    │   ├── fetcher.yaml.j2            # vars: online_class, redis_host, pull_policy, metrics_enabled
    │   ├── otel-local.yaml            # static
    │   ├── otel-{gcp,aws,azure}.yaml.j2
    │   └── {namespace,spark-rbac,postgres,minio,starrocks,loki,promtail,
    │         spark-history-server,redis,eval,frontend}.yaml
    └── overlays/
        ├── local-dev/kustomization.yaml.j2    # no registry prefix
        └── quickstart/kustomization.yaml.j2   # image_prefix from registry
```

## Key mappings

```python
# CHRONON_ONLINE_CLASS
"redis"    -> "ai.chronon.integrations.cloud_k8s.LocalRedisApiImpl"
"local_kv" -> "ai.chronon.integrations.cloud_k8s.LocalTestApiImpl"

# OTel
"none"  -> None             # metrics disabled in fetcher (-Dai.chronon.metrics.enabled=false)
"local" -> "otel-local.yaml"
"gcp"   -> "otel-gcp.yaml"
"aws"   -> "otel-aws.yaml"
"azure" -> "otel-azure.yaml"

# Images (hub and eval both use chronon-spark-k8s — no separate hub image)
# kind load: chronon-spark-k8s, chronon-spark-base, chronon-frontend, chronon-fetcher-k8s
```

## Service ports

| Service             | Internal port | NodePort |
|---------------------|---------------|----------|
| hub                 | 3903          | 30903    |
| eval                | 3904          | 30904    |
| fetcher             | 9200          | 31999    |
| frontend            | 3000          | 30300    |
| spark-history-server| 18080         | 30180    |
| postgres            | 5432          | 30432    |
| minio (API)         | 9000          | 30900    |
| minio (console)     | 9001          | 30901    |
| redis               | 6379          | 30637    |
| loki                | 3100          | 30310    |
| prometheus          | 9090          | 30909    |
| starrocks (MySQL)   | 9030          | 30930    |
| otel grpc           | 4317          | 30417    |
| otel http           | 4318          | 30418    |

## Extending

- **New online_store type**: add to `_VALID_ONLINE_STORE_TYPES` + `_ONLINE_CLASS_MAP`
- **New observability type**: add to `_VALID_OBSERVABILITY_TYPES` + `_OTEL_FILE_MAP` + create `otel-<type>.yaml.j2`
- **New cluster type** (eks/gke): add rendering fn in `local_env.py`, dispatch in `_render_configs`

## Known gaps

- `cluster.type` stored but only `kind` implemented
- `requirements/base.txt` lock file not regenerated (run `requirements upgrade`)
- Applying over an existing deployment with old `commonLabels` in selectors requires deleting and recreating Deployments/DaemonSets (immutable `spec.selector`). This is fixed in the new overlay templates which use `labels` with `includeSelectors: false` instead.
- minio-setup Job only runs once (idempotent on first deploy). If minio PVC is reset, buckets and the `event-logs` prefix in `chronon-spark-logs` must be recreated manually (spark-history-server needs the prefix to exist on startup).
