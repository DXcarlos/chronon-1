# zipline admin generate — Development Context

This document provides context for continuing development of the `zipline admin generate` command, which produces a kustomize + kind directory tree from a user-authored `zipline-stack.yaml`.

---

## What Was Built

### Commands

```
zipline admin init k8s
  → Writes zipline-stack.yaml to the current directory (annotated example).

zipline admin generate [OPTIONS]

Options:
  --config PATH        Path to zipline-stack.yaml  [default: ./zipline-stack.yaml]
  --output-dir PATH    Directory to write generated configs  [default: ./k8s-local]
  --overwrite          Overwrite existing output directory
  --check-tools        Only check kind/docker/kubectl installation, then exit
```

### Files Created / Modified

| File | Status | Purpose |
|---|---|---|
| `python/src/ai/chronon/local_config.py` | **new** | Dataclasses + `load_config()` for zipline-stack.yaml |
| `python/src/ai/chronon/repo/local_env.py` | **new** | `generate` Click command; rendering logic |
| `python/src/ai/chronon/resources/local/` | **new** | Jinja2 templates + static YAMLs bundled as package data |
| `python/src/ai/chronon/repo/admin.py` | modified | Imports and registers `generate` command |
| `python/src/ai/chronon/repo/init.py` | modified | Added `k8s` choice + `_init_k8s()` helper |
| `python/requirements/base.in` | modified | Added `PyYAML`, `Jinja2` |

---

## Quickstart TL;DR

```bash
# 1. Create config
zipline admin init k8s

# 2. Edit zipline-stack.yaml
#    Set registry: ghcr.io/zipline-ai for quickstart, or leave local for local-dev

# 3. Generate k8s configs
zipline admin generate

# 4. Create the kind cluster
kind create cluster --name chronon --config k8s-local/kind/cluster-config.yaml

# 5a. Quickstart (remote images)
kubectl apply -k k8s-local/kustomize/overlays/quickstart

# 5b. Local-dev (locally built images)
kind load docker-image chronon-spark-k8s:latest --name chronon
kind load docker-image chronon-spark-base:latest --name chronon
kind load docker-image chronon-frontend:latest   --name chronon
kind load docker-image chronon-fetcher-k8s:latest --name chronon
kind load docker-image chronon-hub-k8s:latest   --name chronon
kubectl apply -k k8s-local/kustomize/overlays/local-dev
```

---

## Config Schema (`zipline-stack.yaml`)

```yaml
version: "1"

cluster:
  name: chronon          # kind cluster name
  type: kind

online_store:
  type: redis            # redis | local_kv
  host: redis
  port: 6379

observability:
  type: local            # none | local | gcp | aws | azure
  # gcp: gcp_project, gcp_credentials_secret
  # aws: aws_region, aws_credentials_secret
  # azure: azure_connection_string_secret

images:
  registry: local        # local | ghcr.io/zipline-ai | <custom>
  tag: latest
  pull_policy: IfNotPresent
```

### Validation (`local_config.py`)

- `online_store.type`: `{"redis", "local_kv"}`
- `observability.type`: `{"none", "local", "gcp", "aws", "azure"}`
- `images.pull_policy`: `{"IfNotPresent", "Always", "Never"}`

---

## Architecture

### Template Rendering (`local_env.py`)

`_render_configs(config, output_dir)` orchestrates these steps:

1. **`_render_kind_config`** — `kind/cluster-config.yaml.j2` → `kind/cluster-config.yaml`
2. **`_copy_static_base_files`** — copies static YAMLs to `kustomize/base/`
3. **`_render_base_kustomization`** — `kustomization.yaml.j2` with conditional redis + otel entries
4. **`_render_hub`** — injects CHRONON_ONLINE_CLASS, SPARK_IMAGE, pull_policy
5. **`_render_fetcher`** — injects CHRONON_ONLINE_CLASS, pull_policy, metrics flags
6. **`_render_otel`** — skipped entirely when `observability.type == "none"`, static copy for `local`, Jinja2 for cloud variants
7. **`_render_overlays`** — `overlays/{local-dev,quickstart}/kustomization.yaml.j2` with image registry/tag

### Key Mappings

**CHRONON_ONLINE_CLASS:**
```python
"redis"    -> "ai.chronon.integrations.cloud_k8s.LocalRedisApiImpl"
"local_kv" -> "ai.chronon.integrations.cloud_k8s.LocalTestApiImpl"
```

**OTel file:**
```python
"none"  -> None              # no file generated, metrics disabled in fetcher
"local" -> "otel-local.yaml" # static copy, in-cluster otel + prometheus
"gcp"   -> "otel-gcp.yaml"   # rendered from otel-gcp.yaml.j2
"aws"   -> "otel-aws.yaml"   # rendered from otel-aws.yaml.j2
"azure" -> "otel-azure.yaml" # rendered from otel-azure.yaml.j2
```

**Image prefix:**
```python
registry == "local"            -> ""
registry == "ghcr.io/zipline-ai" -> "ghcr.io/zipline-ai/"
registry == "<custom>"          -> "<custom>/"
```

### `observability: none` behaviour

- `_render_otel` returns early, no otel file written
- `kustomization.yaml.j2` uses `{% if otel_file %}` so no otel line in resources list
- `fetcher.yaml.j2` uses `{% if metrics_enabled %}` — when false, sets `-Dai.chronon.metrics.enabled=false` and omits reader/exporter flags

---

## Resources Directory Layout

```
resources/local/
├── zipline-stack.yaml                    # annotated example config (written by init k8s)
├── kind/
│   └── cluster-config.yaml.j2           # vars: cluster_name
└── kustomize/
    ├── base/
    │   ├── kustomization.yaml.j2         # vars: online_store, otel_file
    │   ├── hub.yaml.j2                   # vars: online_class, spark_image, redis_host, pull_policy
    │   ├── fetcher.yaml.j2               # vars: online_class, redis_host, pull_policy, metrics_enabled
    │   ├── otel-local.yaml               # static: in-cluster otel-collector + prometheus
    │   ├── otel-gcp.yaml.j2              # vars: observability (project, credentials_secret)
    │   ├── otel-aws.yaml.j2              # vars: observability (region, credentials_secret)
    │   ├── otel-azure.yaml.j2            # vars: observability (connection_string_secret)
    │   ├── namespace.yaml
    │   ├── spark-rbac.yaml
    │   ├── postgres.yaml
    │   ├── minio.yaml
    │   ├── starrocks.yaml
    │   ├── loki.yaml
    │   ├── promtail.yaml
    │   ├── spark-history-server.yaml
    │   ├── redis.yaml
    │   ├── eval.yaml
    │   ├── frontend.yaml
    │   └── celeborn.yaml
    └── overlays/
        ├── local-dev/
        │   └── kustomization.yaml.j2     # vars: config (images.tag) — no registry prefix
        └── quickstart/
            └── kustomization.yaml.j2     # vars: config (images.tag), image_prefix
```

---

## Extending the Feature

### Adding a new `online_store.type` (e.g. `bigtable`)

1. Add `"bigtable"` to `_VALID_ONLINE_STORE_TYPES` in `local_config.py`
2. Add mapping in `_ONLINE_CLASS_MAP` in `local_env.py`
3. Optionally add Bigtable-specific config fields to `OnlineStoreConfig`

### Adding a new `observability.type` (e.g. `datadog`)

1. Add `"datadog"` to `_VALID_OBSERVABILITY_TYPES` in `local_config.py`
2. Add `"datadog": "otel-datadog.yaml"` to `_OTEL_FILE_MAP` in `local_env.py`
3. Create `resources/local/kustomize/base/otel-datadog.yaml.j2`
4. Add Datadog-specific fields to `ObservabilityConfig` if needed

### Adding a new `cluster.type` (e.g. `eks`)

Currently only `kind` generates configs. To add `eks`:
1. Add `"eks"` handling in a new `_render_eks_config` function in `local_env.py`
2. Dispatch based on `config.cluster.type` in `_render_configs`
3. Add EKS-specific templates under `resources/local/eks/`

---

## Known Limitations / Future Work

- `cluster.type` field is stored but only `kind` is implemented
- Static base YAMLs are authored in-place in `resources/local/kustomize/base/` — update them if the k8s manifests need to change
- `requirements/base.txt` (pinned lock file) was NOT updated — run `requirements upgrade` to regenerate it after adding PyYAML and Jinja2
- The overlays (`local-dev` vs `quickstart`) differ in `commonLabels` and image registry prefix — `local-dev` uses no prefix (kind-loaded images), `quickstart` uses the configured registry
