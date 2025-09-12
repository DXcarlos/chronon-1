# Chronon Feature Fetching Service

Docker image that allows you to run the Chronon Feature Service.

## Launching the Service

To fetch the latest image, run:
```bash
docker pull ziplineai/chronon-fetcher:latest
```

To run the service, you can use the following command:
```bash
docker run \
 -p 9000:9000 \
 ziplineai/chronon-fetcher:latest
```

### Additional Configuration & Environment Variables

There's some additional environment variables that the Docker container can be started up with (using the `-e "VAR=VALUE"` syntax)

|Environment Variable |Description                    |Default Value          |
|---------------------|-------------------------------|-----------------------------|
|**Google Cloud Configuration**
|GCP_PROJECT_ID |GCloud Zipline BigTable project |``            |
|GOOGLE_CLOUD_PROJECT |GCloud Zipline BigTable project |``            |
|GCP_BIGTABLE_INSTANCE_ID |GCloud Zipline BigTable instance id|``|
|GOOGLE_APPLICATION_CREDENTIALS|Path to [GCloud application credentials json file](https://cloud.google.com/docs/authentication/application-default-credentials#GAC)|``
|GCP_BIGTABLE_INSTANCE_ID |GCloud Zipline BigTable instance id|``|
|**Profiler Configuration**
|ENABLE_GCLOUD_PROFILER|If set to 'true', enables profiling service using [cloud profiler](https://cloud.google.com/profiler/docs/about-profiler)|`false`
|**Service Metrics Configuration**
|CHRONON_METRICS_READER | Either of 'http' or 'prometheus'.            |``
|EXPORTER_OTLP_ENDPOINT|If using the 'http' metrics reader, exports metrics to the configured endpoint using [OTLP over http](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter)|`http://localhost:4318`
|CHRONON_PROMETHEUS_SERVER_PORT|If using the 'prometheus' metrics reader, exposes a [Prometheus service endpoint](https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/prometheus/) on the configured port to export Chronon library metrics|`8905`
|VERTX_PROMETHEUS_SERVER_PORT|If using the 'prometheus' metrics reader, exposes a [Prometheus service endpoint](https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/prometheus/) on the configured port to export Vert.x webservice metrics|`8906`
|**Monitoring**
|FETCHER_OOC_TOPIC_INFO|Topic string in the format of: `kafka://my-topic-name/key1=value1/key2=value2`. If configured, the service emits [online offline consistency feature logging](https://chronon.ai/test_deploy_serve/Online_Offline_Consistency.html) data to the given topic|``
|**JVM Configuration**
|JVM_OPTS|Additional JVM options to pass to the service|``

## Exporting Metrics

The Chronon library and [Vert.x](https://vertx.io/) fetcher web service can be collect metrics using the [OpenTelemetry](https://opentelemetry.io/) standard. 
Currently, the service supports two ways of exporting metrics:
1. **OTLP over HTTP**: Metrics can be exported (push) to an OTLP endpoint using the `EXPORTER_OTLP_ENDPOINT` environment variable.
2. **Prometheus**: Metrics can be exposed on a Prometheus endpoint (pull) using the `CHRONON_PROMETHEUS_SERVER_PORT` and `VERTX_PROMETHEUS_SERVER_PORT` environment variables.

## Service profiling
The service can be profiled using the [Google Cloud Profiler](https://cloud.google.com/profiler/docs/about-profiler) by setting the `ENABLE_GCLOUD_PROFILER` environment variable to `true`. 
The profiles are exported to the cloud profiler under the service name 'chronon-fetcher'. 

## Service Endpoints

The service exposes the following HTTP endpoints:

|Verb                 |Endpoint                       |Description                  |
|---------------------|-------------------------------|-----------------------------|
|GET |`/ping` | Health check endpoint            |
|GET |`/config` | Fetcher service configuration            |
|GET |`/v1/joins` | List of Chronon joins marked online            |
|GET|`/v1/join/:name/schema` | Returns a join schema payload (consists of join name, entity key schema as an Avro string, value schema as an Avro string, schema hash
|POST|`/v1/fetch/groupby/:name`| Retrieve features for a specific GroupBy
|POST|`/v1/fetch/join/:name`| Retrieve features for a specific Join

### Example Usage

Some example curl calls to the service are shown below. The fetch join and GroupBy endpoints support bulk lookups, so you can pass a list of entity keys to fetch features for multiple entities in a single request.
```bash
$ curl http://localhost:9000/v1/joins                                                            
{"joinNames":["search.ranking_listing","search.recommendation_v1"]}

$ curl http://localhost:9000/v1/join/search.ranking_listing/schema | jq
{
  "joinName": "search.ranking_listing",
  "keySchema": "...",
  "valueSchema": "...",
  "schemaHash": "c72b6d",
  ...
}

$ curl -X POST http://localhost:9000/v1/fetch/join/ranking_listing -H 'Content-Type: application/json' -d '[{"listing_id":"12345"}]' | jq
{
  "results": [
    {
      "status": "Success",
      "entityKeys": {
        "listing_id": "12345"
      },
      "features": {
        "my_feature_1": 1,
        "my_feature_2": 0.5
        ...
      }
    }
  ]
}
```

## Docker Compose Setups

Three Docker Compose configurations are available for different telemetry scenarios:

### Local Telemetry Setup

For local development with full observability stack:

* **OTEL Collector**: Receives and transforms Chronon metrics
* **Prometheus**: Time-series database for metrics storage and querying

#### Setup Steps

1. **Configure environment**:
   ```shell
   export GCP_PROJECT_ID=your-project-id
   export GCP_BIGTABLE_INSTANCE_ID=your-instance-id
   ```

2. **Start services**:
   ```shell
   docker-compose -f chronon-service-with-local-telemetry.yml up -d
   ```

#### Viewing Local Metrics

After making API calls per [Example Usage](#example-usage):

```bash
# View raw metrics from OTEL Collector
curl http://localhost:9464/metrics

# List available metrics in Prometheus
curl -s "http://localhost:9091/api/v1/label/__name__/values" | jq '.data[]'

# Query metrics in Prometheus UI
open http://localhost:9091
```

### Google Managed Prometheus Setup (Recommended)

For production deployments using Google Cloud Managed Service for Prometheus:

* **OTEL Collector**: Receives metrics and exports to Google Managed Prometheus
* **Google Managed Prometheus**: Fully managed Prometheus-compatible service
* **Local Prometheus**: Optional local access for debugging

#### Prerequisites

1. **GCP Setup**:
   - Enable Cloud Monitoring API in your GCP project
   - Enable Managed Service for Prometheus
   - Authenticate with `gcloud auth application-default login`

2. **Required Environment Variables**:
   ```shell
   export GCP_PROJECT_ID=your-project-id
   export GCP_BIGTABLE_INSTANCE_ID=your-instance-id
   # For local/non-GKE deployments, configure metadata manually:
   export GCP_CLUSTER_NAME=chronon-cluster
   export GCP_NAMESPACE=default  
   export GCP_LOCATION=us-central1
   ```

> **Note**: When deployed in a GKE cluster, the OTEL collector automatically detects cluster metadata (cluster name, location, namespace) using the GCP resource detector. Environment variables are only needed for local Docker deployments.

#### Setup Steps

1. **Start services**:
   ```shell
   # For local deployments, set environment variables:
   GCP_LOCATION=us-central1 GCP_NAMESPACE=default GCP_CLUSTER_NAME=chronon-local \
   docker-compose -f chronon-service-with-managed-prometheus.yml up -d
   ```

#### Viewing Metrics

Metrics are available in multiple locations:

**Local Prometheus Access (for debugging):**
```bash
# Prometheus UI 
open http://localhost:9091

# Raw metrics from OTEL Collector
curl http://localhost:9464/metrics
```

**Google Managed Prometheus:**

Metrics are stored in Managed Service for Prometheus with resource labels from the OTEL processor:

1. **Google Cloud Console**:
   - Navigate to Monitoring > Metrics Explorer
   - Use PromQL queries to filter Chronon metrics
   - Metrics are available without prefixes and include resource labels like `cluster`, `namespace`, `location`
   - If metrics are failing to publish check the [exporter troubleshooting guide](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/googlemanagedprometheusexporter#troubleshooting)

2. **PromQL Queries**:
   ```promql
   # Example queries for Chronon metrics (note: no chronon-fetcher prefix)
   # For local Docker deployments:
   {cluster="chronon-cluster", namespace="default", location="us-central1"}
   
   # For GKE deployments (auto-detected metadata):
   {cluster="my-gke-cluster", namespace="chronon-prod", location="us-west1"}
   
   # Filter by specific metric patterns
   {__name__=~"chronon.*"}
   
   # Query specific service metrics
   {service_name="chronon-fetcher"}
   ```

> **Configuration Reference:** The setup uses the `googlemanagedprometheus` exporter with a `resourcedetection` processor (auto-detects GKE metadata) and `resource` processor (fallback for local deployments). When deployed in GKE, cluster metadata is automatically discovered. For advanced configuration, see the [Google Cloud Managed Prometheus documentation](https://cloud.google.com/stackdriver/docs/managed-prometheus) and [OpenTelemetry Google Managed Prometheus Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/googlemanagedprometheusexporter).