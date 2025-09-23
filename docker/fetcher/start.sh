#!/bin/bash
set -e

# Required environment variables
required_vars=("FETCHER_JAR" "FETCHER_PORT")
for var in "${required_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set"
    exit 1
  fi
done

if [[ $USE_AWS == true ]]; then
  ONLINE_JAR=$CLOUD_AWS_JAR
  ONLINE_CLASS=$AWS_ONLINE_CLASS
else
  ONLINE_JAR=$CLOUD_GCP_JAR
  ONLINE_CLASS=$GCP_ONLINE_CLASS
fi

METRICS_OPTS=""

if [ -n "$CHRONON_METRICS_READER" ]; then
  METRICS_OPTS="-Dai.chronon.metrics.enabled=true"
  METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.reader=$CHRONON_METRICS_READER"

  if [ "$CHRONON_METRICS_READER" = "http" ]; then
    if [ -n "$EXPORTER_OTLP_ENDPOINT" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.exporter.url=$EXPORTER_OTLP_ENDPOINT"
    fi
  elif [ "$CHRONON_METRICS_READER" = "prometheus" ]; then
    if [ -n "$CHRONON_PROMETHEUS_SERVER_PORT" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.metrics.exporter.port=$CHRONON_PROMETHEUS_SERVER_PORT"
    fi
    if [ -n "$VERTX_PROMETHEUS_SERVER_PORT" ]; then
      METRICS_OPTS="$METRICS_OPTS -Dai.chronon.vertx.metrics.exporter.port=$VERTX_PROMETHEUS_SERVER_PORT"
    fi
  fi
else
  echo "No CHRONON_METRICS_READER configured. Disabling metrics reporting"
  METRICS_OPTS="-Dai.chronon.metrics.enabled=false"
fi

# Configure Google Cloud Profiler if enabled
if [ "$ENABLE_GCLOUD_PROFILER" = true ]; then
  JVM_OPTS="$JVM_OPTS -agentpath:/opt/cprof/profiler_java_agent.so=-cprof_service=chronon-fetcher,-logtostderr,-minloglevel=1,-cprof_enable_heap_sampling"
fi

ADD_OPENS_OPTS="
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
"

JVM_OPTS="$JVM_OPTS $ADD_OPENS_OPTS -XX:MaxMetaspaceSize=1g -XX:MaxRAMPercentage=70.0 -XX:MinRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0 -XX:MaxHeapFreeRatio=100 -XX:MinHeapFreeRatio=0"

echo "Starting Fetcher service with online jar $ONLINE_JAR and online class $ONLINE_CLASS"
if ! java $JVM_OPTS -cp $FETCHER_JAR:$ONLINE_JAR ai.chronon.service.ChrononServiceLauncher \
  run ai.chronon.service.FetcherVerticle \
  -Dserver.port=$FETCHER_PORT \
  -Donline.jar=$ONLINE_JAR \
  $METRICS_OPTS \
  -Donline.class=$ONLINE_CLASS; then
  echo "Error: Fetcher service failed to start"
  exit 1
fi
