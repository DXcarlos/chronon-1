#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  emr_spark_connect_bootstrap.sh s3://bucket/path/chronon-cloud-aws.jar [options]

Required:
  s3://... chronon jar location, or CHRONON_JAR_S3_URI
  Default: s3://zipline-artifacts-canary/release/latest/jars/cloud_aws_lib_deploy.jar

Options:
  --port PORT                 Spark Connect gRPC port. Default: 15002
  --master MASTER             Spark master URL. Default: local[2]
  --extra-jars URI[,URI...]   Extra jars to copy from S3 or use from local paths.
  --warehouse S3_URI          Optional Iceberg Glue warehouse location.
  --region AWS_REGION         AWS region for S3 and Glue configs.
  --server-package COORD      Spark Connect server Maven coordinate.
                              Default: org.apache.spark:spark-connect_2.12:3.5.6
  --service-name NAME         systemd service name. Default: chronon-spark-connect
  --help                      Show this help.

Example EMR bootstrap action args:
  s3://my-bucket/jars/chronon-cloud-aws.jar --port 15002 --master local[2]

The script only starts Spark Connect on the EMR primary node. Worker nodes exit
successfully after bootstrap.
EOF
}

log() {
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" >&2
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo is required when the script is not run as root"
  SUDO=(sudo)
fi

is_primary_node() {
  local instance_info="/mnt/var/lib/info/instance.json"

  if [[ ! -f "${instance_info}" ]]; then
    log "EMR instance metadata not found at ${instance_info}; assuming this is the target node."
    return 0
  fi

  grep -Eq '"isMaster"[[:space:]]*:[[:space:]]*true' "${instance_info}"
}

csv_append() {
  local current="$1"
  local next="$2"

  if [[ -z "${current}" ]]; then
    printf '%s' "${next}"
  else
    printf '%s,%s' "${current}" "${next}"
  fi
}

copy_s3_uri() {
  local source_uri="$1"
  local destination_dir="$2"
  local file_name
  local destination_path
  local tmp_file

  [[ "${source_uri}" == s3://* ]] || die "Expected an S3 URI, got: ${source_uri}"
  command -v aws >/dev/null 2>&1 || die "aws CLI is required on the EMR node"

  file_name="$(basename "${source_uri}")"
  destination_path="${destination_dir}/${file_name}"
  tmp_file="/tmp/${file_name}.$$"

  "${SUDO[@]}" mkdir -p "${destination_dir}"
  log "Copying ${source_uri} to ${destination_path}"
  aws s3 cp "${source_uri}" "${tmp_file}" >&2
  "${SUDO[@]}" install -m 0644 "${tmp_file}" "${destination_path}"
  rm -f "${tmp_file}"
  printf '%s' "${destination_path}"
}

CHRONON_JAR_S3_URI="${CHRONON_JAR_S3_URI:-s3://zipline-artifacts-canary/release/latest/jars/cloud_aws_lib_deploy.jar}"
SPARK_CONNECT_PORT="${SPARK_CONNECT_PORT:-15002}"
SPARK_CONNECT_MASTER="${SPARK_CONNECT_MASTER:-local[2]}"
SPARK_CONNECT_EXTRA_JARS="${SPARK_CONNECT_EXTRA_JARS:-}"
SPARK_CONNECT_WAREHOUSE="${SPARK_CONNECT_WAREHOUSE:-}"
SPARK_CONNECT_SERVICE_NAME="${SPARK_CONNECT_SERVICE_NAME:-chronon-spark-connect}"
SPARK_CONNECT_DRIVER_MEMORY="${SPARK_CONNECT_DRIVER_MEMORY:-2g}"
SPARK_CONNECT_SERVER_PACKAGE="${SPARK_CONNECT_SERVER_PACKAGE:-org.apache.spark:spark-connect_2.12:3.5.6}"
SPARK_HOME="${SPARK_HOME:-/usr/lib/spark}"
AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      usage
      exit 0
      ;;
    --port)
      SPARK_CONNECT_PORT="${2:-}"
      shift 2
      ;;
    --master)
      SPARK_CONNECT_MASTER="${2:-}"
      shift 2
      ;;
    --extra-jars)
      SPARK_CONNECT_EXTRA_JARS="${2:-}"
      shift 2
      ;;
    --warehouse)
      SPARK_CONNECT_WAREHOUSE="${2:-}"
      shift 2
      ;;
    --region)
      AWS_REGION="${2:-}"
      shift 2
      ;;
    --server-package)
      SPARK_CONNECT_SERVER_PACKAGE="${2:-}"
      shift 2
      ;;
    --service-name)
      SPARK_CONNECT_SERVICE_NAME="${2:-}"
      shift 2
      ;;
    s3://*)
      CHRONON_JAR_S3_URI="$1"
      shift
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[[ -n "${CHRONON_JAR_S3_URI}" ]] || die "Chronon jar S3 URI is required"
[[ "${CHRONON_JAR_S3_URI}" == s3://* ]] || die "Chronon jar must be an S3 URI: ${CHRONON_JAR_S3_URI}"
[[ -n "${SPARK_CONNECT_PORT}" ]] || die "--port cannot be empty"
[[ -n "${SPARK_CONNECT_MASTER}" ]] || die "--master cannot be empty"
[[ -x "${SPARK_HOME}/bin/spark-submit" ]] || die "spark-submit not found at ${SPARK_HOME}/bin/spark-submit"

if ! is_primary_node; then
  log "This is not the EMR primary node; skipping Spark Connect service setup."
  exit 0
fi

INSTALL_DIR="/opt/chronon"
JARS_DIR="${INSTALL_DIR}/spark-connect-jars"
START_SCRIPT="${INSTALL_DIR}/start-spark-connect.sh"
ENV_FILE="/etc/default/${SPARK_CONNECT_SERVICE_NAME}"
SERVICE_FILE="/etc/systemd/system/${SPARK_CONNECT_SERVICE_NAME}.service"
SPARK_CONNECT_LOG_DIR="/var/log/${SPARK_CONNECT_SERVICE_NAME}"
SPARK_CONNECT_PID_DIR="/run/${SPARK_CONNECT_SERVICE_NAME}"
SPARK_CONNECT_ARTIFACT_DIR="${INSTALL_DIR}/artifacts"

"${SUDO[@]}" mkdir -p "${JARS_DIR}" "${SPARK_CONNECT_LOG_DIR}" "${SPARK_CONNECT_ARTIFACT_DIR}"

chronon_jar_path="$(copy_s3_uri "${CHRONON_JAR_S3_URI}" "${JARS_DIR}")"
jars_csv="${chronon_jar_path}"

if [[ -n "${SPARK_CONNECT_EXTRA_JARS}" ]]; then
  IFS=',' read -r -a extra_jars <<< "${SPARK_CONNECT_EXTRA_JARS}"
  for jar_uri in "${extra_jars[@]}"; do
    [[ -n "${jar_uri}" ]] || continue

    if [[ "${jar_uri}" == s3://* ]]; then
      jar_path="$(copy_s3_uri "${jar_uri}" "${JARS_DIR}")"
    else
      [[ -f "${jar_uri}" ]] || die "Extra jar does not exist: ${jar_uri}"
      jar_path="${jar_uri}"
    fi

    jars_csv="$(csv_append "${jars_csv}" "${jar_path}")"
  done
fi

tmp_env_file="/tmp/${SPARK_CONNECT_SERVICE_NAME}.env.$$"
cat > "${tmp_env_file}" <<EOF
SPARK_HOME=${SPARK_HOME}
SPARK_CONNECT_PORT=${SPARK_CONNECT_PORT}
SPARK_CONNECT_MASTER=${SPARK_CONNECT_MASTER}
SPARK_CONNECT_DRIVER_MEMORY=${SPARK_CONNECT_DRIVER_MEMORY}
SPARK_CONNECT_JARS=${jars_csv}
SPARK_CONNECT_WAREHOUSE=${SPARK_CONNECT_WAREHOUSE}
SPARK_CONNECT_SERVER_PACKAGE=${SPARK_CONNECT_SERVER_PACKAGE}
SPARK_LOG_DIR=${SPARK_CONNECT_LOG_DIR}
SPARK_PID_DIR=${SPARK_CONNECT_PID_DIR}
AWS_REGION=${AWS_REGION}
EOF
"${SUDO[@]}" install -m 0644 "${tmp_env_file}" "${ENV_FILE}"
rm -f "${tmp_env_file}"

tmp_start_script="/tmp/${SPARK_CONNECT_SERVICE_NAME}.start.$$"
cat > "${tmp_start_script}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

CONF_ARGS=(
  --conf "spark.connect.grpc.binding.address=0.0.0.0"
  --conf "spark.connect.grpc.binding.port=${SPARK_CONNECT_PORT}"
  --conf "spark.driver.bindAddress=0.0.0.0"
  --conf "spark.driver.host=$(hostname -f 2>/dev/null || hostname)"
  --conf "spark.driver.memory=${SPARK_CONNECT_DRIVER_MEMORY}"
  --conf "spark.sql.shuffle.partitions=4"
  --conf "spark.sql.adaptive.enabled=false"
  --conf "spark.ui.enabled=false"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.chronon.table_write.format=iceberg"
)

if [[ -n "${AWS_REGION:-}" ]]; then
  CONF_ARGS+=(
    --conf "spark.hadoop.aws.region=${AWS_REGION}"
    --conf "spark.hadoop.com.amazonaws.services.glue.catalog.region=${AWS_REGION}"
  )
fi

if [[ -n "${SPARK_CONNECT_WAREHOUSE:-}" ]]; then
  CONF_ARGS+=(
    --conf "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog"
    --conf "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
    --conf "spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    --conf "spark.sql.catalog.glue.warehouse=${SPARK_CONNECT_WAREHOUSE}"
    --conf "spark.sql.defaultCatalog=glue"
  )
fi

SUBMIT_ARGS=(
  --master "${SPARK_CONNECT_MASTER}"
  "${CONF_ARGS[@]}"
  --class org.apache.spark.sql.connect.service.SparkConnectServer
  --name chronon-spark-connect
)

if [[ -n "${SPARK_CONNECT_JARS:-}" ]]; then
  SUBMIT_ARGS+=(--jars "${SPARK_CONNECT_JARS}")
fi

if [[ -n "${SPARK_CONNECT_SERVER_PACKAGE:-}" ]]; then
  SUBMIT_ARGS+=(--packages "${SPARK_CONNECT_SERVER_PACKAGE}")
fi

exec "${SPARK_HOME}/bin/spark-submit" \
  "${SUBMIT_ARGS[@]}" \
  spark-internal \
  --wait
EOF
"${SUDO[@]}" install -m 0755 "${tmp_start_script}" "${START_SCRIPT}"
rm -f "${tmp_start_script}"

service_user_line=""
if id hadoop >/dev/null 2>&1; then
  "${SUDO[@]}" chown -R hadoop:hadoop "${INSTALL_DIR}" "${SPARK_CONNECT_LOG_DIR}" "${SPARK_CONNECT_ARTIFACT_DIR}"
  service_user_line="User=hadoop"
fi

tmp_service_file="/tmp/${SPARK_CONNECT_SERVICE_NAME}.service.$$"
cat > "${tmp_service_file}" <<EOF
[Unit]
Description=Chronon Spark Connect Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
${service_user_line}
RuntimeDirectory=${SPARK_CONNECT_SERVICE_NAME}
WorkingDirectory=${INSTALL_DIR}
ExecStart=${START_SCRIPT}
Restart=always
RestartSec=10
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF
"${SUDO[@]}" install -m 0644 "${tmp_service_file}" "${SERVICE_FILE}"
rm -f "${tmp_service_file}"

"${SUDO[@]}" systemctl daemon-reload
"${SUDO[@]}" systemctl enable "${SPARK_CONNECT_SERVICE_NAME}"
"${SUDO[@]}" systemctl restart "${SPARK_CONNECT_SERVICE_NAME}" || {
  log "Spark Connect service did not become healthy during bootstrap."
  log "Leaving the EMR cluster up for inspection. Check: sudo journalctl -u ${SPARK_CONNECT_SERVICE_NAME} -n 200 --no-pager"
}

log "Configured ${SPARK_CONNECT_SERVICE_NAME} on port ${SPARK_CONNECT_PORT}"
log "Check status with: sudo systemctl status ${SPARK_CONNECT_SERVICE_NAME}"
