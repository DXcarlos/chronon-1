"""Crucible runner for ``zipline run``.

Submits Spark/Flink jobs to a Crucible gateway via its REST API.
No JVM required — pure Python HTTP client.

Uses CLI tools (gcloud/aws) for cloud storage — no Python SDK deps needed.
Auth uses whatever the user has configured (gcloud auth login, AWS_PROFILE, etc).

Environment variables:
    CRUCIBLE_URL        — Crucible gateway URL (required)
    CRUCIBLE_NAMESPACE  — Target namespace (default: test-ns-a)
    CRUCIBLE_SPARK_IMAGE — Spark image
    CRUCIBLE_FLINK_IMAGE — Flink image
    WAREHOUSE_PREFIX    — GCS/S3 prefix for staging conf files (required)
"""

import hashlib
import json
import os
import subprocess
import time
import uuid

import requests

from ai.chronon.repo.default_runner import Runner

CRUCIBLE_URL_ENV = "CRUCIBLE_URL"
CRUCIBLE_NAMESPACE_ENV = "CRUCIBLE_NAMESPACE"
CRUCIBLE_SPARK_IMAGE_ENV = "CRUCIBLE_SPARK_IMAGE"
CRUCIBLE_FLINK_IMAGE_ENV = "CRUCIBLE_FLINK_IMAGE"
WAREHOUSE_PREFIX_ENV = "WAREHOUSE_PREFIX"

DEFAULT_SPARK_IMAGE = "us-docker.pkg.dev/crucible-io/crucible/spark:3.5-crucible-latest"
DEFAULT_FLINK_IMAGE = "us-docker.pkg.dev/crucible-io/crucible/flink:1.19-crucible-latest"

# Map Chronon conf types to Crucible job types
SPARK_MODES = {"backfill", "upload", "metastore", "check-partitions"}
FLINK_MODES = {"streaming", "streaming-client"}


def _stage_conf(warehouse_prefix, conf_json):
    """Stage conf JSON to cloud storage via CLI tools. Returns the full URI."""
    job_id = str(uuid.uuid4())
    name = hashlib.md5(conf_json.encode()).hexdigest()[:12]
    relative_path = f"metadata/execution/{job_id}/{name}"
    full_path = f"{warehouse_prefix.rstrip('/')}/{relative_path}"

    if full_path.startswith("gs://"):
        subprocess.run(
            ["gcloud", "storage", "cp", "-", full_path],
            input=conf_json.encode("utf-8"),
            check=True,
            capture_output=True,
        )
    elif full_path.startswith("s3://"):
        subprocess.run(
            ["aws", "s3", "cp", "-", full_path],
            input=conf_json.encode("utf-8"),
            check=True,
            capture_output=True,
        )
    else:
        raise ValueError(f"Unsupported storage scheme: {full_path}")

    return full_path


class CrucibleRunner(Runner):
    """Submit jobs to Crucible gateway via REST API."""

    def __init__(self, args):
        super().__init__(args, jar_path=None)
        self.base_url = os.environ.get(CRUCIBLE_URL_ENV)
        if not self.base_url:
            raise ValueError(f"{CRUCIBLE_URL_ENV} environment variable is required")
        self.namespace = os.environ.get(CRUCIBLE_NAMESPACE_ENV, "test-ns-a")
        self.spark_image = os.environ.get(CRUCIBLE_SPARK_IMAGE_ENV, DEFAULT_SPARK_IMAGE)
        self.flink_image = os.environ.get(CRUCIBLE_FLINK_IMAGE_ENV, DEFAULT_FLINK_IMAGE)

    def run(self):
        """Submit job to Crucible and poll until completion."""
        conf_path = os.path.join(self.repo, self.conf) if self.conf else None
        if not conf_path or not os.path.exists(conf_path):
            raise FileNotFoundError(f"Conf file not found: {conf_path}")

        with open(conf_path) as f:
            conf_json = f.read()

        # Read compiled conf metadata
        conf_data = json.loads(conf_json)
        metadata = conf_data.get("metaData", {})
        execution_info = metadata.get("executionInfo", {})
        spark_conf = execution_info.get("conf", {}).get("common", {})
        env_vars = execution_info.get("env", {}).get("common", {})

        # Stage conf to cloud storage
        warehouse_prefix = env_vars.get(
            "WAREHOUSE_PREFIX",
            os.environ.get(WAREHOUSE_PREFIX_ENV, ""),
        )
        if not warehouse_prefix:
            raise ValueError(
                f"{WAREHOUSE_PREFIX_ENV} must be set in env or node metadata"
            )
        staged_uri = _stage_conf(warehouse_prefix, conf_json)
        print(f"Staged conf to {staged_uri}")

        # Determine job type
        is_flink = self.mode in FLINK_MODES
        job_type = "flink" if is_flink else "spark"

        # Build jar URI
        artifact_prefix = env_vars.get("ARTIFACT_PREFIX", "")
        version = env_vars.get("VERSION", "latest")
        jar_name = os.environ.get("CRUCIBLE_JAR_NAME", "cloud_gcp_deploy.jar")
        jar_uri = f"{artifact_prefix}/release/{version}/jars/{jar_name}"

        # Build main class
        main_class = "ai.chronon.spark.batch.BatchNodeRunner"

        # Build application args — use --conf-path with the full cloud URI
        app_args = [f"--conf-path={staged_uri}"]

        if self.start_ds:
            app_args.append(f"--start-ds={self.start_ds}")
        if self.ds:
            app_args.append(f"--end-ds={self.ds}")

        online_class = env_vars.get(
            "CHRONON_ONLINE_CLASS", os.environ.get("CHRONON_ONLINE_CLASS", "")
        )
        if online_class:
            app_args.append(f"--online-class={online_class}")

            # KV store properties
            for key in ["GCP_PROJECT_ID", "GCP_BIGTABLE_INSTANCE_ID", "GCP_REGION"]:
                val = env_vars.get(key, os.environ.get(key, ""))
                if val:
                    app_args.append(f"-Z{key}={val}")

            tpd = os.environ.get("CRUCIBLE_TABLE_PARTITIONS_DATASET", "TABLE_PARTITIONS")
            dqd = os.environ.get("CRUCIBLE_DQ_METRICS_DATASET", "DATA_QUALITY_METRICS")
            app_args.append(f"--table-partitions-dataset={tpd}")
            app_args.append(f"--table-stats-dataset={dqd}")

        # Additional args from CLI
        extra = self._args.get("args", "")
        if extra:
            app_args.extend(extra.split())

        # Build submit body
        name = metadata.get("name", "chronon-job").replace(".", "-").replace("_", "-")[:63]
        body = {
            "name": name,
            "type": job_type,
            "image": self.flink_image if is_flink else self.spark_image,
            "mainClass": main_class,
            "jar": jar_uri,
            "args": app_args,
            "conf": spark_conf,
        }

        # Add extraClassPath for system classpath
        if jar_uri and not is_flink:
            local_jar = f"/opt/spark/work-dir/{jar_uri.split('/')[-1]}"
            body["conf"]["spark.driver.extraClassPath"] = local_jar
            body["conf"]["spark.executor.extraClassPath"] = local_jar

        # Submit
        url = f"{self.base_url}/api/v1/namespaces/{self.namespace}/jobs"
        print(f"Submitting {job_type} job to Crucible: {name}")
        resp = requests.post(url, json=body, timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Submit failed: {resp.status_code} {resp.text}")

        job_id = resp.json().get("id")
        print(f"Job submitted: {job_id}")

        # Poll until terminal state
        poll_url = f"{self.base_url}/api/v1/namespaces/{self.namespace}/jobs/{job_id}"
        terminal = {"COMPLETED", "FAILED", "KILLED"}
        start = time.time()
        timeout = 1800  # 30 min

        while time.time() - start < timeout:
            time.sleep(10)
            try:
                status_resp = requests.get(poll_url, timeout=10)
                if status_resp.status_code == 200:
                    status = status_resp.json().get("status", "UNKNOWN")
                    elapsed = int(time.time() - start)
                    print(f"  [{elapsed}s] {job_id}: {status}")
                    if status in terminal:
                        if status == "COMPLETED":
                            print(f"Job {job_id} completed successfully.")
                            return
                        else:
                            log_resp = requests.get(f"{poll_url}/logs", timeout=10)
                            logs = log_resp.text if log_resp.status_code == 200 else ""
                            error_lines = [
                                l
                                for l in logs.split("\n")
                                if "Exception" in l or "Error" in l
                            ][:5]
                            raise RuntimeError(
                                f"Job {job_id} {status}.\n" + "\n".join(error_lines)
                            )
                elif status_resp.status_code == 404:
                    print(f"Job {job_id} archived (404). Treating as completed.")
                    return
            except requests.ConnectionError:
                print(f"  Connection error polling {job_id}, retrying...")

        raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")
