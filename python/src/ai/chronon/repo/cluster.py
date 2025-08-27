import json


def generate_dataproc_cluster_config(
    num_workers,
    project_id,
    artifact_prefix,
    master_host_type="n2-highmem-64",
    worker_host_type="n2-highmem-16",
    subnetwork="default",
    idle_timeout="7200s",
    initialization_actions=None,
    tags=None,
):
    """
    Create a configuration for a Dataproc cluster.
    :return: A json string representing the configuration.
    """
    if initialization_actions is None:
        initialization_actions = []
    return json.dumps(
        {
            "gceClusterConfig": {
                "subnetworkUri": subnetwork,
                "serviceAccount": "dataproc@" + project_id + ".iam.gserviceaccount.com",
                "serviceAccountScopes": [
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/logging.write",
                ],
                "metadata": {
                    "hive-version": "3.1.2",
                    "SPARK_BQ_CONNECTOR_URL": "gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.1.jar",
                    "artifact_prefix": artifact_prefix.rstrip("/"),
                },
                "tags": tags or [],
            },
            "masterConfig": {
                "numInstances": 1,
                "machineTypeUri": master_host_type,
                "diskConfig": {"bootDiskType": "pd-standard", "bootDiskSizeGb": 1024},
            },
            "workerConfig": {
                "numInstances": num_workers,
                "machineTypeUri": worker_host_type,
                "diskConfig": {
                    "bootDiskType": "pd-standard",
                    "bootDiskSizeGb": 64,
                    "numLocalSsds": 2,
                },
            },
            "softwareConfig": {
                "imageVersion": "2.2.50-debian12",
                "optionalComponents": [
                    "FLINK",
                    "JUPYTER",
                ],
                "properties": {},
            },
            "initializationActions": [
                {"executable_file": initialization_action}
                for initialization_action in (
                    (initialization_actions or [])
                    + [artifact_prefix.rstrip("/") + "/scripts/copy_java_security.sh"]
                )
            ],
            "endpointConfig": {
                "enableHttpPortAccess": True,
            },
            "lifecycleConfig": {
                "idleDeleteTtl": idle_timeout,
            },
        }
    )
