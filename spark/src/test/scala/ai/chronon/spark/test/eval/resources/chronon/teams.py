from ai.chronon.api.ttypes import Team
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "JOB_MODE": "local[*]",
            "HADOOP_DIR": "[STREAMING-TODO]/path/to/folder/containing",
            "CHRONON_ONLINE_CLASS": "[ONLINE-TODO]your.online.class",
            "CHRONON_ONLINE_ARGS": "[ONLINE-TODO]args prefixed with -Z become constructor map for your implementation of ai.chronon.online.Api, -Zkv-host=<YOUR_HOST> -Zkv-port=<YOUR_PORT>",
            "PARTITION_COLUMN": "ds",
            "PARTITION_FORMAT": "yyyy-MM-dd",
            "CUSTOMER_ID": "dev",
            "GCP_PROJECT_ID": "canary-443022",
            "GCP_REGION": "us-central1",
            "GCP_DATAPROC_CLUSTER_NAME": "zipline-canary-cluster",
            "GCP_BIGTABLE_INSTANCE_ID": "zipline-canary-instance",
            "FLINK_STATE_URI": "gs://zipline-warehouse-canary/flink-state",
            "FRONTEND_URL": "http://localhost:5173",
            "HUB_URL": "http://localhost:3903"
        },
    ),
)

test = Team(
    outputNamespace="local_test",
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        modeEnvironments={
            RunMode.BACKFILL: {
                "EXECUTOR_CORES": "2",
                "DRIVER_MEMORY": "15G",
                "EXECUTOR_MEMORY": "4G",
                "PARALLELISM": "4",
                "MAX_EXECUTORS": "4",
            },
            RunMode.UPLOAD: {
                "PARALLELISM": "2",
                "MAX_EXECUTORS": "4",
            }
        }
    ),
)

