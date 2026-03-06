from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ClusterConfigProperties, ConfigProperties, EnvironmentVariables

try:
    from gen_thrift.api.ttypes import Team
except ImportError:
    raise ImportError(
        "gen_thrift not found. Run 'zipline compile' to generate the thrift types first."
    )

quickstart = Team(
    description="Local kind cluster — quickstart team",
    email="dev@example.com",
    outputNamespace="quickstart",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
            # Iceberg catalog backed by PostgreSQL in-cluster
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "jdbc",
            "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://postgres.chronon.svc.cluster.local:5432/iceberg_catalog",
            "spark.sql.catalog.iceberg.jdbc.user": "chronon",
            "spark.sql.catalog.iceberg.jdbc.password": "chronon",
            "spark.sql.catalog.iceberg.warehouse": "s3a://warehouse/",
            # S3A pointing at MinIO in-cluster
            "spark.hadoop.fs.s3a.endpoint": "http://minio.chronon.svc.cluster.local:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": "iceberg",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
        },
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.driver.memory": "512m",
                "spark.executor.memory": "1g",
                "spark.executor.cores": "1",
                "spark.executor.instances": "1",
            },
            RunMode.UPLOAD: {
                "spark.driver.memory": "512m",
                "spark.executor.memory": "512m",
            },
        },
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "K8S_NAMESPACE": "chronon",
            "SPARK_IMAGE": "chronon-spark-k8s:latest",
            "SPARK_SERVICE_ACCOUNT": "spark",
            "SPARK_EVENT_LOG_ENABLED": "true",
            "SPARK_EVENT_LOG_DIR": "s3a://chronon-spark-logs/event-logs",
            "ARTIFACT_PREFIX": "s3a://warehouse",
            "WAREHOUSE_PREFIX": "s3a://warehouse",
            "HUB_URL": "http://localhost:8080",
            "FRONTEND_URL": "http://localhost:3000",
            "ONLINE_CLASS": "ai.chronon.integrations.cloud_k8s.LocalRedisApiImpl",
        },
        modeEnvironments={
            RunMode.BACKFILL: {},
            RunMode.UPLOAD: {},
            RunMode.STREAMING: {},
        },
    ),
)
