from gen_thrift.api.ttypes import Team

from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.table_write.format": "iceberg",
            "spark.sql.defaultCatalog": "bigquery_catalog",
            "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.bigquery_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.bigquery_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.kryo.registrator": "ai.chronon.integrations.cloud_gcp.ChrononIcebergKryoRegistrator",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            # TODO: Please fill in the following values
            "spark.sql.catalog.bigquery_catalog.warehouse": "gs://zipline-warehouse-<customer_id>/data/tables/",
            "spark.sql.catalog.bigquery_catalog.gcp.bigquery.location": "<region>",
            "spark.sql.catalog.bigquery_catalog.gcp.bigquery.project-id": "<project-id>",
            "spark.chronon.partition.format": "<date-format>",  # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>",  # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            # TODO: Please fill in the following values
            "CUSTOMER_ID": "<customer_id>",
            "GCP_PROJECT_ID": "<project-id>",
            "GCP_REGION": "<region>",
            "GCP_DATAPROC_CLUSTER_NAME": "<dataproc-cluster-name>",
            "GCP_BIGTABLE_INSTANCE_ID": "<bigtable-instance-id>",
            "ARTIFACT_PREFIX": "<customer-artifact-bucket>",
            "CLOUD_PROVIDER": "<gcp | aws>",
        },
    ),
)


test = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={}, modeEnvironments={RunMode.BACKFILL: {}, RunMode.UPLOAD: {}}
    ),
)

team_conf = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={},
    ),
)
