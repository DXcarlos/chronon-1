from gen_thrift.api.ttypes import Team

from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_azure.AzureFormatProvider",
            "spark.chronon.table_write.format": "iceberg",
            "spark.kryo.registrator": "ai.chronon.spark.submission.ChrononKryoRegistrator",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",

            # Setup iceberg
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.type": "rest",
            "spark.sql.catalog.spark_catalog.header.X-Iceberg-Access-Delegation": "vended-credentials",

            # TODO: Please fill in the following values
            "spark.sql.catalog.spark_catalog.uri": "<URI>",
            "spark.sql.catalog.spark_catalog.credential": "<CLIENT_ID>:<SECRET>",
            "spark.sql.catalog.spark_catalog.warehouse": "<NAME>",
            "spark.sql.catalog.spark_catalog.scope": "PRINCIPAL_ROLE:<NAME>",
            "spark.chronon.partition.format": "<date-format>",  # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>",  # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "azure",
            # TODO: Please fill in the following values
            "CUSTOMER_ID": "<customer_id>",
            "ARTIFACT_PREFIX": "<customer-artifact-bucket>", # ex: "abfss://dev-zipline-artifacts@ziplineai2.dfs.core.windows.net"
            "VERSION": "<version>",  # ex: "Chronon engine version"
            "SNOWFLAKE_JDBC_URL": "<jdbc-url>",  # ex: "jdbc:snowflake://<account_identifier>.snowflakecomputing.com"
            "SNOWFLAKE_VAULT_URI": "<vault-uri>",  # ex: "https://<your-vault-name>.vault.azure.net/"
            "HUB_URL": "<hub-url>",  # URL to the Zipline Hub control plane
            "FRONTEND_URL": "<frontend-url>",  # URL to the Zipline Frontend
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
