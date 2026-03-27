class Configuration(dict):
    def __init__(self, d: dict[str, str] = None, required: list[str] = None):
        super().__init__(d or {})
        self._required = required or []

    def __call__(self, required_values=None) -> "Configuration":
        if required_values is None:
            required_values = {}
        missing = [k for k in self._required if k not in required_values]
        if missing:
            raise ValueError(f"Missing required values: {missing}")
        # Merge template-resolved config with required_values
        resolved = self.resolve(self, **required_values)
        resolved.update(required_values)
        return Configuration(resolved)

    @staticmethod
    def resolve(d: dict[str, str], **values) -> dict[str, str]:
        return {k: v.format_map(values) for k, v in d.items()}


GlueConfiguration = Configuration(
    {
        # TODO: I noticed there wasn't a "format_provider.class" for AWS. Do we need one?
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    },
    required=[
        "spark.sql.catalog.spark_catalog.warehouse",

    ]
)

BigQueryConfiguration = Configuration(
    {
        "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
        "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
        "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false"
    },
    required=[
        "spark.sql.catalog.spark_catalog.warehouse",
        "spark.sql.catalog.spark_catalog.gcp.bigquery.location",
        "spark.sql.catalog.spark_catalog.gcp.bigquery.project-id",
    ]
)

OpenCatalogConfiguration = Configuration(
    {
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",  # TODO: we probably want to change this to SparkSessionCatalog
        "spark.sql.catalog.spark_catalog.type": "rest",
        "spark.sql.catalog.spark_catalog.header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
    required=[
        "spark.sql.catalog.spark_catalog.uri",
        "spark.sql.catalog.spark_catalog.credential",
        "spark.sql.catalog.spark_catalog.warehouse",
        "spark.sql.catalog.spark_catalog.scope"
    ]
)