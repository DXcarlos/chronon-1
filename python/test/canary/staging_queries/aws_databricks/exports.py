from ai.chronon.types import (
    ConfigProperties,
    EngineType,
    EnvironmentVariables,
    StagingQuery,
    TableDependency,
)

dim_listings = StagingQuery(
    query="""
    SELECT
        *
    FROM workspace.poc.dim_listings
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    version=0,
)

polaris_smoke_dim_listings = StagingQuery(
    query="""
    SELECT
        *
    FROM workspace.poc.dim_listings
    WHERE
    ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="polaris.default",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    conf=ConfigProperties(
        common={
            "spark.sql.catalog.workspace.renewCredential.enabled": "true",
            "spark.sql.catalog.polaris": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.polaris.type": "rest",
            "spark.sql.catalog.polaris.uri": "https://crucible-aws.zipline.ai/services/catalog",
            "spark.sql.catalog.polaris.warehouse": "polaris_crucible",
            "spark.sql.catalog.polaris.credential": "{OC_CREDENTIAL}",
            "spark.sql.catalog.polaris.scope": "PRINCIPAL_ROLE:ALL",
            "spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation": "vended-credentials",
            "spark.sql.catalog.polaris.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        }
    ),
    env_vars=EnvironmentVariables(
        common={
            "ARTIFACT_PREFIX": "s3://zipline-artifacts-crucible",
            "CUSTOMER_ID": "crucible",
            "EVAL_URL": "https://crucible-aws.zipline.ai/services/eval",
            "FRONTEND_URL": "https://crucible-aws.zipline.ai",
            "HUB_URL": "https://crucible-aws.zipline.ai/services/hub",
            "SPARK_CLUSTER_NAME": "crucible-eks",
            "WAREHOUSE_PREFIX": "s3://zipline-warehouse-crucible",
        }
    ),
    environments=["canary"],
    version=0,
)

dim_listings_non_partitioned = StagingQuery(
    query="""
    SELECT
        *, DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_nop
    WHERE
    DATE_FORMAT(updated_at_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_nop", partition_column="updated_at_ts", offset=0)
    ],
    version=0,
)

# Partition testing
dim_listings_pt = StagingQuery(
    query="""
    SELECT * FROM workspace.poc.dim_listings
    WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings", partition_column="ds", offset=0)
    ],
    version=1,
    step_days=30,
)

dim_listings_sparse = StagingQuery(
    query="""
    SELECT * FROM workspace.poc.dim_listings_sparse
    WHERE ds BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_sparse", partition_column="ds", offset=0)
    ],
    version=0,
    step_days=30,
)

dim_listings_unpartitioned = StagingQuery(
    query="""
    SELECT *,
        DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_unpartitioned
    WHERE DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_unpartitioned", partition_column="snapshot_ts", offset=0)
    ],
    version=0,
    step_days=30,
)

dim_listings_unpartitioned_sparse = StagingQuery(
    query="""
    SELECT *,
        DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_unpartitioned_sparse
    WHERE DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_unpartitioned_sparse", partition_column="snapshot_ts", offset=0)
    ],
    version=0,
    step_days=30,
)

# Bug repro: recompute_days causes duplicates on Iceberg
dim_listings_recompute = StagingQuery(
    query="""
    SELECT *,
        DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') AS ds
    FROM workspace.poc.dim_listings_unpartitioned
    WHERE DATE_FORMAT(snapshot_ts, 'yyyy-MM-dd') BETWEEN {{ start_date }} AND {{ end_date }}
    """,
    output_namespace="workspace_iceberg.poc",
    engine_type=EngineType.SPARK,
    dependencies=[
        TableDependency(table="workspace.poc.dim_listings_unpartitioned", partition_column="snapshot_ts", offset=0)
    ],
    version=0,
    recompute_days=3,
    step_days=30,
)
