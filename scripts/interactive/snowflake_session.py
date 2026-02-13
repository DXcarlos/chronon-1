"""
Interact with snowflake catalog using pyspark.

Requires the catalogs to exist and a connection defined with ID and secret (both done in snowflake open catalog UI
"""
import os
import argparse

# -----------------------------
# Parse command-line arguments
# -----------------------------
parser = argparse.ArgumentParser(description="Register an existing Iceberg table in Snowflake Open Catalog")
parser.add_argument("--catalog", required=True, help="Catalog name (e.g., open)")
parser.add_argument("--account", required=True, help="Snowflake account (e.g., xy12345.us-east-1)")
parser.add_argument("--cloud", required=True, choices=["aws", "azure", "gcp"], help="Cloud provider (required)")
args = parser.parse_args()

jar_to_cloud={
        "aws": "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
        "azure": "org.apache.iceberg:iceberg-azure-bundle:1.5.2",
        "gcp": "org.apache.iceberg:iceberg-gcp-bundle:1.5.2",
        }

# -----------------------------
# Debug: Check environment variables
# -----------------------------
print("\n=== Configuration Check ===")
print(f"Cloud provider: {args.cloud}")
print(f"JAR package: {jar_to_cloud[args.cloud]}")
print(f"Catalog URI: https://{args.account}.snowflakecomputing.com/polaris/api/catalog")

client_id = os.environ.get('SNOWFLAKE_CLIENT_ID')
client_secret = os.environ.get('SNOWFLAKE_CLIENT_SECRET')

if not client_id:
    print("❌ ERROR: SNOWFLAKE_CLIENT_ID not set in environment")
    exit(1)
if not client_secret:
    print("❌ ERROR: SNOWFLAKE_CLIENT_SECRET not set in environment")
    exit(1)

# -----------------------------
# Step 2: Spark session
# -----------------------------
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('iceberg_lab') \
    .config('spark.jars.packages', f'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,{jar_to_cloud[args.cloud]}') \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config('spark.sql.defaultCatalog', 'opencatalog') \
    .config('spark.sql.catalog.opencatalog', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.opencatalog.type', 'rest') \
    .config('spark.sql.catalog.opencatalog.uri',f'https://{args.account}.snowflakecomputing.com/polaris/api/catalog') \
    .config('spark.sql.catalog.opencatalog.header.X-Iceberg-Access-Delegation','vended-credentials') \
    .config('spark.sql.catalog.opencatalog.credential',f"{os.environ.get('SNOWFLAKE_CLIENT_ID')}:{os.environ.get('SNOWFLAKE_CLIENT_SECRET')}") \
    .config('spark.sql.catalog.opencatalog.warehouse',args.catalog) \
    .config('spark.sql.catalog.opencatalog.scope','PRINCIPAL_ROLE:engine') \
    .getOrCreate()


# -----------------------------
# Step 3: Register existing table
# -----------------------------
spark.sql("SHOW CATALOGS").show()
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo").show()
spark.sql("SHOW TABLES IN demo").show()
print(""" Starting Debugging Session... for example:

Create a table
spark.sql("CREATE TABLE IF NOT EXISTS opencatalog.demo.test (location STRING) USING iceberg")

or load an existing table in the bucket
spark.sql("CALL opencatalog.system.register_table('demo.table', location)
""")
try:
    import pdb
    pdb.set_trace()
finally:
    spark.stop()
