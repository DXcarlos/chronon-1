"""End-to-end test fixture for scd2_to_entity_staging_queries.

Drives the helper against a small SCD2 user table covering the canonical patterns
(insert, update, delete-by-end_ts) so the same YAML fixture can verify both the
mutation and the snapshot output via the platform ExpectationsRunner CLI.

Output confs (compile names → Iceberg output tables):
  azure.scd2_test.mut_v1__0  → data.azure_scd2_test_mut_v1__0
  azure.scd2_test.snap_v1__0 → data.azure_scd2_test_snap_v1__0
"""

from staging_queries.azure.scd2 import scd2_to_entity_staging_queries

from ai.chronon.staging_query import EngineType

# engine_type=SPARK so the emitted SQL runs against the local Iceberg-backed Spark
# session that ExpectationsRunner stands up. The Snowflake emit is exercised by
# whatever in-warehouse pipeline picks up the helper in production.
mut_v1, snap_v1 = scd2_to_entity_staging_queries(
    scd2_table_name="default.test_scd2_user",
    begin_ts_column="record_begin_timestamp",
    end_ts_column="record_end_timestamp",
    entity_key_columns=["id"],
    engine_type=EngineType.SPARK,
)
