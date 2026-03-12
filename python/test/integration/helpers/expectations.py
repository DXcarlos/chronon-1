"""Output verification helpers for nightly E2E tests.

Validates that backfill outputs exist and contain expected data by querying
AWS Glue / Athena.
"""

import logging
import os
import time

import boto3

logger = logging.getLogger(__name__)


class AthenaValidator:
    """Validate backfill output tables via Athena queries.

    Parameters
    ----------
    database : str
        Glue catalog database name.
    warehouse_bucket : str
        S3 bucket name for Athena query results.
    region : str
        AWS region.
    """

    def __init__(
        self,
        database: str = "default",
        warehouse_bucket: str | None = None,
        region: str | None = None,
    ):
        self.database = database
        self.region = region or os.environ.get("AWS_REGION", "us-west-2")
        self.warehouse_bucket = warehouse_bucket or os.environ.get("WAREHOUSE_BUCKET")
        self.athena = boto3.client("athena", region_name=self.region)
        self.output_location = f"s3://{self.warehouse_bucket}/athena-results/"

    def _run_query(self, sql: str, timeout: int = 120) -> list[dict]:
        """Execute an Athena query and return rows as dicts."""
        logger.info("Athena query: %s", sql)
        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
        )
        execution_id = response["QueryExecutionId"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.athena.get_query_execution(QueryExecutionId=execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                raise RuntimeError(f"Athena query {state}: {reason}")
            time.sleep(5)
        else:
            raise TimeoutError(f"Athena query did not complete within {timeout}s")

        result = self.athena.get_query_results(QueryExecutionId=execution_id)
        columns = [col["Label"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows = []
        for row in result["ResultSet"]["Rows"][1:]:  # skip header
            rows.append({col: datum.get("VarCharValue", "") for col, datum in zip(columns, row["Data"])})
        return rows

    def table_exists(self, table_name: str) -> bool:
        """Check if a Glue table exists."""
        glue = boto3.client("glue", region_name=self.region)
        try:
            glue.get_table(DatabaseName=self.database, Name=table_name)
            return True
        except glue.exceptions.EntityNotFoundException:
            return False

    def row_count(self, table_name: str) -> int:
        """Return the row count of a table."""
        rows = self._run_query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        return int(rows[0]["cnt"])

    def assert_table_has_rows(self, table_name: str, min_rows: int = 1):
        """Assert a table exists and has at least *min_rows* rows."""
        assert self.table_exists(table_name), f"Table {table_name} does not exist"
        count = self.row_count(table_name)
        logger.info("Table %s has %d rows (minimum: %d)", table_name, count, min_rows)
        assert count >= min_rows, f"Table {table_name} has {count} rows, expected >= {min_rows}"

    def assert_columns_present(self, table_name: str, expected_columns: list[str]):
        """Assert that a table contains the expected columns."""
        glue = boto3.client("glue", region_name=self.region)
        table = glue.get_table(DatabaseName=self.database, Name=table_name)
        actual_columns = {col["Name"] for col in table["Table"]["StorageDescriptor"]["Columns"]}
        missing = set(expected_columns) - actual_columns
        assert not missing, f"Table {table_name} missing columns: {missing}"
