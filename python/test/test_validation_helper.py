"""Unit tests for integration validation helpers with mocked cloud clients."""

from unittest.mock import MagicMock, patch

import pytest

from integration.helpers.validation import (
    validate_columns_non_null,
    validate_table_has_data,
)


# ---------------------------------------------------------------------------
# GCP tests
# ---------------------------------------------------------------------------


@patch("integration.helpers.validation._bigquery")
def test_gcp_validate_returns_true_when_data_exists(mock_bq):
    mock_client = MagicMock()
    mock_bq.Client.return_value = mock_client

    row = MagicMock()
    row.cnt = 5
    mock_client.query.return_value.result.return_value = iter([row])

    assert validate_table_has_data("gcp", "project.dataset.table", "ds", "2024-01-01") is True


@patch("integration.helpers.validation._bigquery")
def test_gcp_validate_returns_false_when_no_data(mock_bq):
    mock_client = MagicMock()
    mock_bq.Client.return_value = mock_client

    row = MagicMock()
    row.cnt = 0
    mock_client.query.return_value.result.return_value = iter([row])

    assert validate_table_has_data("gcp", "project.dataset.table", "ds", "2024-01-01") is False


@patch("integration.helpers.validation._bigquery")
def test_gcp_columns_non_null_returns_true(mock_bq):
    mock_client = MagicMock()
    mock_bq.Client.return_value = mock_client

    row = MagicMock()
    row.total = 10
    row.col_a_nonnull = 8
    row.col_b_nonnull = 3
    mock_client.query.return_value.result.return_value = iter([row])

    assert (
        validate_columns_non_null(
            "gcp", "project.dataset.table", ["col_a", "col_b"], "ds", "2024-01-01"
        )
        is True
    )


@patch("integration.helpers.validation._bigquery")
def test_gcp_columns_non_null_returns_false_when_all_null(mock_bq):
    mock_client = MagicMock()
    mock_bq.Client.return_value = mock_client

    row = MagicMock()
    row.total = 10
    row.col_a_nonnull = 0
    row.col_b_nonnull = 5
    mock_client.query.return_value.result.return_value = iter([row])

    assert (
        validate_columns_non_null(
            "gcp", "project.dataset.table", ["col_a", "col_b"], "ds", "2024-01-01"
        )
        is False
    )


# ---------------------------------------------------------------------------
# AWS tests
# ---------------------------------------------------------------------------


@patch("integration.helpers.validation.boto3")
def test_aws_validate_returns_true_when_parquet_exists(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "data/tables/my_table/data/ds=2024-01-01/part-00000.parquet"},
        ]
    }

    assert validate_table_has_data("aws", "my_table", "ds", "2024-01-01") is True


@patch("integration.helpers.validation.boto3")
def test_aws_validate_returns_false_when_no_parquet(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    mock_s3.list_objects_v2.return_value = {"Contents": []}

    assert validate_table_has_data("aws", "my_table", "ds", "2024-01-01") is False


# ---------------------------------------------------------------------------
# Azure tests
# ---------------------------------------------------------------------------


def test_azure_validate_returns_true_stub():
    assert validate_table_has_data("azure", "some_table", "ds", "2024-01-01") is True


def test_azure_columns_non_null_returns_true_stub():
    assert validate_columns_non_null("azure", "some_table", ["col_a"], "ds", "2024-01-01") is True
