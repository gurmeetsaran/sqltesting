"""Tests for the BigQuery adapter."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pandas as pd

from sql_testing_library._adapters.bigquery import BigQueryAdapter, BigQueryTypeConverter
from sql_testing_library._mock_table import BaseMockTable


# Mock google.cloud.bigquery for testing
@mock.patch("google.cloud.bigquery.Client")
class TestBigQueryAdapter(unittest.TestCase):
    """Test BigQuery adapter functionality."""

    def setUp(self):
        """Set up common test data."""
        self.project_id = "test-project"
        self.dataset_id = "test_dataset"
        self.credentials_path = "/path/to/credentials.json"

    def test_initialization_with_credentials_path(self, mock_bigquery_client):
        """Test adapter initialization with credentials path."""
        mock_client = mock.MagicMock()
        mock_bigquery_client.from_service_account_json.return_value = mock_client

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            credentials_path=self.credentials_path,
        )

        # Client should be created with credentials path
        mock_bigquery_client.from_service_account_json.assert_called_once_with(
            self.credentials_path
        )

        # Check properties are set correctly
        self.assertEqual(adapter.project_id, self.project_id)
        self.assertEqual(adapter.dataset_id, self.dataset_id)
        self.assertEqual(adapter.client, mock_client)

    def test_initialization_without_credentials_path(self, mock_bigquery_client):
        """Test adapter initialization without credentials path."""
        mock_client = mock.MagicMock()
        mock_bigquery_client.return_value = mock_client

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Client should be created with project_id
        mock_bigquery_client.assert_called_once_with(project=self.project_id)

        # Check properties are set correctly
        self.assertEqual(adapter.project_id, self.project_id)
        self.assertEqual(adapter.dataset_id, self.dataset_id)
        self.assertEqual(adapter.client, mock_client)

    def test_get_sqlglot_dialect(self, _):
        """Test getting sqlglot dialect."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "bigquery")

    def test_execute_query(self, mock_bigquery_client):
        """Test query execution."""
        # Set up mock client and query job
        mock_client = mock.MagicMock()
        mock_query_job = mock.MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_bigquery_client.return_value = mock_client

        # Mock the DataFrame result
        mock_df = pd.DataFrame([{"id": 1, "name": "Test User"}, {"id": 2, "name": "Another User"}])
        mock_query_job.to_dataframe.return_value = mock_df

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        query = "SELECT * FROM test_table"
        result_df = adapter.execute_query(query)

        # Check client calls
        mock_client.query.assert_called_once_with(query)
        mock_query_job.to_dataframe.assert_called_once()

        # Check DataFrame result
        pd.testing.assert_frame_equal(result_df, mock_df)

    def test_format_value_for_cte(self, _):
        """Test value formatting for CTEs."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Test string formatting
        self.assertEqual(adapter.format_value_for_cte("test", str), "'test'")
        self.assertEqual(
            adapter.format_value_for_cte("test's", str), '''"""test's"""'''
        )  # BigQuery uses triple-quoted strings for strings with quotes

        # Test numeric formatting
        self.assertEqual(adapter.format_value_for_cte(123, int), "123")
        self.assertEqual(adapter.format_value_for_cte(123.45, float), "123.45")
        self.assertEqual(adapter.format_value_for_cte(Decimal("123.45"), Decimal), "123.45")

        # Test boolean formatting
        self.assertEqual(adapter.format_value_for_cte(True, bool), "TRUE")
        self.assertEqual(adapter.format_value_for_cte(False, bool), "FALSE")

        # Test date/time formatting
        test_date = date(2023, 1, 15)
        self.assertEqual(adapter.format_value_for_cte(test_date, date), "DATE('2023-01-15')")
        test_datetime = datetime(2023, 1, 15, 10, 30, 45)
        self.assertEqual(
            adapter.format_value_for_cte(test_datetime, datetime),
            f"DATETIME('{test_datetime.isoformat()}')",
        )

        # Test None
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL")

    def test_create_temp_table(self, mock_bigquery_client):
        """Test temp table creation."""
        # Set up mock client and table creation
        mock_client = mock.MagicMock()
        mock_table = mock.MagicMock()
        mock_job = mock.MagicMock()
        mock_client.create_table.return_value = mock_table
        mock_client.load_table_from_dataframe.return_value = mock_job
        mock_bigquery_client.return_value = mock_client

        # Mock bigquery SchemaField
        with mock.patch("google.cloud.bigquery.SchemaField") as mock_schema_field:
            with mock.patch("google.cloud.bigquery.Table") as mock_table_class:
                mock_schema_field.side_effect = lambda name, field_type: f"{name}:{field_type}"
                mock_table_class.return_value = mock_table

                adapter = BigQueryAdapter(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                )

                # Create a mock table
                @dataclass
                class User:
                    id: int
                    name: str
                    email: str
                    active: bool
                    created_at: date

                class UserMockTable(BaseMockTable):
                    def get_database_name(self) -> str:
                        return "test_dataset"

                    def get_table_name(self) -> str:
                        return "users"

                # Create a mock table with test data
                mock_table = UserMockTable(
                    [
                        User(1, "Alice", "alice@example.com", True, date(2023, 1, 1)),
                        User(2, "Bob", "bob@example.com", False, date(2023, 1, 2)),
                    ]
                )

                # Test create_temp_table
                with mock.patch("time.time", return_value=1234567890.123):
                    table_id = adapter.create_temp_table(mock_table)

                # Check table name format (project.dataset.table)
                self.assertEqual(
                    table_id, f"{self.project_id}.{self.dataset_id}.temp_users_1234567890123"
                )

                # Verify create_table was called
                mock_table_class.assert_called_once()
                mock_client.create_table.assert_called_once()

                # Verify data was loaded
                mock_client.load_table_from_dataframe.assert_called_once()
                mock_job.result.assert_called_once()  # Wait for job completion

    def test_cleanup_temp_tables(self, mock_bigquery_client):
        """Test temp table cleanup."""
        # Set up mock client
        mock_client = mock.MagicMock()
        mock_bigquery_client.return_value = mock_client

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Test cleanup_temp_tables
        table_names = [
            f"{self.project_id}.{self.dataset_id}.temp_table1",
            f"{self.project_id}.{self.dataset_id}.temp_table2",
        ]
        adapter.cleanup_temp_tables(table_names)

        # Verify delete_table was called for each table
        self.assertEqual(mock_client.delete_table.call_count, 2)
        mock_client.delete_table.assert_any_call(f"{self.project_id}.{self.dataset_id}.temp_table1")
        mock_client.delete_table.assert_any_call(f"{self.project_id}.{self.dataset_id}.temp_table2")

    def test_cleanup_temp_tables_with_error(self, mock_bigquery_client):
        """Test temp table cleanup with error handling."""
        # Set up mock client with error on delete
        mock_client = mock.MagicMock()
        mock_client.delete_table.side_effect = Exception("Table not found")
        mock_bigquery_client.return_value = mock_client

        # Mock logging to verify warning
        with mock.patch("logging.warning") as mock_warning:
            adapter = BigQueryAdapter(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
            )

            # Test cleanup_temp_tables with error
            table_name = f"{self.project_id}.{self.dataset_id}.temp_table1"
            adapter.cleanup_temp_tables([table_name])

            # Verify delete_table was called
            mock_client.delete_table.assert_called_once_with(table_name)

            # Verify warning was logged
            mock_warning.assert_called_once()
            warning_message = mock_warning.call_args[0][0]
            self.assertIn("Warning: Failed to delete table", warning_message)
            self.assertIn(table_name, warning_message)


class TestBigQueryTypeConverter(unittest.TestCase):
    """Test BigQuery type converter."""

    def test_convert(self):
        """Test type conversion for BigQuery results."""
        converter = BigQueryTypeConverter()

        # Test basic conversions
        self.assertEqual(converter.convert(123, int), 123)
        self.assertEqual(converter.convert(123.45, float), 123.45)
        self.assertEqual(converter.convert(True, bool), True)
        self.assertEqual(converter.convert(False, bool), False)
        self.assertEqual(converter.convert(date(2023, 1, 15), date), date(2023, 1, 15))

        # Test None handling
        self.assertIsNone(converter.convert(None, str))


if __name__ == "__main__":
    unittest.main()
