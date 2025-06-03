"""Additional tests for BigQuery adapter to improve coverage."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from unittest import mock

from sql_testing_library._adapters.bigquery import BigQueryAdapter, BigQueryTypeConverter
from sql_testing_library._mock_table import BaseMockTable


class TestBigQueryAdapterCoverageBoost(unittest.TestCase):
    """Additional tests to boost BigQuery adapter coverage."""

    def setUp(self):
        """Set up common test data."""
        self.project_id = "test-project"
        self.dataset_id = "test_dataset"
        self.credentials_path = "/path/to/credentials.json"

    def test_has_bigquery_constant_exists(self):
        """Test that the has_bigquery constant exists and is True in test environment."""
        from sql_testing_library._adapters.bigquery import has_bigquery

        # has_bigquery should be True in this test environment (since BigQuery works)
        self.assertTrue(has_bigquery)

        # The constant should be boolean
        self.assertIsInstance(has_bigquery, bool)

    @mock.patch("google.cloud.bigquery.Client")
    def test_initialization_without_credentials(self, mock_bigquery_client):
        """Test adapter initialization without credentials path."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Check properties are set correctly
        self.assertEqual(adapter.project_id, self.project_id)
        self.assertEqual(adapter.dataset_id, self.dataset_id)
        self.assertIsNotNone(adapter.client)

    def test_initialization_with_credentials(self):
        """Test adapter initialization with credentials path."""
        with mock.patch(
            "google.cloud.bigquery.Client.from_service_account_json"
        ) as mock_from_service:
            mock_client = mock.MagicMock()
            mock_from_service.return_value = mock_client

            adapter = BigQueryAdapter(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                credentials_path=self.credentials_path,
            )

            # Check properties are set correctly
            self.assertEqual(adapter.project_id, self.project_id)
            self.assertEqual(adapter.dataset_id, self.dataset_id)
            # Verify credentials were used
            mock_from_service.assert_called_once_with(self.credentials_path)
            self.assertEqual(adapter.client, mock_client)

    @mock.patch("google.cloud.bigquery.Client")
    def test_get_sqlglot_dialect(self, mock_bigquery_client):
        """Test getting sqlglot dialect."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "bigquery")

    @mock.patch("google.cloud.bigquery.Client")
    def test_get_type_converter(self, mock_bigquery_client):
        """Test getting type converter."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        converter = adapter.get_type_converter()
        self.assertIsInstance(converter, BigQueryTypeConverter)

    @mock.patch("google.cloud.bigquery.Client")
    def test_get_query_size_limit(self, mock_bigquery_client):
        """Test getting query size limit."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # BigQuery adapter doesn't implement get_query_size_limit
        # It inherits from base class which returns None
        limit = adapter.get_query_size_limit()
        self.assertIsNone(limit)

    @mock.patch("google.cloud.bigquery.Client")
    def test_cleanup_temp_tables(self, mock_bigquery_client):
        """Test temp table cleanup."""
        mock_client = mock.MagicMock()
        mock_bigquery_client.return_value = mock_client

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Test cleanup_temp_tables - should call delete_table for each table
        table_names = ["temp_table1", "temp_table2"]
        adapter.cleanup_temp_tables(table_names)

        # Verify delete_table was called for each table
        self.assertEqual(mock_client.delete_table.call_count, 2)
        mock_client.delete_table.assert_any_call("temp_table1")
        mock_client.delete_table.assert_any_call("temp_table2")

    @mock.patch("google.cloud.bigquery.Client")
    def test_cleanup_temp_tables_with_errors(self, mock_bigquery_client):
        """Test temp table cleanup with errors."""
        mock_client = mock.MagicMock()
        mock_bigquery_client.return_value = mock_client

        # Mock delete_table to raise an exception
        mock_client.delete_table.side_effect = Exception("Table not found")

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Test that cleanup handles errors gracefully (should not raise)
        with mock.patch("logging.warning") as mock_warning:
            table_names = ["temp_table1", "temp_table2"]
            adapter.cleanup_temp_tables(table_names)  # Should not raise

            # Verify warnings were logged
            self.assertEqual(mock_warning.call_count, 2)

    @mock.patch("google.cloud.bigquery.Client")
    def test_execute_query_error_handling(self, mock_bigquery_client):
        """Test query execution error handling."""
        mock_client = mock.MagicMock()
        mock_bigquery_client.return_value = mock_client

        # Test with query error
        mock_client.query.side_effect = Exception("SQL syntax error")

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        with self.assertRaises(Exception) as context:
            adapter.execute_query("INVALID SQL")

        self.assertIn("SQL syntax error", str(context.exception))

    @mock.patch("google.cloud.bigquery.Client")
    def test_format_value_for_cte_edge_cases(self, mock_bigquery_client):
        """Test value formatting edge cases for CTEs."""
        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        # Test array formatting
        test_array = ["hello", "world", "bigquery"]
        result = adapter.format_value_for_cte(test_array, List[str])
        self.assertIn("[", result)
        self.assertIn("hello", result)
        self.assertIn("world", result)

        # Test integer array
        int_array = [1, 2, 3, 42]
        result = adapter.format_value_for_cte(int_array, List[int])
        self.assertIn("[", result)
        self.assertIn("1, 2, 3, 42", result)

        # Test empty array
        empty_array = []
        result = adapter.format_value_for_cte(empty_array, List[str])
        self.assertIn("[]", result)

        # Test None value
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL")

    @mock.patch("google.cloud.bigquery.Client")
    def test_create_temp_table_coverage(self, mock_bigquery_client):
        """Test create_temp_table method coverage."""
        mock_client = mock.MagicMock()
        mock_bigquery_client.return_value = mock_client

        # Mock the job result
        mock_job = mock.MagicMock()
        mock_client.create_table.return_value = mock.MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_job

        adapter = BigQueryAdapter(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
        )

        @dataclass
        class TestData:
            id: int
            name: str

        class TestMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "test_table"

        # Create mock table with data
        mock_table = TestMockTable([TestData(1, "test")])

        # Call create_temp_table
        result = adapter.create_temp_table(mock_table)

        # Verify table was created and data loaded
        self.assertTrue(result.startswith(f"{self.project_id}.{self.dataset_id}.temp_"))
        mock_client.create_table.assert_called_once()
        mock_client.load_table_from_dataframe.assert_called_once()
        mock_job.result.assert_called_once()


class TestBigQueryTypeConverterCoverage(unittest.TestCase):
    """Additional tests for BigQueryTypeConverter to improve coverage."""

    def test_base_converter_functionality(self):
        """Test that base converter functionality works through BigQuery converter."""
        converter = BigQueryTypeConverter()

        # Test inherited functionality from BaseTypeConverter
        self.assertEqual(converter.convert("123", int), 123)
        self.assertEqual(converter.convert("123.45", float), 123.45)
        self.assertEqual(converter.convert("true", bool), True)
        self.assertEqual(converter.convert("false", bool), False)
        self.assertEqual(converter.convert("2023-01-15", date), date(2023, 1, 15))
        self.assertEqual(
            converter.convert("2023-01-15T10:30:45", datetime), datetime(2023, 1, 15, 10, 30, 45)
        )
        self.assertEqual(converter.convert("123.45", Decimal), Decimal("123.45"))

        # Test None handling
        self.assertIsNone(converter.convert(None, str))
        self.assertIsNone(converter.convert(None, int))

        # Test string conversion
        self.assertEqual(converter.convert("test", str), "test")
        self.assertEqual(converter.convert(123, str), "123")

    def test_bigquery_specific_conversions(self):
        """Test BigQuery-specific type conversions."""
        converter = BigQueryTypeConverter()

        # Test boolean string variations
        self.assertTrue(converter.convert("1", bool))
        self.assertTrue(converter.convert("yes", bool))
        self.assertTrue(converter.convert("t", bool))
        self.assertFalse(converter.convert("0", bool))
        self.assertFalse(converter.convert("no", bool))
        self.assertFalse(converter.convert("f", bool))

        # Test numeric edge cases
        self.assertEqual(converter.convert("0", int), 0)
        self.assertEqual(converter.convert("0.0", float), 0.0)
        self.assertEqual(converter.convert("123.0", int), 123)  # Float string to int

        # Test Decimal edge cases
        self.assertEqual(converter.convert(123, Decimal), Decimal("123"))
        self.assertEqual(converter.convert(123.45, Decimal), Decimal("123.45"))

    def test_array_conversions(self):
        """Test array conversion handling."""
        converter = BigQueryTypeConverter()

        # Test list types
        test_list = [1, 2, 3]
        result = converter.convert(test_list, List[int])
        self.assertEqual(result, test_list)

        # Test string array format
        result = converter.convert("[1, 2, 3]", List[int])
        self.assertEqual(result, [1, 2, 3])

        result = converter.convert("['hello', 'world']", List[str])
        self.assertEqual(result, ["hello", "world"])

        # Test empty array
        result = converter.convert("[]", List[str])
        self.assertEqual(result, [])

        # Test None array
        result = converter.convert(None, List[str])
        self.assertIsNone(result)

    def test_optional_type_handling(self):
        """Test Optional type handling."""
        converter = BigQueryTypeConverter()

        # Test Optional types with values
        self.assertEqual(converter.convert("test", Optional[str]), "test")
        self.assertEqual(converter.convert("123", Optional[int]), 123)
        self.assertEqual(converter.convert("true", Optional[bool]), True)

        # Test Optional types with None
        self.assertIsNone(converter.convert(None, Optional[str]))
        self.assertIsNone(converter.convert(None, Optional[int]))
        self.assertIsNone(converter.convert(None, Optional[bool]))


if __name__ == "__main__":
    unittest.main()
