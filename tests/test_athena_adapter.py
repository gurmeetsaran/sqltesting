"""Tests for the Athena adapter."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pandas as pd

from sql_testing_library.adapters.athena import AthenaAdapter, AthenaTypeConverter
from sql_testing_library.mock_table import BaseMockTable


# Mock boto3 for testing
@mock.patch("boto3.client")
class TestAthenaAdapter(unittest.TestCase):
    """Test Athena adapter functionality."""

    def setUp(self):
        """Set up common test data."""
        self.database = "test_db"
        self.s3_output_location = "s3://test-bucket/test-output/"
        self.region = "us-west-2"

    def test_initialization(self, mock_boto3_client):
        """Test adapter initialization."""
        # Test with credentials
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
            region=self.region,
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
        )

        mock_boto3_client.assert_called_once_with(
            "athena",
            region_name=self.region,
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
        )
        self.assertEqual(adapter.database, self.database)
        self.assertEqual(adapter.s3_output_location, self.s3_output_location)
        self.assertEqual(adapter.region, self.region)

        # Reset mock
        mock_boto3_client.reset_mock()

        # Test without credentials
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        mock_boto3_client.assert_called_once_with(
            "athena",
            region_name="us-west-2",  # Default region
        )

    def test_get_sqlglot_dialect(self, _):
        """Test getting sqlglot dialect."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "presto")

    def test_execute_query(self, mock_boto3_client):
        """Test query execution."""
        # Mock client responses
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        # Set up mock response for start_query_execution
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "test_query_id"}

        # Set up mock response for get_query_execution
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        # Set up mock response for get_query_results
        mock_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                    {
                        "Data": [
                            {"VarCharValue": "1"},
                            {"VarCharValue": "Test User"},
                        ]
                    },
                ]
            }
        }

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        query = "SELECT * FROM test_table"
        result_df = adapter.execute_query(query)

        # Check client calls
        mock_client.start_query_execution.assert_called_once_with(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.s3_output_location},
        )
        mock_client.get_query_execution.assert_called_with(QueryExecutionId="test_query_id")
        mock_client.get_query_results.assert_called_once_with(QueryExecutionId="test_query_id")

        # Check DataFrame result
        expected_df = pd.DataFrame([["1", "Test User"]], columns=["id", "name"])
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_format_value_for_cte(self, _):
        """Test value formatting for CTEs."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        # Test string formatting
        self.assertEqual(adapter.format_value_for_cte("test", str), "'test'")
        self.assertEqual(
            adapter.format_value_for_cte("test's", str), "'test''s'"
        )  # Note the escaped quote

        # Test numeric formatting
        self.assertEqual(adapter.format_value_for_cte(123, int), "123")
        self.assertEqual(adapter.format_value_for_cte(123.45, float), "123.45")
        self.assertEqual(adapter.format_value_for_cte(Decimal("123.45"), Decimal), "123.45")

        # Test boolean formatting
        self.assertEqual(adapter.format_value_for_cte(True, bool), "TRUE")
        self.assertEqual(adapter.format_value_for_cte(False, bool), "FALSE")

        # Test date/time formatting
        test_date = date(2023, 1, 15)
        self.assertEqual(adapter.format_value_for_cte(test_date, date), "DATE '2023-01-15'")
        test_datetime = datetime(2023, 1, 15, 10, 30, 45)
        self.assertEqual(
            adapter.format_value_for_cte(test_datetime, datetime),
            "TIMESTAMP '2023-01-15 10:30:45'",
        )

        # Test None
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL")

    def test_create_temp_table(self, mock_boto3_client):
        """Test temp table creation."""
        # Mock client responses
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        # Set up mock responses for execute_query calls
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "test_query_id"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        mock_client.get_query_results.return_value = {"ResultSet": {"Rows": []}}

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
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
                return "test_db"

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
            table_name = adapter.create_temp_table(mock_table)

        self.assertEqual(table_name, "test_db.temp_users_1234567890123")

        # Check that CTAS was called
        self.assertEqual(mock_client.start_query_execution.call_count, 1)
        ctas_call = mock_client.start_query_execution.call_args_list[0]

        # Verify it's a CTAS query
        self.assertIn("CREATE TABLE", ctas_call[1]["QueryString"])
        self.assertIn("AS SELECT", ctas_call[1]["QueryString"])

        # Check for data values in the query
        self.assertIn("1", ctas_call[1]["QueryString"])
        self.assertIn("'Alice'", ctas_call[1]["QueryString"])
        self.assertIn("'alice@example.com'", ctas_call[1]["QueryString"])
        self.assertIn("TRUE", ctas_call[1]["QueryString"])
        self.assertIn("DATE '2023-01-01'", ctas_call[1]["QueryString"])

        # Check for UNION ALL for the second row
        self.assertIn("UNION ALL", ctas_call[1]["QueryString"])
        self.assertIn("'Bob'", ctas_call[1]["QueryString"])
        self.assertIn("'bob@example.com'", ctas_call[1]["QueryString"])
        self.assertIn("FALSE", ctas_call[1]["QueryString"])
        self.assertIn("DATE '2023-01-02'", ctas_call[1]["QueryString"])

    def test_cleanup_temp_tables(self, mock_boto3_client):
        """Test temp table cleanup."""
        # Mock client responses
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        # Set up mock responses for execute_query calls
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "test_query_id"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        mock_client.get_query_results.return_value = {"ResultSet": {"Rows": []}}

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        # Test cleanup_temp_tables
        table_names = ["test_db.temp_table1", "test_db.temp_table2"]
        adapter.cleanup_temp_tables(table_names)

        # Check that DROP TABLE was called for each table
        self.assertEqual(mock_client.start_query_execution.call_count, 2)

        drop_call1 = mock_client.start_query_execution.call_args_list[0]
        self.assertIn("DROP TABLE IF EXISTS temp_table1", drop_call1[1]["QueryString"])

        drop_call2 = mock_client.start_query_execution.call_args_list[1]
        self.assertIn("DROP TABLE IF EXISTS temp_table2", drop_call2[1]["QueryString"])


class TestAthenaTypeConverter(unittest.TestCase):
    """Test Athena type converter."""

    def test_convert(self):
        """Test type conversion for Athena results."""
        converter = AthenaTypeConverter()

        # Test basic conversions
        self.assertEqual(converter.convert("123", int), 123)
        self.assertEqual(converter.convert("123.45", float), 123.45)
        self.assertEqual(converter.convert("true", bool), True)
        self.assertEqual(converter.convert("2023-01-15", date), date(2023, 1, 15))

        # Test None handling
        self.assertIsNone(converter.convert(None, str))
        self.assertIsNone(converter.convert("NULL", str))  # Athena NULL


if __name__ == "__main__":
    unittest.main()
