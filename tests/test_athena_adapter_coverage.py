"""Additional tests for Athena adapter to improve coverage."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from unittest import mock

import pandas as pd

from sql_testing_library._adapters.athena import AthenaAdapter, AthenaTypeConverter
from sql_testing_library._mock_table import BaseMockTable


class TestAthenaAdapterCoverageBoost(unittest.TestCase):
    """Additional tests to boost Athena adapter coverage."""

    def setUp(self):
        """Set up common test data."""
        self.database = "test_db"
        self.s3_output_location = "s3://test-bucket/test-output/"
        self.region = "us-west-2"

    def test_has_boto3_constant_exists(self):
        """Test that the has_boto3 constant exists and is True in test environment."""
        from sql_testing_library._adapters.athena import has_boto3

        # has_boto3 should be True in this test environment (since Athena works)
        self.assertTrue(has_boto3)

        # The constant should be boolean
        self.assertIsInstance(has_boto3, bool)

    @mock.patch("boto3.client")
    def test_execute_query_failed_status(self, mock_boto3_client):
        """Test query execution with failed status."""
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        # Mock query execution response
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "failed_query_id"}

        # Mock failed query status with error details
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "SYNTAX_ERROR: line 1:1: mismatched input",
                }
            }
        }

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        with self.assertRaises(Exception) as context:
            adapter.execute_query("INVALID SQL")

        error_message = str(context.exception)
        self.assertIn("Athena query failed with status: FAILED", error_message)
        self.assertIn("Error details: SYNTAX_ERROR", error_message)

    @mock.patch("boto3.client")
    def test_execute_query_cancelled_status(self, mock_boto3_client):
        """Test query execution with cancelled status."""
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        mock_client.start_query_execution.return_value = {"QueryExecutionId": "cancelled_query_id"}

        # Mock cancelled query status with AthenaError
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "CANCELLED",
                    "AthenaError": {
                        "ErrorType": "USER_CANCELLED",
                        "ErrorMessage": "Query was cancelled by user",
                    },
                }
            }
        }

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        with self.assertRaises(Exception) as context:
            adapter.execute_query("SELECT * FROM table")

        error_message = str(context.exception)
        self.assertIn("Athena query failed with status: CANCELLED", error_message)
        self.assertIn("Error details: USER_CANCELLED: Query was cancelled by user", error_message)

    @mock.patch("boto3.client")
    def test_execute_query_timeout(self, mock_boto3_client):
        """Test query execution timeout."""
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        mock_client.start_query_execution.return_value = {"QueryExecutionId": "timeout_query_id"}

        # Mock query that never completes (always RUNNING)
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        # Mock time.sleep to avoid actual waiting
        with mock.patch("time.sleep"):
            # Use a very small max_retries for testing
            with mock.patch.object(adapter, "_wait_for_query_with_error") as mock_wait:
                mock_wait.return_value = (
                    "TIMEOUT",
                    "Query execution timed out after waiting for completion",
                )

                with self.assertRaises(Exception) as context:
                    adapter.execute_query("SELECT * FROM table")

                error_message = str(context.exception)
                self.assertIn("Athena query failed with status: TIMEOUT", error_message)

    @mock.patch("boto3.client")
    def test_execute_query_empty_results(self, mock_boto3_client):
        """Test query execution with empty results."""
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        mock_client.start_query_execution.return_value = {"QueryExecutionId": "empty_query_id"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        # Test case 1: No ResultSet at all
        mock_client.get_query_results.return_value = {}

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        result_df = adapter.execute_query("SELECT COUNT(*) FROM empty_table")
        self.assertTrue(result_df.empty)

        # Test case 2: ResultSet exists but no Rows
        mock_client.get_query_results.return_value = {"ResultSet": {}}
        result_df = adapter.execute_query("SELECT COUNT(*) FROM empty_table")
        self.assertTrue(result_df.empty)

        # Test case 3: ResultSet and Rows exist but empty
        mock_client.get_query_results.return_value = {"ResultSet": {"Rows": []}}
        result_df = adapter.execute_query("SELECT COUNT(*) FROM empty_table")
        self.assertTrue(result_df.empty)

    @mock.patch("boto3.client")
    def test_cleanup_temp_tables_with_errors(self, mock_boto3_client):
        """Test temp table cleanup with errors."""
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        # Mock execute_query to raise an exception for table cleanup
        def mock_execute_query(query):
            if "DROP TABLE" in query:
                raise Exception("Table does not exist or access denied")
            return pd.DataFrame()

        adapter.execute_query = mock_execute_query

        # Test that cleanup handles errors gracefully (should not raise)
        with mock.patch("logging.warning") as mock_warning:
            table_names = ["test_db.temp_table1", "test_db.temp_table2"]
            adapter.cleanup_temp_tables(table_names)  # Should not raise

            # Verify warnings were logged
            self.assertEqual(mock_warning.call_count, 2)
            mock_warning.assert_any_call(
                "Warning: Failed to drop table test_db.temp_table1: "
                "Table does not exist or access denied"
            )
            mock_warning.assert_any_call(
                "Warning: Failed to drop table test_db.temp_table2: "
                "Table does not exist or access denied"
            )

    @mock.patch("boto3.client")
    def test_cleanup_temp_tables_without_database_prefix(self, mock_boto3_client):
        """Test temp table cleanup with table names without database prefix."""
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        # Set up successful query execution
        mock_client.start_query_execution.return_value = {"QueryExecutionId": "cleanup_query_id"}
        mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        mock_client.get_query_results.return_value = {"ResultSet": {"Rows": []}}

        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        # Test cleanup with table names that don't have database prefix
        table_names = ["temp_table1", "temp_table2"]
        adapter.cleanup_temp_tables(table_names)

        # Verify DROP TABLE calls were made correctly
        self.assertEqual(mock_client.start_query_execution.call_count, 2)

        drop_calls = mock_client.start_query_execution.call_args_list
        self.assertIn("DROP TABLE IF EXISTS temp_table1", drop_calls[0][1]["QueryString"])
        self.assertIn("DROP TABLE IF EXISTS temp_table2", drop_calls[1][1]["QueryString"])

    @mock.patch("boto3.client")
    def test_get_type_converter(self, mock_boto3_client):
        """Test getting type converter."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        converter = adapter.get_type_converter()
        self.assertIsInstance(converter, AthenaTypeConverter)

    @mock.patch("boto3.client")
    def test_get_query_size_limit(self, mock_boto3_client):
        """Test getting query size limit."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        limit = adapter.get_query_size_limit()
        self.assertEqual(limit, 256 * 1024)  # 256KB

    @mock.patch("boto3.client")
    def test_build_s3_location(self, mock_boto3_client):
        """Test S3 location building."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        # Test with trailing slash in s3_output_location
        location = adapter._build_s3_location("test_table")
        self.assertEqual(location, "s3://test-bucket/test-output/test_table/")

        # Test with s3_output_location without trailing slash
        adapter.s3_output_location = "s3://test-bucket/test-output"
        location = adapter._build_s3_location("test_table")
        self.assertEqual(location, "s3://test-bucket/test-output/test_table/")

    @mock.patch("boto3.client")
    def test_generate_ctas_sql_empty_table(self, mock_boto3_client):
        """Test CTAS SQL generation for empty tables."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        @dataclass
        class EmptyUser:
            id: int
            name: str
            email: Optional[str]
            active: bool
            created_at: date
            score: float
            balance: Decimal

        class EmptyUserMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "empty_users"

        # Create empty mock table
        empty_mock_table = EmptyUserMockTable([])

        ctas_sql = adapter._generate_ctas_sql("temp_empty_users_123", empty_mock_table)

        # Should create external table with empty schema (no columns since no data to infer from)
        self.assertIn("CREATE EXTERNAL TABLE", ctas_sql)
        self.assertIn("temp_empty_users_123", ctas_sql)
        self.assertIn("STORED AS PARQUET", ctas_sql)
        self.assertIn("LOCATION", ctas_sql)

        # Should not contain data values
        self.assertNotIn("UNION ALL", ctas_sql)
        self.assertNotIn("AS SELECT", ctas_sql)  # No AS SELECT for empty tables

    @mock.patch("boto3.client")
    def test_generate_ctas_sql_with_data(self, mock_boto3_client):
        """Test CTAS SQL generation for tables with data."""
        adapter = AthenaAdapter(
            database=self.database,
            s3_output_location=self.s3_output_location,
        )

        @dataclass
        class User:
            id: int
            name: str
            email: Optional[str]
            active: bool
            created_at: date
            score: float
            balance: Decimal

        class UserMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "users"

        # Create mock table with data
        mock_table = UserMockTable(
            [
                User(
                    1,
                    "Alice",
                    "alice@example.com",
                    True,
                    date(2023, 1, 1),
                    95.5,
                    Decimal("100.50"),
                ),
                User(2, "Bob", None, False, date(2023, 1, 2), 87.2, Decimal("50.25")),
            ]
        )

        ctas_sql = adapter._generate_ctas_sql("temp_users_123", mock_table)

        # Should create table with data using AS SELECT
        self.assertIn("CREATE TABLE", ctas_sql)
        self.assertIn("temp_users_123", ctas_sql)
        self.assertIn("AS", ctas_sql)
        self.assertIn("SELECT", ctas_sql)
        self.assertIn("UNION ALL", ctas_sql)  # Multiple rows

        # Should contain data values
        self.assertIn("1", ctas_sql)
        self.assertIn("'Alice'", ctas_sql)
        self.assertIn("'alice@example.com'", ctas_sql)
        self.assertIn("TRUE", ctas_sql)
        self.assertIn("FALSE", ctas_sql)
        self.assertIn("DATE '2023-01-01'", ctas_sql)
        self.assertIn("95.5", ctas_sql)
        self.assertIn("100.50", ctas_sql)

        # Should handle NULL for Optional fields
        self.assertIn("CAST(NULL AS VARCHAR)", ctas_sql)  # For None email


class TestAthenaTypeConverterCoverage(unittest.TestCase):
    """Additional tests for AthenaTypeConverter to improve coverage."""

    def test_athena_null_string_conversion(self):
        """Test Athena-specific NULL string handling."""
        converter = AthenaTypeConverter()

        # Test Athena NULL string (different from Python None)
        self.assertIsNone(converter.convert("NULL", str))
        self.assertIsNone(converter.convert("NULL", int))
        self.assertIsNone(converter.convert("NULL", float))
        self.assertIsNone(converter.convert("NULL", bool))

        # Test normal values still work
        self.assertEqual(converter.convert("test", str), "test")
        self.assertEqual(converter.convert("123", int), 123)
        self.assertEqual(converter.convert("true", bool), True)

    def test_base_converter_functionality(self):
        """Test that base converter functionality still works."""
        converter = AthenaTypeConverter()

        # Test inherited functionality from BaseTypeConverter
        self.assertEqual(converter.convert("123.45", float), 123.45)
        self.assertEqual(converter.convert("2023-01-15", date), date(2023, 1, 15))
        self.assertEqual(
            converter.convert("2023-01-15T10:30:45", datetime),
            datetime(2023, 1, 15, 10, 30, 45),
        )
        self.assertEqual(converter.convert("123.45", Decimal), Decimal("123.45"))

        # Test boolean conversions
        self.assertTrue(converter.convert("1", bool))
        self.assertTrue(converter.convert("yes", bool))
        self.assertFalse(converter.convert("0", bool))
        self.assertFalse(converter.convert("no", bool))


if __name__ == "__main__":
    unittest.main()
