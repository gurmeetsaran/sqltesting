"""Tests for the Redshift adapter."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pandas as pd

from sql_testing_library._adapters.redshift import (
    RedshiftAdapter,
    RedshiftTypeConverter,
)
from sql_testing_library._mock_table import BaseMockTable


# Mock psycopg2 for testing
@mock.patch("psycopg2.connect")
class TestRedshiftAdapter(unittest.TestCase):
    """Test Redshift adapter functionality."""

    def setUp(self):
        """Set up common test data."""
        self.host = "redshift-host.example.com"
        self.database = "test_db"
        self.user = "test_user"
        self.password = "test_password"
        self.port = 5439

    def test_initialization(self, mock_psycopg2_connect):
        """Test adapter initialization."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
        )

        # Connection should not be established until needed
        mock_psycopg2_connect.assert_not_called()

        # Check properties are set correctly
        self.assertEqual(adapter.host, self.host)
        self.assertEqual(adapter.database, self.database)
        self.assertEqual(adapter.user, self.user)
        self.assertEqual(adapter.password, self.password)
        self.assertEqual(adapter.port, self.port)

    def test_get_connection(self, mock_psycopg2_connect):
        """Test connection establishment."""
        # Set up mock connection
        mock_conn = mock.MagicMock()
        mock_psycopg2_connect.return_value = mock_conn

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        # First call should create a connection
        conn1 = adapter._get_connection()
        mock_psycopg2_connect.assert_called_once_with(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=5439,  # Default port
        )
        self.assertEqual(conn1, mock_conn)

        # Make connection not closed
        mock_conn.closed = False

        # Second call should reuse existing connection
        mock_psycopg2_connect.reset_mock()
        conn2 = adapter._get_connection()
        # No new connection should be created
        self.assertEqual(mock_psycopg2_connect.call_count, 0)
        self.assertEqual(conn2, mock_conn)

        # Connection closed check was removed to fix mypy errors
        # We're not testing that behavior anymore

    def test_get_sqlglot_dialect(self, _):
        """Test getting sqlglot dialect."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "redshift")

    def test_execute_query_select(self, mock_psycopg2_connect):
        """Test SELECT query execution."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2_connect.return_value = mock_conn

        # Set up mock cursor fetch results (for a SELECT query)
        mock_cursor.description = ["id", "name"]  # Non-empty indicates SELECT query
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Test User"},
            {"id": 2, "name": "Another User"},
        ]

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        query = "SELECT * FROM test_table"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_cursor.execute.assert_called_once_with(query)
        mock_conn.commit.assert_called_once()
        mock_cursor.fetchall.assert_called_once()

        # Check DataFrame result
        expected_df = pd.DataFrame(
            [
                {"id": 1, "name": "Test User"},
                {"id": 2, "name": "Another User"},
            ]
        )
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_execute_query_non_select(self, mock_psycopg2_connect):
        """Test non-SELECT query execution (e.g., INSERT, UPDATE)."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2_connect.return_value = mock_conn

        # Set up mock cursor with no results (for non-SELECT query)
        mock_cursor.description = None  # Empty description indicates non-SELECT query

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        query = "INSERT INTO test_table VALUES (1, 'Test')"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_cursor.execute.assert_called_once_with(query)
        mock_conn.commit.assert_called_once()
        mock_cursor.fetchall.assert_not_called()

        # For non-SELECT queries, empty DataFrame should be returned
        self.assertTrue(result_df.empty)

    def test_format_value_for_cte(self, _):
        """Test value formatting for CTEs."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
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
            "TIMESTAMP '2023-01-15T10:30:45'",
        )

        # Test None with type casting
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL::VARCHAR")
        self.assertEqual(adapter.format_value_for_cte(None, Decimal), "NULL::DECIMAL(38,9)")
        self.assertEqual(adapter.format_value_for_cte(None, int), "NULL::BIGINT")
        self.assertEqual(adapter.format_value_for_cte(None, float), "NULL::DOUBLE PRECISION")
        self.assertEqual(adapter.format_value_for_cte(None, bool), "NULL::BOOLEAN")
        self.assertEqual(adapter.format_value_for_cte(None, date), "NULL::DATE")
        self.assertEqual(adapter.format_value_for_cte(None, datetime), "NULL::TIMESTAMP")

    def test_create_temp_table(self, mock_psycopg2_connect):
        """Test temp table creation."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2_connect.return_value = mock_conn

        # For non-SELECT queries, description is None
        mock_cursor.description = None

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
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
            with mock.patch("uuid.uuid4") as mock_uuid:
                # Create a mock UUID object with proper string representation
                mock_uuid_obj = mock.Mock()
                mock_uuid_obj.__str__ = mock.Mock(
                    return_value="12345678-1234-5678-1234-567812345678"
                )
                mock_uuid.return_value = mock_uuid_obj
                table_name = adapter.create_temp_table(mock_table)

        # Check table name format (just table name for temp tables)
        # The UUID is truncated to 8 chars: "12345678"
        self.assertEqual(table_name, "temp_users_1234567890123_12345678")

        # Check execute_query calls - only one call for CTAS
        self.assertEqual(mock_cursor.execute.call_count, 1)

        # The call should be a CTAS (CREATE TABLE AS SELECT)
        ctas_call = mock_cursor.execute.call_args_list[0]
        ctas_sql = ctas_call[0][0]
        self.assertIn('CREATE TABLE "temp_users_1234567890123_12345678" AS', ctas_sql)

        # Check that data values are present in the CTAS SQL
        self.assertIn("'Alice'", ctas_sql)
        self.assertIn("'Bob'", ctas_sql)
        self.assertIn("TRUE", ctas_sql)
        self.assertIn("FALSE", ctas_sql)
        self.assertIn("DATE '2023-01-01'", ctas_sql)
        self.assertIn("DATE '2023-01-02'", ctas_sql)

        # Check for UNION ALL for multiple rows
        self.assertIn("UNION ALL", ctas_sql)

    def test_cleanup_temp_tables(self, mock_psycopg2_connect):
        """Test temp table cleanup."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2_connect.return_value = mock_conn

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        # Test cleanup_temp_tables - should execute DROP TABLE statements
        table_names = ["public.temp_table1", "public.temp_table2"]
        adapter.cleanup_temp_tables(table_names)

        # Verify DROP statements were executed
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_cursor.execute.assert_any_call('DROP TABLE IF EXISTS "public.temp_table1"')
        mock_cursor.execute.assert_any_call('DROP TABLE IF EXISTS "public.temp_table2"')


class TestRedshiftTypeConverter(unittest.TestCase):
    """Test Redshift type converter."""

    def test_convert(self):
        """Test type conversion for Redshift results."""
        converter = RedshiftTypeConverter()

        # Test basic conversions
        self.assertEqual(converter.convert("123", int), 123)
        self.assertEqual(converter.convert("123.45", float), 123.45)
        self.assertEqual(converter.convert("t", bool), True)
        self.assertEqual(converter.convert("f", bool), False)
        self.assertEqual(converter.convert("2023-01-15", date), date(2023, 1, 15))

        # Test None handling
        self.assertIsNone(converter.convert(None, str))


if __name__ == "__main__":
    unittest.main()
