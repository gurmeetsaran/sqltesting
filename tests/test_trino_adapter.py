"""Tests for the Trino adapter."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pandas as pd

from sql_testing_library.adapters.trino import TrinoAdapter, TrinoTypeConverter
from sql_testing_library.mock_table import BaseMockTable


# Mock trino for testing
@mock.patch("trino.dbapi.connect")
class TestTrinoAdapter(unittest.TestCase):
    """Test Trino adapter functionality."""

    def setUp(self):
        """Set up common test data."""
        self.host = "trino-host.example.com"
        self.port = 8080
        self.user = "test_user"
        self.catalog = "test_catalog"
        self.schema = "test_schema"
        self.http_scheme = "http"
        self.auth = {"type": "basic", "user": "test_user", "password": "test_password"}

    def test_initialization(self, mock_trino_connect):
        """Test adapter initialization."""
        adapter = TrinoAdapter(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme=self.http_scheme,
            auth=self.auth,
        )

        # Connection should be established during initialization
        mock_trino_connect.assert_called_once_with(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme=self.http_scheme,
            auth=self.auth,
        )

        # Check properties are set correctly
        self.assertEqual(adapter.host, self.host)
        self.assertEqual(adapter.port, self.port)
        self.assertEqual(adapter.user, self.user)
        self.assertEqual(adapter.catalog, self.catalog)
        self.assertEqual(adapter.schema, self.schema)
        self.assertEqual(adapter.http_scheme, self.http_scheme)
        self.assertEqual(adapter.auth, self.auth)

    def test_get_connection(self, mock_trino_connect):
        """Test connection establishment."""
        # Set up mock connection
        mock_conn = mock.MagicMock()
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )

        # Reset the mock to clear the call from initialization
        mock_trino_connect.reset_mock()
        adapter.conn = None  # Reset connection for testing

        # First call should create a connection
        conn1 = adapter._get_connection()
        mock_trino_connect.assert_called_once_with(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme="http",  # Default http_scheme
            auth=None,  # No auth by default
        )
        self.assertEqual(conn1, mock_conn)

        # Second call should reuse existing connection
        mock_trino_connect.reset_mock()
        conn2 = adapter._get_connection()
        # No new connection should be created
        self.assertEqual(mock_trino_connect.call_count, 0)
        self.assertEqual(conn2, mock_conn)

    def test_get_sqlglot_dialect(self, _):
        """Test getting sqlglot dialect."""
        adapter = TrinoAdapter(
            host=self.host,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "trino")

    def test_execute_query_select(self, mock_trino_connect):
        """Test SELECT query execution."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        # Set up mock cursor fetch results (for a SELECT query)
        mock_cursor.description = [("id", None), ("name", None)]
        mock_cursor.fetchall.return_value = [(1, "Test User"), (2, "Another User")]

        adapter = TrinoAdapter(
            host=self.host,
            catalog=self.catalog,
            schema=self.schema,
        )

        query = "SELECT * FROM test_table"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchall.assert_called_once()

        # Check DataFrame result
        expected_df = pd.DataFrame([(1, "Test User"), (2, "Another User")], columns=["id", "name"])
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_execute_query_non_select(self, mock_trino_connect):
        """Test non-SELECT query execution (e.g., CREATE, DROP)."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        # Set up mock cursor with no results (for non-SELECT query)
        mock_cursor.description = None  # Empty description indicates non-SELECT query

        adapter = TrinoAdapter(
            host=self.host,
            catalog=self.catalog,
            schema=self.schema,
        )

        query = "CREATE TABLE test_table (id INTEGER, name VARCHAR)"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_cursor.execute.assert_called_once_with(query)

        # For non-SELECT queries, empty DataFrame should be returned
        self.assertTrue(result_df.empty)

    def test_format_value_for_cte(self, _):
        """Test value formatting for CTEs."""
        adapter = TrinoAdapter(
            host=self.host,
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

        # Test None (unified approach uses simple NULL for most dialects)
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL")
        self.assertEqual(adapter.format_value_for_cte(None, Decimal), "NULL")
        self.assertEqual(adapter.format_value_for_cte(None, int), "NULL")
        self.assertEqual(adapter.format_value_for_cte(None, float), "NULL")
        self.assertEqual(adapter.format_value_for_cte(None, bool), "NULL")
        self.assertEqual(adapter.format_value_for_cte(None, date), "NULL")
        self.assertEqual(adapter.format_value_for_cte(None, datetime), "NULL")

    def test_create_temp_table(self, mock_trino_connect):
        """Test temp table creation."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        # For non-SELECT queries, description is None
        mock_cursor.description = None

        adapter = TrinoAdapter(
            host=self.host,
            catalog=self.catalog,
            schema=self.schema,
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

        # Check table name format (catalog.schema.table)
        self.assertEqual(table_name, f"{self.catalog}.{self.schema}.temp_users_1234567890123")

        # Check execute_query calls - only one call for CTAS
        self.assertEqual(mock_cursor.execute.call_count, 1)

        # The call should be a CTAS (CREATE TABLE AS SELECT)
        ctas_call = mock_cursor.execute.call_args_list[0]
        ctas_sql = ctas_call[0][0]

        # Check that the CTAS SQL contains the expected elements
        self.assertIn(f"CREATE TABLE {self.schema}.temp_users_1234567890123", ctas_sql)
        self.assertIn("WITH (format = 'ORC')", ctas_sql)
        self.assertIn("AS SELECT", ctas_sql)

        # Check that data values are present in the CTAS SQL
        self.assertIn("'Alice'", ctas_sql)
        self.assertIn("'Bob'", ctas_sql)
        self.assertIn("TRUE", ctas_sql)
        self.assertIn("FALSE", ctas_sql)
        self.assertIn("DATE '2023-01-01'", ctas_sql)
        self.assertIn("DATE '2023-01-02'", ctas_sql)

        # Check for UNION ALL for multiple rows
        self.assertIn("UNION ALL", ctas_sql)

    def test_create_empty_temp_table(self, mock_trino_connect):
        """Test creating an empty temp table."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(
            host=self.host,
            catalog=self.catalog,
            schema=self.schema,
        )

        # Create a mock table class
        @dataclass
        class User:
            id: int
            name: str
            active: bool

        class EmptyUserMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "empty_users"

            def get_column_types(self) -> dict:
                return {"id": int, "name": str, "active": bool}

        # Create an empty mock table
        empty_mock_table = EmptyUserMockTable([])

        # Test create_temp_table with empty table
        with mock.patch("time.time", return_value=1234567890.123):
            table_name = adapter.create_temp_table(empty_mock_table)

        # Check table name format
        self.assertEqual(table_name, f"{self.catalog}.{self.schema}.temp_empty_users_1234567890123")

        # Check execute_query calls
        self.assertEqual(mock_cursor.execute.call_count, 1)

        # The call should create an empty table with the correct schema
        create_call = mock_cursor.execute.call_args_list[0]
        create_sql = create_call[0][0]

        # Verify the SQL contains the expected elements for creating an empty table
        self.assertIn(f"CREATE TABLE {self.schema}.temp_empty_users_1234567890123", create_sql)
        self.assertIn("WITH (format = 'ORC')", create_sql)
        self.assertIn('"id" BIGINT', create_sql)
        self.assertIn('"name" VARCHAR', create_sql)
        self.assertIn('"active" BOOLEAN', create_sql)

    def test_cleanup_temp_tables(self, mock_trino_connect):
        """Test temp table cleanup."""
        # Set up mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(
            host=self.host,
            catalog=self.catalog,
            schema=self.schema,
        )

        # Test cleanup_temp_tables with various table name formats
        table_names = [
            f"{self.catalog}.{self.schema}.temp_table1",  # Full format
            f"{self.schema}.temp_table2",  # Schema.table format
            "temp_table3",  # Just table name
        ]
        adapter.cleanup_temp_tables(table_names)

        # Verify DROP queries were executed for each table
        self.assertEqual(mock_cursor.execute.call_count, 3)

        # Check the exact DROP statements
        drop_calls = mock_cursor.execute.call_args_list

        # For fully qualified name
        self.assertEqual(
            drop_calls[0][0][0],
            f'DROP TABLE IF EXISTS {self.catalog}.{self.schema}."temp_table1"',
        )

        # For schema.table format
        self.assertEqual(
            drop_calls[1][0][0],
            f'DROP TABLE IF EXISTS {self.catalog}.{self.schema}."temp_table2"',
        )

        # For just table name
        self.assertEqual(
            drop_calls[2][0][0],
            f'DROP TABLE IF EXISTS {self.catalog}.{self.schema}."temp_table3"',
        )


class TestTrinoTypeConverter(unittest.TestCase):
    """Test Trino type converter."""

    def test_convert(self):
        """Test type conversion for Trino results."""
        converter = TrinoTypeConverter()

        # Test basic conversions
        self.assertEqual(converter.convert("123", int), 123)
        self.assertEqual(converter.convert("123.45", float), 123.45)
        self.assertEqual(converter.convert(True, bool), True)
        self.assertEqual(converter.convert(False, bool), False)
        self.assertEqual(converter.convert("2023-01-15", date), date(2023, 1, 15))

        # Test None handling
        self.assertIsNone(converter.convert(None, str))


if __name__ == "__main__":
    unittest.main()
