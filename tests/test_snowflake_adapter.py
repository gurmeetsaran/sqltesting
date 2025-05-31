"""Tests for the Snowflake adapter."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

import pandas as pd

from sql_testing_library._adapters.snowflake import (
    SnowflakeAdapter,
    SnowflakeTypeConverter,
)
from sql_testing_library._mock_table import BaseMockTable


# Mock snowflake.connector for testing
@mock.patch("snowflake.connector.connect")
class TestSnowflakeAdapter(unittest.TestCase):
    """Test Snowflake adapter functionality."""

    def setUp(self):
        """Set up common test data."""
        self.account = "test_account"
        self.user = "test_user"
        self.password = "test_password"
        self.database = "test_db"
        self.schema = "test_schema"
        self.warehouse = "test_warehouse"
        self.role = "test_role"

    def test_initialization(self, mock_snowflake_connect):
        """Test adapter initialization."""
        # Test with all parameters
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
        )

        # Force connection to be established
        adapter._get_connection()

        mock_snowflake_connect.assert_called_once_with(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
        )

        # Reset mock
        mock_snowflake_connect.reset_mock()

        # Test with minimal parameters
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # Force connection to be established
        adapter._get_connection()

        mock_snowflake_connect.assert_called_once_with(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema="PUBLIC",  # Default schema
        )

    def test_get_sqlglot_dialect(self, _):
        """Test getting sqlglot dialect."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "snowflake")

    def test_execute_query(self, mock_snowflake_connect):
        """Test query execution."""
        # Mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Set up mock cursor response
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test User")]

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        query = "SELECT * FROM test_table"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchall.assert_called_once()

        # Check DataFrame result
        expected_df = pd.DataFrame([(1, "Test User")], columns=["id", "name"])
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_format_value_for_cte(self, _):
        """Test value formatting for CTEs."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
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

        # Test None
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL")

    def test_create_temp_table(self, mock_snowflake_connect):
        """Test temp table creation."""
        # Mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
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

        self.assertEqual(table_name, "test_schema.TEMP_users_1234567890123")

        # Check that execute_query was called with CTAS
        mock_cursor.execute.assert_called_once()
        ctas_query = mock_cursor.execute.call_args[0][0]

        # Verify it's a CTAS query
        self.assertIn("CREATE TEMPORARY TABLE", ctas_query)
        self.assertIn("AS", ctas_query)

        # Check for data values in the query
        self.assertIn("1", ctas_query)
        self.assertIn("'Alice'", ctas_query)
        self.assertIn("'alice@example.com'", ctas_query)
        self.assertIn("TRUE", ctas_query)
        self.assertIn("DATE '2023-01-01'", ctas_query)

        # Check for UNION ALL for the second row
        self.assertIn("UNION ALL", ctas_query)
        self.assertIn("'Bob'", ctas_query)
        self.assertIn("'bob@example.com'", ctas_query)
        self.assertIn("FALSE", ctas_query)
        self.assertIn("DATE '2023-01-02'", ctas_query)

    def test_cleanup_temp_tables(self, mock_snowflake_connect):
        """Test temp table cleanup."""
        # Mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
        )

        # Test cleanup_temp_tables
        table_names = [
            "test_db.test_schema.temp_table1",
            "test_db.test_schema.temp_table2",
        ]
        adapter.cleanup_temp_tables(table_names)

        # Check that DROP TABLE was called for each table
        self.assertEqual(mock_cursor.execute.call_count, 2)

        drop_call1 = mock_cursor.execute.call_args_list[0]
        self.assertIn(
            'DROP TABLE IF EXISTS "test_db"."test_schema"."temp_table1"',
            drop_call1[0][0],
        )

        drop_call2 = mock_cursor.execute.call_args_list[1]
        self.assertIn(
            'DROP TABLE IF EXISTS "test_db"."test_schema"."temp_table2"',
            drop_call2[0][0],
        )


class TestSnowflakeTypeConverter(unittest.TestCase):
    """Test Snowflake type converter."""

    def test_convert(self):
        """Test type conversion for Snowflake results."""
        converter = SnowflakeTypeConverter()

        # Test basic conversions
        self.assertEqual(converter.convert("123", int), 123)
        self.assertEqual(converter.convert("123.45", float), 123.45)
        self.assertEqual(converter.convert("true", bool), True)
        self.assertEqual(converter.convert("2023-01-15", date), date(2023, 1, 15))

        # Test None handling
        self.assertIsNone(converter.convert(None, str))


if __name__ == "__main__":
    unittest.main()
