"""Additional tests for Snowflake adapter to improve coverage."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from unittest import mock

from sql_testing_library._adapters.snowflake import SnowflakeAdapter, SnowflakeTypeConverter
from sql_testing_library._mock_table import BaseMockTable


class TestSnowflakeAdapterCoverageBoost(unittest.TestCase):
    """Additional tests to boost Snowflake adapter coverage."""

    def setUp(self):
        """Set up common test data."""
        self.account = "test-account"
        self.user = "test_user"
        self.password = "test_password"
        self.database = "test_db"
        self.schema = "public"
        self.warehouse = "test_warehouse"
        self.role = "test_role"

    def test_has_snowflake_constant_exists(self):
        """Test that the has_snowflake constant exists and is True in test environment."""
        from sql_testing_library._adapters.snowflake import has_snowflake

        # has_snowflake should be True in this test environment (since Snowflake works)
        self.assertTrue(has_snowflake)

        # The constant should be boolean
        self.assertIsInstance(has_snowflake, bool)

    @mock.patch("snowflake.connector.connect")
    def test_initialization_without_optional_params(self, mock_snowflake_connect):
        """Test adapter initialization without optional parameters."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # Check properties are set correctly
        self.assertEqual(adapter.account, self.account)
        self.assertEqual(adapter.user, self.user)
        self.assertEqual(adapter.password, self.password)
        self.assertEqual(adapter.database, self.database)
        self.assertEqual(adapter.schema, "PUBLIC")  # Default value
        self.assertIsNone(adapter.warehouse)
        self.assertIsNone(adapter.role)

    @mock.patch("snowflake.connector.connect")
    def test_initialization_with_all_params(self, mock_snowflake_connect):
        """Test adapter initialization with all parameters."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            role=self.role,
        )

        # Check properties are set correctly
        self.assertEqual(adapter.account, self.account)
        self.assertEqual(adapter.user, self.user)
        self.assertEqual(adapter.password, self.password)
        self.assertEqual(adapter.database, self.database)
        self.assertEqual(adapter.schema, self.schema)
        self.assertEqual(adapter.warehouse, self.warehouse)
        self.assertEqual(adapter.role, self.role)

    @mock.patch("snowflake.connector.connect")
    def test_get_connection_with_warehouse_and_role(self, mock_snowflake_connect):
        """Test connection creation with warehouse and role."""
        mock_conn = mock.MagicMock()
        mock_snowflake_connect.return_value = mock_conn

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
        )

        conn = adapter._get_connection()

        mock_snowflake_connect.assert_called_once_with(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema="PUBLIC",
            warehouse=self.warehouse,
            role=self.role,
        )
        self.assertEqual(conn, mock_conn)

    @mock.patch("snowflake.connector.connect")
    def test_get_connection_without_warehouse_and_role(self, mock_snowflake_connect):
        """Test connection creation without warehouse and role."""
        mock_conn = mock.MagicMock()
        mock_snowflake_connect.return_value = mock_conn

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        conn = adapter._get_connection()

        mock_snowflake_connect.assert_called_once_with(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema="PUBLIC",
        )
        self.assertEqual(conn, mock_conn)

    @mock.patch("snowflake.connector.connect")
    def test_get_sqlglot_dialect(self, mock_snowflake_connect):
        """Test getting sqlglot dialect."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "snowflake")

    @mock.patch("snowflake.connector.connect")
    def test_get_type_converter(self, mock_snowflake_connect):
        """Test getting type converter."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        converter = adapter.get_type_converter()
        self.assertIsInstance(converter, SnowflakeTypeConverter)

    @mock.patch("snowflake.connector.connect")
    def test_get_query_size_limit(self, mock_snowflake_connect):
        """Test getting query size limit."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        limit = adapter.get_query_size_limit()
        self.assertEqual(limit, 1024 * 1024)  # 1MB

    @mock.patch("snowflake.connector.connect")
    def test_execute_query_error_handling(self, mock_snowflake_connect):
        """Test query execution error handling."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Test with cursor execution error
        mock_cursor.execute.side_effect = Exception("SQL syntax error")

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        with self.assertRaises(Exception) as context:
            adapter.execute_query("INVALID SQL")

        self.assertIn("SQL syntax error", str(context.exception))

    @mock.patch("snowflake.connector.connect")
    def test_execute_query_non_select(self, mock_snowflake_connect):
        """Test non-SELECT query execution."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Set up mock cursor with no results (for non-SELECT query)
        mock_cursor.description = None  # Empty description indicates non-SELECT query

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        query = "CREATE TABLE test_table (id INT)"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchall.assert_not_called()  # Should not fetch for non-SELECT

        # For non-SELECT queries, empty DataFrame should be returned
        self.assertTrue(result_df.empty)

    @mock.patch("snowflake.connector.connect")
    def test_cleanup_temp_tables_with_errors(self, mock_snowflake_connect):
        """Test temp table cleanup with errors."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # Mock execute to raise an exception for table cleanup
        mock_cursor.execute.side_effect = Exception("Table does not exist")

        # Test that cleanup handles errors gracefully (should not raise)
        with mock.patch("logging.warning") as mock_warning:
            table_names = ["temp_table1", "temp_table2"]
            adapter.cleanup_temp_tables(table_names)  # Should not raise

            # Verify warnings were logged
            self.assertEqual(mock_warning.call_count, 2)
            mock_warning.assert_any_call(
                "Warning: Failed to drop table temp_table1: Table does not exist"
            )
            mock_warning.assert_any_call(
                "Warning: Failed to drop table temp_table2: Table does not exist"
            )

    @mock.patch("snowflake.connector.connect")
    def test_generate_ctas_sql_empty_table(self, mock_snowflake_connect):
        """Test CTAS SQL generation for empty tables."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
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

        # Should create table with empty schema
        self.assertIn("CREATE TEMPORARY TABLE", ctas_sql)
        self.assertIn("temp_empty_users_123", ctas_sql)

        # Should not contain data values for empty table
        self.assertNotIn("UNION ALL", ctas_sql)
        self.assertNotIn("AS", ctas_sql)

    @mock.patch("snowflake.connector.connect")
    def test_generate_ctas_sql_with_data(self, mock_snowflake_connect):
        """Test CTAS SQL generation for tables with data."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
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
            tags: List[str]

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
                    ["python", "sql"],
                ),
                User(2, "Bob", None, False, date(2023, 1, 2), 87.2, Decimal("50.25"), ["java"]),
            ]
        )

        ctas_sql = adapter._generate_ctas_sql("temp_users_123", mock_table)

        # Should create temporary table with data using AS SELECT
        self.assertIn("CREATE TEMPORARY TABLE", ctas_sql)
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

        # Should handle arrays using ARRAY_CONSTRUCT
        self.assertIn("ARRAY_CONSTRUCT", ctas_sql)

        # Should handle NULL for Optional fields
        self.assertIn("NULL", ctas_sql)  # For None email

    @mock.patch("snowflake.connector.connect")
    def test_format_value_for_cte_edge_cases(self, mock_snowflake_connect):
        """Test value formatting edge cases for CTEs."""
        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # Test array formatting
        test_array = ["hello", "world", "snowflake"]
        result = adapter.format_value_for_cte(test_array, List[str])
        self.assertIn("ARRAY_CONSTRUCT", result)
        self.assertIn("'hello'", result)
        self.assertIn("'world'", result)

        # Test integer array
        int_array = [1, 2, 3, 42]
        result = adapter.format_value_for_cte(int_array, List[int])
        self.assertIn("ARRAY_CONSTRUCT", result)
        self.assertIn("1, 2, 3, 42", result)

        # Test empty array
        empty_array = []
        result = adapter.format_value_for_cte(empty_array, List[str])
        self.assertIn("ARRAY_CONSTRUCT()", result)

        # Test None value
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL")

    @mock.patch("snowflake.connector.connect")
    def test_connection_reuse(self, mock_snowflake_connect):
        """Test that connections are reused properly."""
        mock_conn = mock.MagicMock()
        mock_snowflake_connect.return_value = mock_conn

        adapter = SnowflakeAdapter(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # First call should create connection
        conn1 = adapter._get_connection()
        mock_snowflake_connect.assert_called_once()
        self.assertEqual(conn1, mock_conn)

        # Second call should reuse connection
        mock_snowflake_connect.reset_mock()
        conn2 = adapter._get_connection()
        mock_snowflake_connect.assert_not_called()  # Should not create new connection
        self.assertEqual(conn2, mock_conn)


class TestSnowflakeTypeConverterCoverage(unittest.TestCase):
    """Additional tests for SnowflakeTypeConverter to improve coverage."""

    def test_base_converter_functionality(self):
        """Test that base converter functionality works through Snowflake converter."""
        converter = SnowflakeTypeConverter()

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

    def test_snowflake_specific_conversions(self):
        """Test Snowflake-specific type conversions."""
        converter = SnowflakeTypeConverter()

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
        converter = SnowflakeTypeConverter()

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
        converter = SnowflakeTypeConverter()

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
