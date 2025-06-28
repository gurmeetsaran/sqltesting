"""Additional tests for Redshift adapter to improve coverage."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from unittest import mock

from sql_testing_library._adapters.redshift import RedshiftAdapter, RedshiftTypeConverter
from sql_testing_library._mock_table import BaseMockTable


class TestRedshiftAdapterCoverageBoost(unittest.TestCase):
    """Additional tests to boost Redshift adapter coverage."""

    def setUp(self):
        """Set up common test data."""
        self.host = "redshift-host.example.com"
        self.database = "test_db"
        self.user = "test_user"
        self.password = "test_password"
        self.port = 5439

    def test_has_psycopg2_constant_exists(self):
        """Test that the has_psycopg2 constant exists and is True in test environment."""
        from sql_testing_library._adapters.redshift import has_psycopg2

        # has_psycopg2 should be True in this test environment (since Redshift works)
        self.assertTrue(has_psycopg2)

        # The constant should be boolean
        self.assertIsInstance(has_psycopg2, bool)

    @mock.patch("psycopg2.connect")
    def test_get_sqlglot_dialect(self, mock_psycopg2_connect):
        """Test getting sqlglot dialect."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        self.assertEqual(adapter.get_sqlglot_dialect(), "redshift")

    @mock.patch("psycopg2.connect")
    def test_get_type_converter(self, mock_psycopg2_connect):
        """Test getting type converter."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        converter = adapter.get_type_converter()
        self.assertIsInstance(converter, RedshiftTypeConverter)

    @mock.patch("psycopg2.connect")
    def test_get_query_size_limit(self, mock_psycopg2_connect):
        """Test getting query size limit."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        limit = adapter.get_query_size_limit()
        self.assertEqual(limit, 16 * 1024 * 1024)  # 16MB

    @mock.patch("psycopg2.connect")
    def test_cleanup_temp_tables(self, mock_psycopg2_connect):
        """Test temp table cleanup (should do nothing for Redshift)."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        # Test cleanup_temp_tables - should do nothing since Redshift
        # temporary tables are automatically dropped at the end of the session
        table_names = ["temp_table1", "temp_table2"]
        adapter.cleanup_temp_tables(table_names)  # Should not raise

    @mock.patch("psycopg2.connect")
    def test_generate_ctas_sql_empty_table(self, mock_psycopg2_connect):
        """Test CTAS SQL generation for empty tables."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
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

        # Should create regular table with empty schema
        self.assertIn("CREATE TABLE", ctas_sql)
        self.assertIn("temp_empty_users_123", ctas_sql)

        # Should not contain data values for empty table
        self.assertNotIn("UNION ALL", ctas_sql)
        self.assertNotIn("AS", ctas_sql)

    @mock.patch("psycopg2.connect")
    def test_generate_ctas_sql_with_data(self, mock_psycopg2_connect):
        """Test CTAS SQL generation for tables with data."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
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

        # Should create regular table with data using AS SELECT
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

        # Should handle arrays as JSON
        self.assertIn("JSON_PARSE", ctas_sql)
        self.assertIn('["python", "sql"]', ctas_sql)
        self.assertIn('["java"]', ctas_sql)

        # Should handle NULL for Optional fields
        self.assertIn("NULL::VARCHAR", ctas_sql)  # For None email

    @mock.patch("psycopg2.connect")
    def test_generate_ctas_sql_with_optional_types(self, mock_psycopg2_connect):
        """Test CTAS SQL generation with Optional type handling."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        @dataclass
        class OptionalUser:
            id: int
            name: Optional[str]
            score: Optional[float]
            active: Optional[bool]
            created_at: Optional[date]
            balance: Optional[Decimal]

        class OptionalUserMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "optional_users"

        # Create empty mock table to test Optional type mapping
        empty_mock_table = OptionalUserMockTable([])

        ctas_sql = adapter._generate_ctas_sql("temp_optional_users_123", empty_mock_table)

        # Should create regular table
        self.assertIn("CREATE TABLE", ctas_sql)
        self.assertIn("temp_optional_users_123", ctas_sql)

    @mock.patch("psycopg2.connect")
    def test_connection_reuse(self, mock_psycopg2_connect):
        """Test that connections are reused properly."""
        mock_conn = mock.MagicMock()
        mock_psycopg2_connect.return_value = mock_conn

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        # First call should create connection
        conn1 = adapter._get_connection()
        mock_psycopg2_connect.assert_called_once()
        self.assertEqual(conn1, mock_conn)

        # Second call should reuse connection
        mock_psycopg2_connect.reset_mock()
        conn2 = adapter._get_connection()
        mock_psycopg2_connect.assert_not_called()  # Should not create new connection
        self.assertEqual(conn2, mock_conn)

    @mock.patch("psycopg2.connect")
    def test_execute_query_error_handling(self, mock_psycopg2_connect):
        """Test query execution error handling."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2_connect.return_value = mock_conn

        # Test with cursor execution error
        mock_cursor.execute.side_effect = Exception("SQL syntax error")

        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        with self.assertRaises(Exception) as context:
            adapter.execute_query("INVALID SQL")

        self.assertIn("SQL syntax error", str(context.exception))

    @mock.patch("psycopg2.connect")
    def test_format_value_for_cte_edge_cases(self, mock_psycopg2_connect):
        """Test value formatting edge cases for CTEs."""
        adapter = RedshiftAdapter(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        # Test array formatting
        test_array = ["hello", "world", "redshift"]
        result = adapter.format_value_for_cte(test_array, List[str])
        self.assertIn("JSON_PARSE", result)
        self.assertIn('"hello"', result)
        self.assertIn('"world"', result)

        # Test integer array
        int_array = [1, 2, 3, 42]
        result = adapter.format_value_for_cte(int_array, List[int])
        self.assertIn("JSON_PARSE", result)
        self.assertIn("[1, 2, 3, 42]", result)

        # Test Decimal array (should convert to float for JSON)
        decimal_array = [Decimal("1.5"), Decimal("2.7")]
        result = adapter.format_value_for_cte(decimal_array, List[Decimal])
        self.assertIn("JSON_PARSE", result)
        self.assertIn("[1.5, 2.7]", result)

        # Test empty array
        empty_array = []
        result = adapter.format_value_for_cte(empty_array, List[str])
        self.assertIn("JSON_PARSE('[]')", result)

        # Test None value with different types
        self.assertEqual(adapter.format_value_for_cte(None, str), "NULL::VARCHAR")
        self.assertEqual(adapter.format_value_for_cte(None, int), "NULL::BIGINT")
        self.assertEqual(adapter.format_value_for_cte(None, float), "NULL::DOUBLE PRECISION")
        self.assertEqual(adapter.format_value_for_cte(None, bool), "NULL::BOOLEAN")
        self.assertEqual(adapter.format_value_for_cte(None, date), "NULL::DATE")
        self.assertEqual(adapter.format_value_for_cte(None, datetime), "NULL::TIMESTAMP")
        self.assertEqual(adapter.format_value_for_cte(None, Decimal), "NULL::DECIMAL(38,9)")

    @mock.patch("psycopg2.connect")
    def test_execute_query_non_select(self, mock_psycopg2_connect):
        """Test non-SELECT query execution."""
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

        query = "CREATE TABLE test_table (id INT)"
        result_df = adapter.execute_query(query)

        # Check cursor calls
        mock_cursor.execute.assert_called_once_with(query)
        mock_conn.commit.assert_called_once()
        mock_cursor.fetchall.assert_not_called()  # Should not fetch for non-SELECT

        # For non-SELECT queries, empty DataFrame should be returned
        self.assertTrue(result_df.empty)


class TestRedshiftTypeConverterCoverage(unittest.TestCase):
    """Additional tests for RedshiftTypeConverter to improve coverage."""

    def test_base_converter_functionality(self):
        """Test that base converter functionality works through Redshift converter."""
        converter = RedshiftTypeConverter()

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

    def test_redshift_specific_conversions(self):
        """Test Redshift-specific type conversions."""
        converter = RedshiftTypeConverter()

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
        converter = RedshiftTypeConverter()

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
        converter = RedshiftTypeConverter()

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
