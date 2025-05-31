"""Additional tests for Trino adapter to improve coverage."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from unittest import mock

from sql_testing_library._adapters.trino import TrinoAdapter, TrinoTypeConverter
from sql_testing_library._mock_table import BaseMockTable


class TestTrinoAdapterCoverageBoost(unittest.TestCase):
    """Additional tests to boost Trino adapter coverage."""

    def setUp(self):
        """Set up common test data."""
        self.host = "trino-host.example.com"
        self.port = 8080
        self.user = "test_user"
        self.catalog = "memory"
        self.schema = "default"
        self.http_scheme = "http"

    def test_has_trino_constant_exists(self):
        """Test that the HAS_TRINO constant exists and is True in test environment."""
        from sql_testing_library._adapters.trino import HAS_TRINO

        # HAS_TRINO should be True in this test environment (since Trino works)
        self.assertTrue(HAS_TRINO)

        # The constant should be boolean
        self.assertIsInstance(HAS_TRINO, bool)

    @mock.patch("trino.dbapi.connect")
    def test_initialization_with_auth(self, mock_trino_connect):
        """Test adapter initialization with authentication."""
        auth = {"type": "basic", "user": "test_user", "password": "test_pass"}

        adapter = TrinoAdapter(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme=self.http_scheme,
            auth=auth,
        )

        # Check properties are set correctly
        self.assertEqual(adapter.host, self.host)
        self.assertEqual(adapter.port, self.port)
        self.assertEqual(adapter.user, self.user)
        self.assertEqual(adapter.catalog, self.catalog)
        self.assertEqual(adapter.schema, self.schema)
        self.assertEqual(adapter.http_scheme, self.http_scheme)
        self.assertEqual(adapter.auth, auth)

    @mock.patch("trino.dbapi.connect")
    def test_get_sqlglot_dialect(self, mock_trino_connect):
        """Test getting sqlglot dialect."""
        adapter = TrinoAdapter(host=self.host)
        self.assertEqual(adapter.get_sqlglot_dialect(), "trino")

    @mock.patch("trino.dbapi.connect")
    def test_get_type_converter(self, mock_trino_connect):
        """Test getting type converter."""
        adapter = TrinoAdapter(host=self.host)

        converter = adapter.get_type_converter()
        self.assertIsInstance(converter, TrinoTypeConverter)

    @mock.patch("trino.dbapi.connect")
    def test_get_query_size_limit(self, mock_trino_connect):
        """Test getting query size limit."""
        adapter = TrinoAdapter(host=self.host)

        limit = adapter.get_query_size_limit()
        self.assertEqual(limit, 16 * 1024 * 1024)  # 16MB

    @mock.patch("trino.dbapi.connect")
    def test_cleanup_temp_tables(self, mock_trino_connect):
        """Test temp table cleanup."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(host=self.host)

        # Test cleanup_temp_tables - should execute DROP TABLE statements
        table_names = ["catalog.schema.temp_table1", "catalog.schema.temp_table2"]
        adapter.cleanup_temp_tables(table_names)

        # Verify DROP TABLE was executed for each table
        self.assertEqual(mock_cursor.execute.call_count, 2)

    @mock.patch("trino.dbapi.connect")
    def test_connection_with_auth_params(self, mock_trino_connect):
        """Test connection creation with auth parameters."""
        mock_conn = mock.MagicMock()
        mock_trino_connect.return_value = mock_conn

        auth = {"type": "basic", "user": "test_user", "password": "test_pass"}
        adapter = TrinoAdapter(
            host=self.host,
            port=self.port,
            user=self.user,
            auth=auth,
        )

        conn = adapter._get_connection()

        # Verify connect was called with correct parameters
        mock_trino_connect.assert_called_with(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog="memory",  # default
            schema="default",  # default
            http_scheme="http",  # default
            auth=auth,
        )
        self.assertEqual(conn, mock_conn)

    @mock.patch("trino.dbapi.connect")
    def test_connection_without_auth(self, mock_trino_connect):
        """Test connection creation without auth."""
        mock_conn = mock.MagicMock()
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(host=self.host)

        conn = adapter._get_connection()

        # Verify connect was called without auth
        mock_trino_connect.assert_called_with(
            host=self.host,
            port=8080,  # default
            user=None,  # default
            catalog="memory",  # default
            schema="default",  # default
            http_scheme="http",  # default
            auth=None,  # default
        )
        self.assertEqual(conn, mock_conn)

    @mock.patch("trino.dbapi.connect")
    def test_execute_query_error_handling(self, mock_trino_connect):
        """Test query execution error handling."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        # Test with cursor execution error
        mock_cursor.execute.side_effect = Exception("SQL syntax error")

        adapter = TrinoAdapter(host=self.host)

        with self.assertRaises(Exception) as context:
            adapter.execute_query("INVALID SQL")

        self.assertIn("SQL syntax error", str(context.exception))

    @mock.patch("trino.dbapi.connect")
    def test_format_value_for_cte_edge_cases(self, mock_trino_connect):
        """Test value formatting edge cases for CTEs."""
        adapter = TrinoAdapter(host=self.host)

        # Test array formatting
        test_array = ["hello", "world", "trino"]
        result = adapter.format_value_for_cte(test_array, List[str])
        self.assertIn("ARRAY", result)
        self.assertIn("'hello'", result)
        self.assertIn("'world'", result)

        # Test integer array
        int_array = [1, 2, 3, 42]
        result = adapter.format_value_for_cte(int_array, List[int])
        self.assertIn("ARRAY", result)
        self.assertIn("1, 2, 3, 42", result)

        # Test empty array
        empty_array = []
        result = adapter.format_value_for_cte(empty_array, List[str])
        self.assertIn("ARRAY[]", result)

        # Test None value with different types
        self.assertEqual(adapter.format_value_for_cte(None, str), "CAST(NULL AS VARCHAR)")
        self.assertEqual(adapter.format_value_for_cte(None, int), "CAST(NULL AS BIGINT)")
        self.assertEqual(adapter.format_value_for_cte(None, float), "CAST(NULL AS DOUBLE)")
        self.assertEqual(adapter.format_value_for_cte(None, bool), "CAST(NULL AS BOOLEAN)")
        self.assertEqual(adapter.format_value_for_cte(None, date), "CAST(NULL AS DATE)")
        self.assertEqual(adapter.format_value_for_cte(None, datetime), "CAST(NULL AS TIMESTAMP)")
        self.assertEqual(adapter.format_value_for_cte(None, Decimal), "CAST(NULL AS DECIMAL(38,9))")

    @mock.patch("trino.dbapi.connect")
    def test_connection_reuse(self, mock_trino_connect):
        """Test that connections are reused properly."""
        mock_conn = mock.MagicMock()
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(host=self.host)

        # First call should create connection
        conn1 = adapter._get_connection()
        self.assertEqual(conn1, mock_conn)

        # Second call should reuse connection
        conn2 = adapter._get_connection()
        self.assertEqual(conn2, mock_conn)

        # Connection should only be created once
        mock_trino_connect.assert_called_once()

    @mock.patch("trino.dbapi.connect")
    def test_cleanup_temp_tables_with_errors(self, mock_trino_connect):
        """Test temp table cleanup with errors."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        # Mock execute to raise an exception
        mock_cursor.execute.side_effect = Exception("Table does not exist")

        adapter = TrinoAdapter(host=self.host)

        # Test that cleanup handles errors gracefully (should not raise)
        with mock.patch("logging.warning") as mock_warning:
            table_names = ["temp_table1", "temp_table2"]
            adapter.cleanup_temp_tables(table_names)  # Should not raise

            # Verify warnings were logged
            self.assertEqual(mock_warning.call_count, 2)

    @mock.patch("trino.dbapi.connect")
    def test_create_temp_table_coverage(self, mock_trino_connect):
        """Test create_temp_table method coverage."""
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino_connect.return_value = mock_conn

        adapter = TrinoAdapter(host=self.host)

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

        # Verify table was created
        self.assertTrue(result.startswith(f"{adapter.catalog}.{adapter.schema}.temp_"))
        mock_cursor.execute.assert_called_once()


class TestTrinoTypeConverterCoverage(unittest.TestCase):
    """Additional tests for TrinoTypeConverter to improve coverage."""

    def test_base_converter_functionality(self):
        """Test that base converter functionality works through Trino converter."""
        converter = TrinoTypeConverter()

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

    def test_array_conversions(self):
        """Test array conversion handling."""
        converter = TrinoTypeConverter()

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
        converter = TrinoTypeConverter()

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
