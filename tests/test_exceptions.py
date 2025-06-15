"""Test exception classes and error handling."""

import unittest
from typing import Optional

from sql_testing_library._exceptions import (
    MockTableNotFoundError,
    QuerySizeLimitExceeded,
    SQLParseError,
    SQLTestingError,
    TypeConversionError,
)


class TestSQLTestingError(unittest.TestCase):
    """Test base SQLTestingError exception."""

    def test_basic_instantiation(self):
        """Test basic error instantiation."""
        error = SQLTestingError("Test error message")

        self.assertEqual(str(error), "Test error message")
        self.assertIsInstance(error, Exception)

    def test_empty_message(self):
        """Test error with empty message."""
        error = SQLTestingError("")

        self.assertEqual(str(error), "")

    def test_none_message(self):
        """Test error with None message."""
        error = SQLTestingError(None)

        self.assertEqual(str(error), "None")

    def test_inheritance(self):
        """Test that it's a proper Exception subclass."""
        error = SQLTestingError("test")

        self.assertIsInstance(error, Exception)
        self.assertIsInstance(error, SQLTestingError)


class TestMockTableNotFoundError(unittest.TestCase):
    """Test MockTableNotFoundError exception."""

    def test_basic_instantiation(self):
        """Test basic error with table name and mock tables."""
        mock_table_names = ["mock1", "mock2", "mock3"]
        error = MockTableNotFoundError("database.table", mock_table_names)

        self.assertEqual(error.qualified_table_name, "database.table")
        self.assertEqual(error.available_mocks, mock_table_names)

        error_str = str(error)
        self.assertIn("Mock table not found: 'database.table'", error_str)
        self.assertIn("Available:", error_str)
        self.assertIn("mock1", error_str)
        self.assertIn("mock2", error_str)
        self.assertIn("mock3", error_str)

    def test_empty_mock_tables(self):
        """Test error with empty mock tables list."""
        error = MockTableNotFoundError("test.table", [])

        self.assertEqual(error.qualified_table_name, "test.table")
        self.assertEqual(error.available_mocks, [])

        error_str = str(error)
        self.assertIn("Mock table not found: 'test.table'", error_str)
        self.assertIn("Available: None", error_str)

    def test_single_mock_table(self):
        """Test error with single mock table."""
        error = MockTableNotFoundError("prod.users", ["users_table"])

        error_str = str(error)
        self.assertIn("Mock table not found: 'prod.users'", error_str)
        self.assertIn("users_table", error_str)

    def test_inheritance(self):
        """Test proper inheritance."""
        error = MockTableNotFoundError("test.table", ["mock"])

        self.assertIsInstance(error, SQLTestingError)
        self.assertIsInstance(error, Exception)


class TestSQLParseError(unittest.TestCase):
    """Test SQLParseError exception."""

    def test_basic_instantiation(self):
        """Test basic SQL parse error."""
        sql = "SELECT * FROM invalid syntax"
        error_msg = "Syntax error at line 1"
        error = SQLParseError(sql, error_msg)

        self.assertEqual(error.query, sql)
        self.assertEqual(error.parse_error, error_msg)

        error_str = str(error)
        self.assertIn("Failed to parse SQL:", error_str)
        self.assertIn(error_msg, error_str)
        # SQL content is not included in the default error message

    def test_multiline_sql(self):
        """Test error with multiline SQL."""
        sql = """
        SELECT
            col1,
            col2
        FROM table1
        WHERE invalid syntax
        """
        error = SQLParseError(sql, "Invalid WHERE clause")

        error_str = str(error)
        self.assertIn("Failed to parse SQL:", error_str)
        self.assertIn("Invalid WHERE clause", error_str)

    def test_empty_sql(self):
        """Test error with empty SQL."""
        error = SQLParseError("", "Empty query")

        self.assertEqual(error.query, "")
        self.assertEqual(error.parse_error, "Empty query")

    def test_none_values(self):
        """Test error with None values."""
        error = SQLParseError(None, None)

        self.assertIsNone(error.query)
        self.assertIsNone(error.parse_error)

    def test_inheritance(self):
        """Test proper inheritance."""
        error = SQLParseError("SELECT", "error")

        self.assertIsInstance(error, SQLTestingError)
        self.assertIsInstance(error, Exception)


class TestQuerySizeLimitExceeded(unittest.TestCase):
    """Test QuerySizeLimitExceeded exception."""

    def test_basic_instantiation(self):
        """Test basic query size limit error."""
        query_size = 1024000
        max_size = 1000000
        adapter_name = "bigquery"
        error = QuerySizeLimitExceeded(query_size, max_size, adapter_name)

        self.assertEqual(error.actual_size, query_size)
        self.assertEqual(error.limit, max_size)
        self.assertEqual(error.adapter_name, adapter_name)

        error_str = str(error)
        self.assertIn("Query size", error_str)
        self.assertIn(str(query_size), error_str)
        self.assertIn(str(max_size), error_str)

    def test_zero_sizes(self):
        """Test with zero sizes."""
        error = QuerySizeLimitExceeded(0, 0, "test")

        self.assertEqual(error.actual_size, 0)
        self.assertEqual(error.limit, 0)

    def test_large_sizes(self):
        """Test with large sizes."""
        query_size = 999999999
        max_size = 500000000
        error = QuerySizeLimitExceeded(query_size, max_size, "athena")

        error_str = str(error)
        self.assertIn(str(query_size), error_str)
        self.assertIn(str(max_size), error_str)

    def test_inheritance(self):
        """Test proper inheritance."""
        error = QuerySizeLimitExceeded(100, 50, "redshift")

        self.assertIsInstance(error, SQLTestingError)
        self.assertIsInstance(error, Exception)


class TestTypeConversionError(unittest.TestCase):
    """Test TypeConversionError exception."""

    def test_basic_instantiation(self):
        """Test basic type conversion error."""
        value = "not_a_number"
        target_type = int
        column_name = "user_id"
        error = TypeConversionError(value, target_type, column_name)

        self.assertEqual(error.value, value)
        self.assertEqual(error.target_type, target_type)
        self.assertEqual(error.column_name, column_name)

        error_str = str(error)
        self.assertIn("Cannot convert", error_str)
        self.assertIn("user_id", error_str)
        self.assertIn("'not_a_number'", error_str)
        self.assertIn("int", error_str)

    def test_with_optional_column_name(self):
        """Test error without column name."""
        error = TypeConversionError("invalid", float, None)

        self.assertEqual(error.value, "invalid")
        self.assertEqual(error.target_type, float)
        self.assertIsNone(error.column_name)

        error_str = str(error)
        self.assertIn("Cannot convert", error_str)
        self.assertIn("'invalid'", error_str)
        self.assertIn("float", error_str)

    def test_complex_types(self):
        """Test with complex types."""
        from datetime import date

        error = TypeConversionError("2023-13-45", Optional[date], "birth_date")

        error_str = str(error)
        self.assertIn("birth_date", error_str)
        self.assertIn("2023-13-45", error_str)
        # The type representation might vary between Python versions
        # Just check that some form of date type is mentioned
        self.assertTrue("date" in error_str.lower() or "typing" in error_str.lower())

    def test_none_value(self):
        """Test with None value."""
        error = TypeConversionError(None, str, "name")

        self.assertIsNone(error.value)
        self.assertEqual(error.target_type, str)
        self.assertEqual(error.column_name, "name")

    def test_inheritance(self):
        """Test proper inheritance."""
        error = TypeConversionError("test", int, "col")

        self.assertIsInstance(error, SQLTestingError)
        self.assertIsInstance(error, Exception)


class TestExceptionChaining(unittest.TestCase):
    """Test exception chaining and context."""

    def test_exception_chaining(self):
        """Test that exceptions can be properly chained."""
        try:
            # Simulate inner exception
            raise ValueError("Original error")
        except ValueError as e:
            # Chain with SQL testing error
            sql_error = SQLParseError("SELECT", "Parse failed")
            sql_error.__cause__ = e

            self.assertIsInstance(sql_error.__cause__, ValueError)
            self.assertEqual(str(sql_error.__cause__), "Original error")

    def test_exception_context(self):
        """Test exception context preservation."""
        with self.assertRaises(TypeConversionError) as context:
            try:
                int("not_a_number")
            except ValueError:
                raise TypeConversionError("not_a_number", int, "test_col")  # noqa: B904

        error = context.exception
        self.assertIsInstance(error, TypeConversionError)
        self.assertEqual(error.column_name, "test_col")


if __name__ == "__main__":
    unittest.main()
