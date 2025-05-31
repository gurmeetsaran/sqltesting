"""Additional tests for core.py to boost coverage."""

import contextlib
import unittest
from unittest.mock import MagicMock, patch

from sql_testing_library._adapters.base import DatabaseAdapter
from sql_testing_library._core import SQLTestCase, SQLTestFramework
from sql_testing_library._mock_table import BaseMockTable


class MockAdapter(DatabaseAdapter):
    """Mock adapter for testing."""

    def get_sqlglot_dialect(self) -> str:
        return "test"

    def execute_query(self, query: str):
        return MagicMock()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        return "temp_table_123"

    def cleanup_temp_tables(self, table_names):
        pass

    def format_value_for_cte(self, value, column_type) -> str:
        return str(value)

    def get_type_converter(self):
        from sql_testing_library._types import BaseTypeConverter

        return BaseTypeConverter()


class SimpleMockTable(BaseMockTable):
    """Simple mock table for testing."""

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "test_table"


class TestSQLTestCaseExtra(unittest.TestCase):
    """Test SQLTestCase specific functionality."""

    def test_sqltest_case_attributes(self):
        """Test SQLTestCase attributes and __test__ attribute."""
        # Test that __test__ is False to prevent pytest collection
        self.assertFalse(SQLTestCase.__test__)

        # Test basic instantiation
        test_case = SQLTestCase(query="SELECT * FROM test", execution_database="test_db")

        self.assertEqual(test_case.query, "SELECT * FROM test")
        self.assertEqual(test_case.execution_database, "test_db")
        self.assertIsNone(test_case.mock_tables)
        self.assertIsNone(test_case.result_class)
        self.assertFalse(test_case.use_physical_tables)
        self.assertIsNone(test_case.description)
        self.assertIsNone(test_case.adapter_type)


class TestSQLTestFrameworkExtra(unittest.TestCase):
    """Additional tests for SQLTestFramework edge cases."""

    def setUp(self):
        """Set up test framework."""
        self.mock_adapter = MockAdapter()
        self.framework = SQLTestFramework(self.mock_adapter)

    def test_framework_init(self):
        """Test framework initialization."""
        self.assertIs(self.framework.adapter, self.mock_adapter)
        self.assertIsNotNone(self.framework.type_converter)
        self.assertEqual(self.framework.temp_tables, [])

    def test_error_when_missing_mock_tables(self):
        """Test error when mock_tables is None."""
        test_case = SQLTestCase(
            query="SELECT * FROM test",
            execution_database="test_db",
            mock_tables=None,  # This should cause an error
            result_class=dict,
        )

        with self.assertRaises(ValueError) as context:
            self.framework.run_test(test_case)

        self.assertIn("mock_tables must be provided", str(context.exception))

    def test_error_when_missing_result_class(self):
        """Test error when result_class is None."""
        mock_table = SimpleMockTable([{"id": 1, "name": "test"}])

        test_case = SQLTestCase(
            query="SELECT * FROM test",
            execution_database="test_db",
            mock_tables=[mock_table],
            result_class=None,  # This should cause an error
        )

        with self.assertRaises(ValueError) as context:
            self.framework.run_test(test_case)

        self.assertIn("result_class must be provided", str(context.exception))

    @patch("sql_testing_library._core.sqlglot")
    def test_sql_parsing_error(self, mock_sqlglot):
        """Test SQL parsing error handling."""
        from sql_testing_library._exceptions import SQLParseError

        # Mock sqlglot to raise an exception
        mock_sqlglot.parse_one.side_effect = Exception("Parse error")

        mock_table = SimpleMockTable([{"id": 1, "name": "test"}])

        test_case = SQLTestCase(
            query="INVALID SQL",
            execution_database="test_db",
            mock_tables=[mock_table],
            result_class=dict,
        )

        with self.assertRaises(SQLParseError):
            self.framework.run_test(test_case)

    def test_cleanup_finally_called(self):
        """Test that cleanup is called even when errors occur."""
        mock_table = SimpleMockTable([{"id": 1, "name": "test"}])

        # Add some temp tables to the framework
        self.framework.temp_tables = ["temp_table_1", "temp_table_2"]

        test_case = SQLTestCase(
            query="SELECT * FROM test",
            execution_database="test_db",
            mock_tables=[mock_table],
            result_class=None,  # This will cause an error
        )

        # Mock the cleanup method to track if it's called
        original_cleanup = self.framework.adapter.cleanup_temp_tables
        cleanup_called = []

        def mock_cleanup(tables):
            cleanup_called.append(tables)
            return original_cleanup(tables)

        self.framework.adapter.cleanup_temp_tables = mock_cleanup

        # Run test that should fail
        with self.assertRaises(ValueError):
            self.framework.run_test(test_case)

        # Verify cleanup was called even though test failed
        self.assertTrue(len(cleanup_called) > 0)
        self.assertEqual(self.framework.temp_tables, [])


class TestSQLTestFrameworkCTEHandling(unittest.TestCase):
    """Test CTE handling edge cases."""

    def setUp(self):
        """Set up test framework."""
        self.mock_adapter = MockAdapter()
        self.framework = SQLTestFramework(self.mock_adapter)

    def test_query_with_existing_with_clause(self):
        """Test handling query that already has WITH clause."""
        mock_table = SimpleMockTable([{"id": 1, "name": "test"}])

        # Mock the necessary methods
        with contextlib.ExitStack() as stack:
            mock_parse = stack.enter_context(patch.object(self.framework, "_parse_sql_tables"))
            mock_resolve = stack.enter_context(patch.object(self.framework, "_resolve_table_names"))
            stack.enter_context(patch.object(self.framework, "_validate_mock_tables"))
            mock_mapping = stack.enter_context(
                patch.object(self.framework, "_create_table_mapping")
            )
            mock_cte = stack.enter_context(patch.object(self.framework, "_generate_cte_query"))
            mock_deserialize = stack.enter_context(
                patch.object(self.framework, "_deserialize_results")
            )
            mock_execute = stack.enter_context(
                patch.object(self.framework.adapter, "execute_query")
            )
            mock_parse.return_value = ["test_table"]
            mock_resolve.return_value = ["test_db.test_table"]
            mock_mapping.return_value = {"test_db.test_table": mock_table}
            mock_cte.return_value = "WITH existing AS (SELECT 1) SELECT * FROM test"
            mock_execute.return_value = MagicMock()
            mock_deserialize.return_value = [{"id": 1}]

            test_case = SQLTestCase(
                query="WITH existing AS (SELECT 1) SELECT * FROM test",
                execution_database="test_db",
                mock_tables=[mock_table],
                result_class=dict,
            )

            result = self.framework.run_test(test_case)
            self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
