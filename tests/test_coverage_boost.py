"""Simple tests to boost coverage for import statements."""

import unittest


class TestCoverageBoost(unittest.TestCase):
    """Simple tests to increase coverage for import-heavy modules."""

    def test_main_init_imports(self):
        """Test main __init__.py imports."""
        # This will execute all the import statements in __init__.py
        import sql_testing_library

        # Test some basic attributes exist
        self.assertTrue(hasattr(sql_testing_library, "__version__"))
        self.assertTrue(hasattr(sql_testing_library, "__all__"))

        # Test that main classes are importable
        from sql_testing_library import BaseMockTable, SQLTestCase, sql_test

        self.assertIsNotNone(SQLTestCase)
        self.assertIsNotNone(sql_test)
        self.assertIsNotNone(BaseMockTable)

        # Test backward compatibility
        from sql_testing_library import TestCase

        self.assertIs(TestCase, SQLTestCase)

    def test_adapters_init_imports(self):
        """Test adapters __init__.py imports."""
        # This will execute all the conditional imports

        # Test __all__ is populated
        from sql_testing_library._adapters import __all__

        self.assertIsInstance(__all__, list)

        # Try importing some adapters that should be available
        try:
            from sql_testing_library._adapters import BigQueryAdapter

            self.assertIsNotNone(BigQueryAdapter)
        except ImportError:
            pass  # BigQuery not available in this environment

        try:
            from sql_testing_library._adapters import AthenaAdapter

            self.assertIsNotNone(AthenaAdapter)
        except ImportError:
            pass  # Athena not available in this environment

    def test_exception_classes_basic(self):
        """Test basic exception class instantiation."""
        from sql_testing_library._exceptions import (
            MockTableNotFoundError,
            QuerySizeLimitExceeded,
            SQLParseError,
            SQLTestingError,
            TypeConversionError,
        )

        # Test basic instantiation to cover __init__ methods
        base_error = SQLTestingError("test message")
        self.assertIsInstance(base_error, Exception)

        mock_error = MockTableNotFoundError("table", ["mock1"])
        self.assertEqual(mock_error.qualified_table_name, "table")

        parse_error = SQLParseError("SELECT", "error")
        self.assertEqual(parse_error.query, "SELECT")

        size_error = QuerySizeLimitExceeded(100, 50, "adapter")
        self.assertEqual(size_error.actual_size, 100)

        type_error = TypeConversionError("value", str, "col")
        self.assertEqual(type_error.value, "value")

    def test_type_converter_basic(self):
        """Test basic type converter functionality."""
        from sql_testing_library._types import BaseTypeConverter

        converter = BaseTypeConverter()

        # Test basic conversions to cover method bodies
        self.assertEqual(converter.convert("test", str), "test")
        self.assertEqual(converter.convert(42, int), 42)
        self.assertEqual(converter.convert(3.14, float), 3.14)
        self.assertEqual(converter.convert(True, bool), True)

        # Test None handling
        self.assertIsNone(converter.convert(None, str))

    def test_mock_table_basic(self):
        """Test basic mock table functionality."""
        from sql_testing_library._mock_table import BaseMockTable

        class SimpleMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "test_table"

        # Test with simple data
        table = SimpleMockTable([{"id": 1, "name": "test"}])
        self.assertEqual(len(table.data), 1)
        self.assertEqual(table.get_database_name(), "test_db")
        self.assertEqual(table.get_table_name(), "test_table")
        self.assertEqual(table.get_qualified_name(), "test_db.test_table")

        # Test DataFrame conversion
        df = table.to_dataframe()
        self.assertEqual(len(df), 1)


if __name__ == "__main__":
    unittest.main()
