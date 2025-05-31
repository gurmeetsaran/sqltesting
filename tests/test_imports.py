"""Test package imports and module structure."""

import unittest


class TestPackageImports(unittest.TestCase):
    """Test all package imports work correctly."""

    def test_main_package_imports(self):
        """Test main package imports."""
        # Test main imports
        from sql_testing_library import (
            BaseMockTable,
            DatabaseAdapter,
            MockTableNotFoundError,
            QuerySizeLimitExceeded,
            SQLParseError,
            SQLTestCase,
            SQLTestFramework,
            SQLTestingError,
            TypeConversionError,
            sql_test,
        )

        # Verify imports are not None
        self.assertIsNotNone(sql_test)
        self.assertIsNotNone(SQLTestCase)
        self.assertIsNotNone(SQLTestFramework)
        self.assertIsNotNone(BaseMockTable)
        self.assertIsNotNone(DatabaseAdapter)
        self.assertIsNotNone(SQLTestingError)
        self.assertIsNotNone(MockTableNotFoundError)
        self.assertIsNotNone(SQLParseError)
        self.assertIsNotNone(QuerySizeLimitExceeded)
        self.assertIsNotNone(TypeConversionError)

    def test_backward_compatibility_alias(self):
        """Test that TestCase alias works."""
        from sql_testing_library import SQLTestCase, TestCase

        # TestCase should be an alias to SQLTestCase
        self.assertIs(TestCase, SQLTestCase)

    def test_version_attribute(self):
        """Test that version is accessible."""
        import sql_testing_library

        self.assertTrue(hasattr(sql_testing_library, "__version__"))
        self.assertIsInstance(sql_testing_library.__version__, str)

    def test_all_attribute(self):
        """Test that __all__ is properly defined."""
        import sql_testing_library

        self.assertTrue(hasattr(sql_testing_library, "__all__"))
        self.assertIsInstance(sql_testing_library.__all__, list)

        # Check key exports are in __all__
        expected_exports = [
            "SQLTestFramework",
            "TestCase",
            "BaseMockTable",
            "DatabaseAdapter",
            "sql_test",
        ]

        for export in expected_exports:
            self.assertIn(export, sql_testing_library.__all__)


class TestAdapterImports(unittest.TestCase):
    """Test adapter module imports."""

    def test_adapters_init_import(self):
        """Test adapters __init__.py imports."""
        from sql_testing_library._adapters import __all__

        self.assertIsInstance(__all__, list)
        # After lazy loading optimization, __all__ is empty by design
        # Individual adapters are imported directly when needed
        self.assertEqual(len(__all__), 0)

    def test_conditional_adapter_imports(self):
        """Test that adapters are conditionally imported."""
        # Test BigQuery adapter import if available
        try:
            from sql_testing_library._adapters.bigquery import BigQueryAdapter

            self.assertIsNotNone(BigQueryAdapter)
            # After lazy loading optimization, adapters are imported directly
            # No longer tracked in _adapters.__all__
        except ImportError:
            # BigQuery not available - this is expected in some environments
            pass

    def test_base_adapter_import(self):
        """Test base adapter import."""
        from sql_testing_library._adapters.base import DatabaseAdapter

        self.assertIsNotNone(DatabaseAdapter)

        # Test that it's an abstract class
        with self.assertRaises(TypeError):
            DatabaseAdapter()

    def test_adapter_all_list(self):
        """Test adapter __all__ list."""
        from sql_testing_library._adapters import __all__

        self.assertIsInstance(__all__, list)
        # After lazy loading optimization, __all__ is empty by design
        # This reduces import time and prevents loading all database SDKs
        self.assertEqual(len(__all__), 0)


if __name__ == "__main__":
    unittest.main()
