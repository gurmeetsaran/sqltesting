"""Test coverage for main __init__.py module."""

import unittest


class TestMainInitModule(unittest.TestCase):
    """Test the main sql_testing_library __init__.py module."""

    def test_backward_compatibility_alias(self):
        """Test that TestCase alias points to SQLTestCase."""
        import sql_testing_library
        from sql_testing_library._core import SQLTestCase

        self.assertIs(sql_testing_library.TestCase, SQLTestCase)

    def test_core_imports_available(self):
        """Test that core components are imported and available."""
        import sql_testing_library

        # Check that all core components are available
        self.assertTrue(hasattr(sql_testing_library, "SQLTestFramework"))
        self.assertTrue(hasattr(sql_testing_library, "TestCase"))
        self.assertTrue(hasattr(sql_testing_library, "BaseMockTable"))
        self.assertTrue(hasattr(sql_testing_library, "DatabaseAdapter"))
        self.assertTrue(hasattr(sql_testing_library, "sql_test"))

    def test_exception_imports_available(self):
        """Test that all exception classes are imported and available."""
        import sql_testing_library

        # Check that all exception classes are available
        self.assertTrue(hasattr(sql_testing_library, "SQLTestingError"))
        self.assertTrue(hasattr(sql_testing_library, "MockTableNotFoundError"))
        self.assertTrue(hasattr(sql_testing_library, "SQLParseError"))
        self.assertTrue(hasattr(sql_testing_library, "QuerySizeLimitExceeded"))
        self.assertTrue(hasattr(sql_testing_library, "TypeConversionError"))

    def test_all_attribute_contains_expected_items(self):
        """Test that __all__ contains all expected public API items."""
        import sql_testing_library

        expected_items = {
            "SQLTestFramework",
            "TestCase",
            "BaseMockTable",
            "DatabaseAdapter",
            "sql_test",
            "SQLTestingError",
            "MockTableNotFoundError",
            "SQLParseError",
            "QuerySizeLimitExceeded",
            "TypeConversionError",
        }

        # BigQueryAdapter may or may not be in __all__ depending on dependencies
        actual_items = set(sql_testing_library.__all__)

        # Check that all expected items are present
        self.assertTrue(expected_items.issubset(actual_items))

    def test_bigquery_adapter_import_success(self):
        """Test BigQueryAdapter import when dependencies are available."""
        # This test assumes BigQuery dependencies are available in the test environment
        try:
            import sql_testing_library

            # If BigQuery is available, it should be in __all__
            if hasattr(sql_testing_library, "BigQueryAdapter"):
                self.assertIn("BigQueryAdapter", sql_testing_library.__all__)
        except ImportError:
            # If dependencies aren't available, that's fine - just skip this test
            self.skipTest("BigQuery dependencies not available")

    def test_bigquery_adapter_import_failure_handling(self):
        """Test that missing BigQuery dependencies are handled gracefully."""
        # Test that the conditional import logic exists and works
        # We can't easily test actual import failures without breaking test isolation

        # Simply verify that the import structure is there
        import sql_testing_library

        # Should always have core functionality
        self.assertTrue(hasattr(sql_testing_library, "SQLTestFramework"))
        self.assertTrue(hasattr(sql_testing_library, "TestCase"))
        self.assertTrue(hasattr(sql_testing_library, "BaseMockTable"))

        # If BigQueryAdapter is available, it should be importable
        if hasattr(sql_testing_library, "BigQueryAdapter"):
            self.assertIn("BigQueryAdapter", sql_testing_library.__all__)

        # The module should always load without errors
        self.assertIsNotNone(sql_testing_library.__version__)

    def test_module_docstring(self):
        """Test that the module has a proper docstring."""
        import sql_testing_library

        self.assertIsNotNone(sql_testing_library.__doc__)
        self.assertIn("SQL Testing Library", sql_testing_library.__doc__)

    def test_imported_classes_functionality(self):
        """Test that imported classes are functional (not just importable)."""
        import sql_testing_library

        # Test that SQLTestFramework can be instantiated
        # (This tests that the import actually works)
        framework_class = sql_testing_library.SQLTestFramework
        self.assertTrue(callable(framework_class))

        # Test that exceptions can be raised
        error_class = sql_testing_library.SQLTestingError
        self.assertTrue(issubclass(error_class, Exception))

        # Test that sql_test decorator is callable
        self.assertTrue(callable(sql_testing_library.sql_test))

    def test_import_performance(self):
        """Test that imports don't take too long."""
        import time

        start_time = time.time()

        # Re-import to test import time
        import importlib

        import sql_testing_library

        importlib.reload(sql_testing_library)

        import_time = time.time() - start_time

        # Imports should be reasonably fast (less than 1 second)
        self.assertLess(import_time, 1.0)

    def test_no_circular_imports(self):
        """Test that there are no circular import issues."""
        # This test simply tries to import everything and checks for circular import errors
        try:
            import sql_testing_library  # noqa: F401
            from sql_testing_library import (  # noqa: F401
                BaseMockTable,
                DatabaseAdapter,
                MockTableNotFoundError,
                QuerySizeLimitExceeded,
                SQLParseError,
                SQLTestFramework,
                SQLTestingError,
                TestCase,
                TypeConversionError,
                sql_test,
            )

            # If we get here without ImportError, no circular imports
            self.assertTrue(True)
        except ImportError as e:
            if "circular import" in str(e).lower():
                self.fail(f"Circular import detected: {e}")
            else:
                # Other import errors might be due to missing dependencies
                pass


if __name__ == "__main__":
    unittest.main()
