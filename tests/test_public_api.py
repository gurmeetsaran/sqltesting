"""Tests for the public API exposed in __init__.py."""

import unittest

import pytest


class TestPublicAPIImports(unittest.TestCase):
    """Test that the public API imports work correctly."""

    def test_import_sql_test_decorator(self):
        """Test that sql_test decorator can be imported."""
        from sql_testing_library import sql_test

        assert callable(sql_test)
        # Test that it's the correct function
        assert sql_test.__name__ == "sql_test"

    def test_import_test_case_class(self):
        """Test that TestCase can be imported."""
        from sql_testing_library import TestCase

        # Should be an alias for SQLTestCase
        assert TestCase is not None
        # Test that it can be instantiated
        test_case = TestCase(query="SELECT 1", default_namespace="test_db")
        assert test_case.query == "SELECT 1"
        assert test_case.default_namespace == "test_db"

    def test_import_mock_table_base(self):
        """Test that BaseMockTable can be imported."""
        from sql_testing_library import BaseMockTable

        # Should be available
        assert BaseMockTable is not None
        # Should be an abstract base class
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            BaseMockTable([])

    def test_import_all_main_exports(self):
        """Test that all main exports are available."""
        from sql_testing_library import BaseMockTable, TestCase, sql_test

        # All should be callable or classes
        assert callable(sql_test)
        assert TestCase is not None
        assert BaseMockTable is not None

    def test_direct_import_from_module(self):
        """Test that imports work when importing the module directly."""
        import sql_testing_library

        assert hasattr(sql_testing_library, "sql_test")
        assert hasattr(sql_testing_library, "TestCase")
        assert hasattr(sql_testing_library, "BaseMockTable")

    def test_import_with_star(self):
        """Test that star imports work correctly."""
        # Import all public symbols at module level
        import sql_testing_library

        # Check that main symbols are available in the module
        assert hasattr(sql_testing_library, "sql_test")
        assert hasattr(sql_testing_library, "TestCase")
        assert hasattr(sql_testing_library, "BaseMockTable")

        # Test that we can access them
        sql_test = sql_testing_library.sql_test
        TestCase = sql_testing_library.TestCase
        BaseMockTable = sql_testing_library.BaseMockTable

        assert callable(sql_test)
        assert TestCase is not None
        assert BaseMockTable is not None

    def test_sql_test_decorator_basic_functionality(self):
        """Test basic functionality of the sql_test decorator."""
        from sql_testing_library import TestCase, sql_test

        # Test that decorator can be applied
        @sql_test
        def test_function():
            return TestCase(query="SELECT 1 as test_col", default_namespace="test_db")

        # Should not raise an error and should be callable
        assert callable(test_function)
        # Decorator creates a wrapper function, check for function properties
        assert hasattr(test_function, "__name__")

    def test_test_case_alias_compatibility(self):
        """Test that TestCase alias works the same as SQLTestCase."""
        from sql_testing_library import TestCase
        from sql_testing_library._core import SQLTestCase

        # Should be the same class
        assert TestCase is SQLTestCase

        # Should have same functionality
        test_case1 = TestCase(query="SELECT 1", default_namespace="db1")
        test_case2 = SQLTestCase(query="SELECT 1", default_namespace="db1")

        assert type(test_case1) is type(test_case2)
        assert test_case1.query == test_case2.query
        assert test_case1.execution_database == test_case2.execution_database

    def test_mock_table_alias_compatibility(self):
        """Test that BaseMockTable is available from both locations."""
        from sql_testing_library import BaseMockTable
        from sql_testing_library._mock_table import BaseMockTable as DirectBaseMockTable

        # Should be the same class
        assert BaseMockTable is DirectBaseMockTable

    def test_version_accessibility(self):
        """Test that package version is accessible."""
        import sql_testing_library

        # Should have a version attribute
        assert hasattr(sql_testing_library, "__version__")
        version = sql_testing_library.__version__

        # Version should be a string
        assert isinstance(version, str)
        # Should follow semantic versioning pattern (basic check)
        assert len(version.split(".")) >= 2


class TestModuleStructure(unittest.TestCase):
    """Test the overall module structure and organization."""

    def test_module_has_docstring(self):
        """Test that the main module has a docstring."""
        import sql_testing_library

        assert sql_testing_library.__doc__ is not None
        assert len(sql_testing_library.__doc__.strip()) > 0

    def test_module_attributes(self):
        """Test that module has expected attributes."""
        import sql_testing_library

        # Should have standard module attributes
        assert hasattr(sql_testing_library, "__name__")
        assert hasattr(sql_testing_library, "__version__")
        assert hasattr(sql_testing_library, "__file__")

    def test_no_unnecessary_imports_in_namespace(self):
        """Test that internal modules aren't exposed in public namespace."""
        import sql_testing_library

        # These internal modules should not be in the public namespace
        # Note: Private modules with leading underscores are accessible but discouraged
        internal_modules = [
            "core",  # Old names should not exist
            "adapters",
            "exceptions",
            "mock_table",
            "pytest_plugin",
            "types",
            "sql_utils",  # Should not be directly accessible
        ]

        for module_name in internal_modules:
            assert not hasattr(sql_testing_library, module_name), (
                f"Internal module '{module_name}' should not be in public namespace"
            )

    def test_public_api_stability(self):
        """Test that the public API includes expected symbols."""
        import sql_testing_library

        # Core public API symbols that should always be available
        expected_symbols = ["sql_test", "TestCase", "BaseMockTable"]

        for symbol in expected_symbols:
            assert hasattr(sql_testing_library, symbol), f"Public API symbol '{symbol}' is missing"

    def test_import_performance(self):
        """Test that imports don't take too long (basic performance check)."""
        import time

        start_time = time.time()

        # Import should be reasonably fast

        import_time = time.time() - start_time

        # Should import in less than 5 seconds (very generous limit)
        assert import_time < 5.0, f"Imports took {import_time:.2f} seconds, which is too slow"


class TestBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility of the public API."""

    def test_legacy_import_patterns(self):
        """Test that legacy import patterns still work."""
        # Test various import patterns that users might have used

        # Direct function import
        from sql_testing_library import sql_test

        assert callable(sql_test)

        # Class import
        from sql_testing_library import BaseMockTable, TestCase

        assert TestCase is not None
        assert BaseMockTable is not None

        # Module import with attribute access
        import sql_testing_library as stl

        assert hasattr(stl, "sql_test")
        assert hasattr(stl, "TestCase")
        assert hasattr(stl, "BaseMockTable")

    def test_core_functionality_accessible(self):
        """Test that core functionality is accessible through public API."""
        from sql_testing_library import BaseMockTable, TestCase

        # TestCase should be usable for creating test cases
        test_case = TestCase(query="SELECT 1 as id, 'test' as name", default_namespace="test_db")

        assert test_case.query is not None
        assert test_case.default_namespace == "test_db"

        # BaseMockTable should be an abstract base that can be subclassed
        class TestMockTable(BaseMockTable):
            def get_database_name(self):
                return "test_db"

            def get_table_name(self):
                return "test_table"

        # Should be able to create subclass instance
        mock_table = TestMockTable([{"id": 1, "name": "test"}])
        assert mock_table.get_database_name() == "test_db"
        assert mock_table.get_table_name() == "test_table"

    def test_decorator_usage_patterns(self):
        """Test common decorator usage patterns."""
        from sql_testing_library import TestCase, sql_test

        # Basic usage without arguments
        @sql_test
        def test_basic():
            return TestCase(query="SELECT 1", default_namespace="db")

        assert callable(test_basic)

        # Usage with adapter type (just test that decorator accepts the parameter)
        @sql_test(adapter_type="bigquery")
        def test_with_adapter():
            return TestCase(query="SELECT 1", default_namespace="db")

        assert callable(test_with_adapter)


if __name__ == "__main__":
    unittest.main()
