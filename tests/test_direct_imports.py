"""Direct import tests to trigger module loading."""

import unittest


class TestDirectImports(unittest.TestCase):
    """Tests to directly trigger imports of modules."""

    def test_main_init_imports(self):
        """Test main __init__.py imports to trigger coverage."""
        # This will execute the main __init__.py file
        import sql_testing_library

        # Access attributes to ensure they're evaluated
        version = sql_testing_library.__version__
        all_exports = sql_testing_library.__all__

        self.assertIsInstance(version, str)
        self.assertIsInstance(all_exports, list)

        # Test importing main classes
        sqltest_case = sql_testing_library.SQLTestCase
        test_case = sql_testing_library.TestCase
        sql_test = sql_testing_library.sql_test
        framework = sql_testing_library.SQLTestFramework
        base_mock = sql_testing_library.BaseMockTable
        database_adapter = sql_testing_library.DatabaseAdapter

        # Test that classes are not None
        self.assertIsNotNone(sqltest_case)
        self.assertIsNotNone(test_case)
        self.assertIsNotNone(sql_test)
        self.assertIsNotNone(framework)
        self.assertIsNotNone(base_mock)
        self.assertIsNotNone(database_adapter)

        # Test exceptions
        sql_error = sql_testing_library.SQLTestingError
        mock_error = sql_testing_library.MockTableNotFoundError
        parse_error = sql_testing_library.SQLParseError
        size_error = sql_testing_library.QuerySizeLimitExceeded
        type_error = sql_testing_library.TypeConversionError

        self.assertIsNotNone(sql_error)
        self.assertIsNotNone(mock_error)
        self.assertIsNotNone(parse_error)
        self.assertIsNotNone(size_error)
        self.assertIsNotNone(type_error)

    def test_adapters_init_imports(self):
        """Test adapters __init__.py imports."""
        # Import the adapters module to trigger __init__.py execution
        import sql_testing_library.adapters

        # Access the __all__ attribute
        all_adapters = sql_testing_library.adapters.__all__
        self.assertIsInstance(all_adapters, list)

        # Try to import each adapter that might be available
        adapter_names = [
            "BigQueryAdapter",
            "AthenaAdapter",
            "RedshiftAdapter",
            "TrinoAdapter",
            "SnowflakeAdapter",
        ]

        for adapter_name in adapter_names:
            try:
                adapter_class = getattr(sql_testing_library.adapters, adapter_name)
                self.assertIsNotNone(adapter_class)
            except AttributeError:
                # Adapter not available in current environment
                pass

    def test_import_conditional_adapters(self):
        """Test importing adapters conditionally."""
        # This should trigger the try/except blocks in adapters/__init__.py

        # Test BigQuery
        try:
            from sql_testing_library.adapters import BigQueryAdapter

            self.assertIsNotNone(BigQueryAdapter)
        except ImportError:
            pass

        # Test Athena
        try:
            from sql_testing_library.adapters import AthenaAdapter

            self.assertIsNotNone(AthenaAdapter)
        except ImportError:
            pass

        # Test Redshift
        try:
            from sql_testing_library.adapters import RedshiftAdapter

            self.assertIsNotNone(RedshiftAdapter)
        except ImportError:
            pass

        # Test Trino
        try:
            from sql_testing_library.adapters import TrinoAdapter

            self.assertIsNotNone(TrinoAdapter)
        except ImportError:
            pass

        # Test Snowflake
        try:
            from sql_testing_library.adapters import SnowflakeAdapter

            self.assertIsNotNone(SnowflakeAdapter)
        except ImportError:
            pass

    def test_bigquery_conditional_import(self):
        """Test BigQuery adapter conditional import in main __init__.py."""
        # This should trigger the try/except in main __init__.py for BigQuery
        import sql_testing_library

        # Check if BigQueryAdapter is in __all__
        all_exports = sql_testing_library.__all__
        if "BigQueryAdapter" in all_exports:
            bigquery_adapter = sql_testing_library.BigQueryAdapter
            self.assertIsNotNone(bigquery_adapter)


if __name__ == "__main__":
    unittest.main()
