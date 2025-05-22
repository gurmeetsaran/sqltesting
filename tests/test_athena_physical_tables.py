"""Tests for using physical tables with Athena adapter."""

import unittest
from dataclasses import dataclass
from datetime import date
from unittest import mock

from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library.mock_table import BaseMockTable
from sql_testing_library.pytest_plugin import SQLTestDecorator


@dataclass
class Product:
    """Test product data class."""

    id: int
    name: str
    price: float
    category: str
    created_at: date


class ProductResult(BaseModel):
    """Test product result model."""

    id: int
    name: str
    price: float
    category: str


class ProductsMockTable(BaseMockTable):
    """Mock table for product data."""

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "products"


@mock.patch("boto3.client")
class TestAthenaPhysicalTables(unittest.TestCase):
    """Test Athena physical tables support."""

    def setUp(self):
        """Set up mock configuration."""
        # Create a mock ConfigParser with Athena config
        self.mock_config = mock.MagicMock()
        self.mock_config.__contains__ = lambda self, key: key in {
            "sql_testing",
            "sql_testing.athena",
        }
        self.mock_config.__getitem__ = lambda self, key: {
            "sql_testing": {"adapter": "athena"},
            "sql_testing.athena": {
                "database": "test_db",
                "s3_output_location": "s3://test-bucket/output/",
                "region": "us-west-2",
            },
        }[key]

        # Mock Athena client responses
        self.mock_client = mock.MagicMock()
        self.mock_client.start_query_execution.return_value = {
            "QueryExecutionId": "test_query_id"
        }
        self.mock_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        self.mock_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {
                        "Data": [
                            {"VarCharValue": "id"},
                            {"VarCharValue": "name"},
                            {"VarCharValue": "price"},
                            {"VarCharValue": "category"},
                        ]
                    },
                    {
                        "Data": [
                            {"VarCharValue": "1"},
                            {"VarCharValue": "Product A"},
                            {"VarCharValue": "19.99"},
                            {"VarCharValue": "Electronics"},
                        ]
                    },
                ]
            }
        }

        # Create test products
        self.test_products = [
            Product(1, "Product A", 19.99, "Electronics", date(2023, 1, 1)),
            Product(2, "Product B", 29.99, "Home", date(2023, 1, 2)),
            Product(3, "Product C", 9.99, "Books", date(2023, 1, 3)),
        ]

    def test_athena_physical_tables(self, mock_boto3_client):
        """Test using physical tables with Athena."""
        mock_boto3_client.return_value = self.mock_client

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Save original decorator instance

        try:
            # Mock the global decorator instance
            from sql_testing_library import pytest_plugin

            original_decorator_instance = pytest_plugin._sql_test_decorator
            pytest_plugin._sql_test_decorator = decorator

            # The test uses the global instance

            # Define a test case with Athena adapter using physical tables
            @sql_test(
                adapter_type="athena",
                mock_tables=[ProductsMockTable(self.test_products)],
                result_class=ProductResult,
                use_physical_tables=True,
            )
            def test_athena_physical_tables():
                return TestCase(
                    query="SELECT id, name, price, category FROM products"
                    + " WHERE category = 'Electronics'",
                    execution_database="test_db",
                )

            # Execute the test
            results = test_athena_physical_tables()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Product A")
            self.assertEqual(results[0].price, 19.99)
            self.assertEqual(results[0].category, "Electronics")

            # Verify temp table was created and query executed
            create_table_calls = [
                call
                for call in self.mock_client.start_query_execution.call_args_list
                if "CREATE TABLE" in call[1]["QueryString"]
            ]
            self.assertGreaterEqual(len(create_table_calls), 1)

            # Verify CTAS or query execution
            query_calls = [
                call
                for call in self.mock_client.start_query_execution.call_args_list
                if "SELECT id, name, price, category FROM" in call[1]["QueryString"]
            ]
            self.assertGreaterEqual(len(query_calls), 1)

            # Verify cleanup was called
            drop_calls = [
                call
                for call in self.mock_client.start_query_execution.call_args_list
                if "DROP TABLE IF EXISTS" in call[1]["QueryString"]
            ]
            self.assertGreaterEqual(len(drop_calls), 1)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance

    def test_athena_large_query_fallback(self, mock_boto3_client):
        """Test fallback to physical tables for large queries."""
        mock_boto3_client.return_value = self.mock_client

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Save original decorator instance

        try:
            # Mock the global decorator instance
            from sql_testing_library import pytest_plugin

            original_decorator_instance = pytest_plugin._sql_test_decorator
            pytest_plugin._sql_test_decorator = decorator

            # The test uses the global instance

            # Create a query that will exceed our mocked size limit
            conditions = [f"id = {i}" for i in range(10)]
            large_query = "SELECT * FROM products WHERE " + " OR ".join(conditions)

            # Define a test case with automatic fallback to physical tables
            @sql_test(
                adapter_type="athena",
                mock_tables=[ProductsMockTable(self.test_products)],
                result_class=ProductResult,
                use_physical_tables=True,  # Force physical tables
            )
            def test_athena_large_query():
                return TestCase(query=large_query, execution_database="test_db")

            # Execute the test with physical tables
            results = test_athena_large_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Product A")

            # Verify temp table was created
            create_table_calls = [
                call
                for call in self.mock_client.start_query_execution.call_args_list
                if "CREATE TABLE" in call[1]["QueryString"]
            ]
            self.assertGreaterEqual(len(create_table_calls), 1)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
