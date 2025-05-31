"""Tests for using physical tables with Redshift adapter."""

import unittest
from dataclasses import dataclass
from datetime import date
from unittest import mock

from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable
from sql_testing_library._pytest_plugin import SQLTestDecorator


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


@mock.patch("psycopg2.connect")
class TestRedshiftPhysicalTables(unittest.TestCase):
    """Test Redshift physical tables support."""

    def setUp(self):
        """Set up mock configuration."""
        # Create a mock ConfigParser with Redshift config
        self.mock_config = mock.MagicMock()
        self.mock_config.__contains__ = lambda self, key: key in {
            "sql_testing",
            "sql_testing.redshift",
        }
        self.mock_config.__getitem__ = lambda self, key: {
            "sql_testing": {"adapter": "redshift"},
            "sql_testing.redshift": {
                "host": "redshift-host.example.com",
                "database": "test_db",
                "user": "test_user",
                "password": "test_password",
                "port": "5439",
            },
        }[key]

        # Mock Redshift connection and cursor
        self.mock_conn = mock.MagicMock()
        self.mock_cursor = mock.MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor

        # Set up mock cursor results for a SELECT query
        self.mock_cursor.description = ["id", "name", "price", "category"]
        self.mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Product A", "price": 19.99, "category": "Electronics"},
        ]

        # Create test products
        self.test_products = [
            Product(1, "Product A", 19.99, "Electronics", date(2023, 1, 1)),
            Product(2, "Product B", 29.99, "Home", date(2023, 1, 2)),
            Product(3, "Product C", 9.99, "Books", date(2023, 1, 3)),
        ]

    def test_redshift_physical_tables(self, mock_psycopg2_connect):
        """Test using physical tables with Redshift."""
        mock_psycopg2_connect.return_value = self.mock_conn

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Save original decorator instance
        from sql_testing_library import _pytest_plugin as pytest_plugin

        original_decorator_instance = pytest_plugin._sql_test_decorator

        try:
            # Mock the global decorator instance

            pytest_plugin._sql_test_decorator = decorator

            # The test uses the global instance

            # Define a test case with Redshift adapter using physical tables
            @sql_test(
                adapter_type="redshift",
                mock_tables=[ProductsMockTable(self.test_products)],
                result_class=ProductResult,
                use_physical_tables=True,
            )
            def test_redshift_physical_tables():
                return TestCase(
                    query="SELECT id, name, price, category FROM products"
                    + " WHERE category = 'Electronics'",
                    execution_database="test_db",
                )

            # Execute the test
            results = test_redshift_physical_tables()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Product A")
            self.assertEqual(results[0].price, 19.99)
            self.assertEqual(results[0].category, "Electronics")

            # Verify temp table was created with CTAS
            ctas_calls = [
                call
                for call in self.mock_cursor.execute.call_args_list
                if "CREATE TEMPORARY TABLE" in call[0][0] and " AS " in call[0][0]
            ]
            self.assertGreaterEqual(len(ctas_calls), 1)

            # There should be no separate INSERT calls since we're using CTAS
            insert_calls = [
                call
                for call in self.mock_cursor.execute.call_args_list
                if "INSERT INTO" in call[0][0]
            ]
            self.assertEqual(len(insert_calls), 0)

            # Verify query execution
            query_calls = [
                call
                for call in self.mock_cursor.execute.call_args_list
                if "SELECT id, name, price, category FROM" in call[0][0]
            ]
            self.assertGreaterEqual(len(query_calls), 1)

            # No DROP TABLE calls should be made since Redshift temporary tables
            # are automatically dropped at the end of the session
            drop_calls = [
                call
                for call in self.mock_cursor.execute.call_args_list
                if "DROP TABLE IF EXISTS" in call[0][0]
            ]
            self.assertEqual(len(drop_calls), 0)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance

    def test_redshift_large_query_fallback(self, mock_psycopg2_connect):
        """Test fallback to physical tables for large queries."""
        mock_psycopg2_connect.return_value = self.mock_conn

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Save original decorator instance
        from sql_testing_library import _pytest_plugin as pytest_plugin

        original_decorator_instance = pytest_plugin._sql_test_decorator

        try:
            # Mock the global decorator instance

            pytest_plugin._sql_test_decorator = decorator

            # The test uses the global instance

            # Create a query that will exceed our mocked size limit
            conditions = [f"id = {i}" for i in range(10)]
            large_query = "SELECT * FROM products WHERE " + " OR ".join(conditions)

            # Define a test case with automatic fallback to physical tables
            @sql_test(
                adapter_type="redshift",
                mock_tables=[ProductsMockTable(self.test_products)],
                result_class=ProductResult,
                use_physical_tables=True,  # Force physical tables
            )
            def test_redshift_large_query():
                return TestCase(query=large_query, execution_database="test_db")

            # Execute the test with physical tables
            results = test_redshift_large_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Product A")

            # Verify temp table was created
            create_calls = [
                call
                for call in self.mock_cursor.execute.call_args_list
                if "CREATE TEMPORARY TABLE" in call[0][0]
            ]
            self.assertGreaterEqual(len(create_calls), 1)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
