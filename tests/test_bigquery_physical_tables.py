"""Tests for using physical tables with BigQuery adapter."""

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
        return "test_dataset"

    def get_table_name(self) -> str:
        return "products"


@mock.patch("google.cloud.bigquery.Client")
class TestBigQueryPhysicalTables(unittest.TestCase):
    """Test BigQuery physical tables support."""

    def setUp(self):
        """Set up mock configuration."""
        # Create a mock ConfigParser with BigQuery config
        self.mock_config = mock.MagicMock()
        self.mock_config.__contains__ = lambda self, key: key in {
            "sql_testing",
            "sql_testing.bigquery",
        }
        self.mock_config.__getitem__ = lambda self, key: {
            "sql_testing": {"adapter": "bigquery"},
            "sql_testing.bigquery": {
                "project_id": "test-project",
                "dataset_id": "test_dataset",
                "credentials_path": "/path/to/credentials.json",
            },
        }[key]

        # Mock BigQuery client
        self.mock_client = mock.MagicMock()
        self.mock_query_job = mock.MagicMock()
        self.mock_client.query.return_value = self.mock_query_job

        # Mock create_table and load_table_from_dataframe
        self.mock_table = mock.MagicMock()
        self.mock_client.create_table.return_value = self.mock_table
        self.mock_load_job = mock.MagicMock()
        self.mock_client.load_table_from_dataframe.return_value = self.mock_load_job

        # Mock the DataFrame result
        import pandas as pd

        self.mock_df = pd.DataFrame(
            [{"id": 1, "name": "Product A", "price": 19.99, "category": "Electronics"}]
        )
        self.mock_query_job.to_dataframe.return_value = self.mock_df

        # Create test products
        self.test_products = [
            Product(1, "Product A", 19.99, "Electronics", date(2023, 1, 1)),
            Product(2, "Product B", 29.99, "Home", date(2023, 1, 2)),
            Product(3, "Product C", 9.99, "Books", date(2023, 1, 3)),
        ]

    def test_bigquery_physical_tables(self, mock_bigquery_client):
        """Test using physical tables with BigQuery."""
        mock_bigquery_client.from_service_account_json.return_value = self.mock_client
        mock_bigquery_client.return_value = self.mock_client

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        try:
            # Mock the global decorator instance
            from sql_testing_library import _pytest_plugin as pytest_plugin

            original_decorator_instance = pytest_plugin._sql_test_decorator
            pytest_plugin._sql_test_decorator = decorator

            # Define a test case with BigQuery adapter using physical tables
            @sql_test(
                adapter_type="bigquery",
                mock_tables=[ProductsMockTable(self.test_products)],
                result_class=ProductResult,
                use_physical_tables=True,
            )
            def test_bigquery_physical_tables():
                return TestCase(
                    query="SELECT id, name, price, category FROM products"
                    + " WHERE category = 'Electronics'",
                    execution_database="test_dataset",
                )

            # Execute the test
            results = test_bigquery_physical_tables()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Product A")
            self.assertEqual(results[0].price, 19.99)
            self.assertEqual(results[0].category, "Electronics")

            # Verify table was created
            self.mock_client.create_table.assert_called()

            # Verify data was loaded into the table
            self.mock_client.load_table_from_dataframe.assert_called()
            self.mock_load_job.result.assert_called_once()  # Wait for job completion

            # Verify query was executed
            self.mock_client.query.assert_called()
            query_call = self.mock_client.query.call_args_list[-1]  # Get the last query call
            query_sql = query_call[0][0]
            self.assertIn("SELECT id, name, price, category FROM", query_sql)
            self.assertIn("WHERE category = 'Electronics'", query_sql)

            # Verify temporary table was deleted
            self.mock_client.delete_table.assert_called()

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance

    def test_bigquery_large_query(self, mock_bigquery_client):
        """Test handling large queries with physical tables."""
        mock_bigquery_client.from_service_account_json.return_value = self.mock_client
        mock_bigquery_client.return_value = self.mock_client

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        try:
            # Mock the global decorator instance
            from sql_testing_library import _pytest_plugin as pytest_plugin

            original_decorator_instance = pytest_plugin._sql_test_decorator
            pytest_plugin._sql_test_decorator = decorator

            # Create a complex query with multiple conditions
            conditions = [f"id = {i}" for i in range(10)]
            large_query = "SELECT id, name, price, category FROM products WHERE " + " OR ".join(
                conditions
            )

            # Define a test case with physical tables
            @sql_test(
                adapter_type="bigquery",
                mock_tables=[ProductsMockTable(self.test_products)],
                result_class=ProductResult,
                use_physical_tables=True,
            )
            def test_bigquery_large_query():
                return TestCase(query=large_query, execution_database="test_dataset")

            # Execute the test
            results = test_bigquery_large_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Product A")
            self.assertEqual(results[0].price, 19.99)
            self.assertEqual(results[0].category, "Electronics")

            # Verify table was created
            self.mock_client.create_table.assert_called()

            # Verify data was loaded
            self.mock_client.load_table_from_dataframe.assert_called()

            # Verify query contained our conditions
            query_calls = self.mock_client.query.call_args_list
            any_matched = False
            for call in query_calls:
                if all(cond in call[0][0] for cond in conditions):
                    any_matched = True
                    break
            self.assertTrue(any_matched, "Query with all conditions was not executed")

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
