"""Tests for using the SQL testing library with BigQuery."""

import unittest
from dataclasses import dataclass
from datetime import date
from unittest import mock

import pandas as pd
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable
from sql_testing_library._pytest_plugin import SQLTestDecorator


@dataclass
class User:
    """Test user data class."""

    id: int
    name: str
    email: str
    active: bool
    created_at: date


class UserResult(BaseModel):
    """Test user result model."""

    id: int
    name: str


class UsersMockTable(BaseMockTable):
    """Mock table for user data."""

    def get_database_name(self) -> str:
        return "test_dataset"

    def get_table_name(self) -> str:
        return "users"


@mock.patch("google.cloud.bigquery.Client")
class TestBigQueryIntegration(unittest.TestCase):
    """Test BigQuery integration with the SQL testing library."""

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

        # Mock the DataFrame result
        self.mock_df = pd.DataFrame([{"id": 1, "name": "Alice"}])
        self.mock_query_job.to_dataframe.return_value = self.mock_df

    def test_bigquery_configuration(self, mock_bigquery_client):
        """Test BigQuery configuration loading."""
        mock_bigquery_client.from_service_account_json.return_value = self.mock_client

        # Create test instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Initialize config
        config = decorator._load_config()
        self.assertEqual(config["adapter"], "bigquery")

        # Initialize adapter config
        adapter_config = decorator._load_adapter_config()
        self.assertEqual(adapter_config["project_id"], "test-project")
        self.assertEqual(adapter_config["dataset_id"], "test_dataset")
        self.assertEqual(adapter_config["credentials_path"], "/path/to/credentials.json")

        # Create framework
        framework = decorator._create_framework_from_config()

        # Verify BigQuery adapter was created correctly
        self.assertEqual(framework.adapter.__class__.__name__, "BigQueryAdapter")
        self.assertEqual(framework.adapter.project_id, "test-project")
        self.assertEqual(framework.adapter.dataset_id, "test_dataset")

        # Verify client was created with credentials
        mock_bigquery_client.from_service_account_json.assert_called_with(
            "/path/to/credentials.json"
        )

    def test_bigquery_sql_test_decorator(self, mock_bigquery_client):
        """Test sql_test decorator with BigQuery adapter."""
        mock_bigquery_client.from_service_account_json.return_value = self.mock_client
        mock_bigquery_client.return_value = self.mock_client

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Save original decorator instance
        from sql_testing_library import _pytest_plugin as pytest_plugin

        original_decorator_instance = pytest_plugin._sql_test_decorator

        try:
            # Mock the global decorator instance

            pytest_plugin._sql_test_decorator = decorator

            # Define a test case with BigQuery adapter
            @sql_test(
                adapter_type="bigquery",
                mock_tables=[
                    UsersMockTable(
                        [
                            User(1, "Alice", "alice@example.com", True, date(2023, 1, 1)),
                            User(2, "Bob", "bob@example.com", False, date(2023, 1, 2)),
                        ]
                    )
                ],
                result_class=UserResult,
            )
            def test_bigquery_query():
                return TestCase(
                    query="SELECT id, name FROM users WHERE id = 1",
                    execution_database="test_dataset",
                )

            # Execute the test
            results = test_bigquery_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Alice")

            # Verify BigQuery client was used
            self.mock_client.query.assert_called_once()

            # Verify query contains our WHERE clause
            query_arg = self.mock_client.query.call_args[0][0]
            self.assertIn("SELECT id, name FROM", query_arg)
            self.assertIn("WHERE id = 1", query_arg)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
