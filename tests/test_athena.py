"""Tests for using the SQL testing library with Athena."""

import unittest
from dataclasses import dataclass
from datetime import date
from unittest import mock

from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library.mock_table import BaseMockTable
from sql_testing_library.pytest_plugin import SQLTestDecorator


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
        return "test_db"

    def get_table_name(self) -> str:
        return "users"


@mock.patch("boto3.client")
class TestAthenaIntegration(unittest.TestCase):
    """Test Athena integration with the SQL testing library."""

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
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                    {
                        "Data": [
                            {"VarCharValue": "1"},
                            {"VarCharValue": "Alice"},
                        ]
                    },
                ]
            }
        }

    def test_athena_configuration(self, mock_boto3_client):
        """Test Athena configuration loading."""
        mock_boto3_client.return_value = self.mock_client

        # Create test instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Initialize config
        config = decorator._load_config()
        self.assertEqual(config["adapter"], "athena")

        # Initialize adapter config
        adapter_config = decorator._load_adapter_config()
        self.assertEqual(adapter_config["database"], "test_db")
        self.assertEqual(
            adapter_config["s3_output_location"], "s3://test-bucket/output/"
        )
        self.assertEqual(adapter_config["region"], "us-west-2")

        # Create framework
        framework = decorator._create_framework_from_config()

        # Verify Athena adapter was created correctly
        self.assertEqual(framework.adapter.__class__.__name__, "AthenaAdapter")
        self.assertEqual(framework.adapter.database, "test_db")
        self.assertEqual(
            framework.adapter.s3_output_location, "s3://test-bucket/output/"
        )

    def test_athena_sql_test_decorator(self, mock_boto3_client):
        """Test sql_test decorator with Athena adapter."""
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

            # Define a test case with Athena adapter
            @sql_test(
                adapter_type="athena",
                mock_tables=[
                    UsersMockTable(
                        [
                            User(
                                1, "Alice", "alice@example.com", True, date(2023, 1, 1)
                            ),
                            User(2, "Bob", "bob@example.com", False, date(2023, 1, 2)),
                        ]
                    )
                ],
                result_class=UserResult,
            )
            def test_athena_query():
                return TestCase(
                    query="SELECT id, name FROM users WHERE id = 1",
                    execution_database="test_db",
                )

            # Execute the test
            results = test_athena_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Alice")

            # Verify Athena client was called
            mock_boto3_client.assert_called_with("athena", region_name="us-west-2")
            self.mock_client.start_query_execution.assert_called()

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
