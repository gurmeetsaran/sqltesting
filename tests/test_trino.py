"""Tests for using the SQL testing library with Trino."""

import unittest
from dataclasses import dataclass
from datetime import date
from unittest import mock

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
        return "test_db"

    def get_table_name(self) -> str:
        return "users"


@mock.patch("trino.dbapi.connect")
class TestTrinoIntegration(unittest.TestCase):
    """Test Trino integration with the SQL testing library."""

    def setUp(self):
        """Set up mock configuration."""
        # Create a mock ConfigParser with Trino config
        self.mock_config = mock.MagicMock()
        self.mock_config.__contains__ = lambda self, key: key in {
            "sql_testing",
            "sql_testing.trino",
        }
        self.mock_config.__getitem__ = lambda self, key: {
            "sql_testing": {"adapter": "trino"},
            "sql_testing.trino": {
                "host": "trino-host.example.com",
                "port": "8080",
                "user": "test_user",
                "catalog": "test_catalog",
                "schema": "test_schema",
                "http_scheme": "http",
                "auth_type": "basic",
                "password": "test_password",
            },
        }[key]

        # Mock Trino connection and cursor
        self.mock_conn = mock.MagicMock()
        self.mock_cursor = mock.MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

        # Set up mock cursor results for a SELECT query
        # Non-empty description indicates SELECT query
        self.mock_cursor.description = [("id", None), ("name", None)]
        self.mock_cursor.fetchall.return_value = [(1, "Alice")]

    def test_trino_configuration(self, mock_trino_connect):
        """Test Trino configuration loading."""
        mock_trino_connect.return_value = self.mock_conn

        # Create test instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Initialize config
        config = decorator._load_config()
        self.assertEqual(config["adapter"], "trino")

        # Initialize adapter config
        adapter_config = decorator._load_adapter_config()
        self.assertEqual(adapter_config["host"], "trino-host.example.com")
        self.assertEqual(adapter_config["port"], "8080")
        self.assertEqual(adapter_config["user"], "test_user")
        self.assertEqual(adapter_config["catalog"], "test_catalog")
        self.assertEqual(adapter_config["schema"], "test_schema")
        self.assertEqual(adapter_config["http_scheme"], "http")
        self.assertEqual(adapter_config["auth_type"], "basic")
        self.assertEqual(adapter_config["password"], "test_password")

        # Create framework
        framework = decorator._create_framework_from_config()

        # Verify Trino adapter was created correctly
        self.assertEqual(framework.adapter.__class__.__name__, "TrinoAdapter")
        self.assertEqual(framework.adapter.host, "trino-host.example.com")
        self.assertEqual(framework.adapter.port, 8080)
        self.assertEqual(framework.adapter.user, "test_user")
        self.assertEqual(framework.adapter.catalog, "test_catalog")
        self.assertEqual(framework.adapter.schema, "test_schema")
        self.assertEqual(framework.adapter.http_scheme, "http")

        # Verify auth dictionary is properly structured
        self.assertEqual(framework.adapter.auth["type"], "basic")
        self.assertEqual(framework.adapter.auth["user"], "test_user")
        self.assertEqual(framework.adapter.auth["password"], "test_password")

    def test_trino_sql_test_decorator(self, mock_trino_connect):
        """Test sql_test decorator with Trino adapter."""
        mock_trino_connect.return_value = self.mock_conn

        # Create a test decorator instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        try:
            # Mock the global decorator instance
            from sql_testing_library import _pytest_plugin as pytest_plugin

            original_decorator_instance = pytest_plugin._sql_test_decorator
            pytest_plugin._sql_test_decorator = decorator

            # Define a test case with Trino adapter
            @sql_test(
                adapter_type="trino",
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
            def test_trino_query():
                return TestCase(
                    query="SELECT id, name FROM users WHERE id = 1",
                    default_namespace="test_db",
                )

            # Execute the test
            results = test_trino_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Alice")

            # Verify Trino was called with the correct parameters
            mock_trino_connect.assert_called_with(
                host="trino-host.example.com",
                port=8080,
                user="test_user",
                catalog="test_catalog",
                schema="test_schema",
                http_scheme="http",
                auth={
                    "type": "basic",
                    "user": "test_user",
                    "password": "test_password",
                },
            )

            # Verify query was executed (once for the actual query)
            self.assertEqual(self.mock_cursor.execute.call_count, 1)

            # Verify the query was constructed properly
            execute_call = self.mock_cursor.execute.call_args[0][0]
            self.assertIn("SELECT id, name FROM", execute_call)
            self.assertIn("WHERE id = 1", execute_call)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
