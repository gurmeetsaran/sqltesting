"""Tests for using the SQL testing library with Redshift."""

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


@mock.patch("psycopg2.connect")
class TestRedshiftIntegration(unittest.TestCase):
    """Test Redshift integration with the SQL testing library."""

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
        # Non-empty description indicates SELECT query
        self.mock_cursor.description = ["id", "name"]
        self.mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
        ]

    def test_redshift_configuration(self, mock_psycopg2_connect):
        """Test Redshift configuration loading."""
        mock_psycopg2_connect.return_value = self.mock_conn

        # Create test instance
        decorator = SQLTestDecorator()
        decorator._config_parser = self.mock_config

        # Initialize config
        config = decorator._load_config()
        self.assertEqual(config["adapter"], "redshift")

        # Initialize adapter config
        adapter_config = decorator._load_adapter_config()
        self.assertEqual(adapter_config["host"], "redshift-host.example.com")
        self.assertEqual(adapter_config["database"], "test_db")
        self.assertEqual(adapter_config["user"], "test_user")
        self.assertEqual(adapter_config["password"], "test_password")
        self.assertEqual(adapter_config["port"], "5439")

        # Create framework
        framework = decorator._create_framework_from_config()

        # Verify Redshift adapter was created correctly
        self.assertEqual(framework.adapter.__class__.__name__, "RedshiftAdapter")
        self.assertEqual(framework.adapter.host, "redshift-host.example.com")
        self.assertEqual(framework.adapter.database, "test_db")
        self.assertEqual(framework.adapter.user, "test_user")
        self.assertEqual(framework.adapter.password, "test_password")
        self.assertEqual(framework.adapter.port, 5439)

    def test_redshift_sql_test_decorator(self, mock_psycopg2_connect):
        """Test sql_test decorator with Redshift adapter."""
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

            # Define a test case with Redshift adapter
            @sql_test(
                adapter_type="redshift",
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
            def test_redshift_query():
                return TestCase(
                    query="SELECT id, name FROM users WHERE id = 1",
                    default_namespace="test_db",
                )

            # Execute the test
            results = test_redshift_query()

            # Verify results
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].id, 1)
            self.assertEqual(results[0].name, "Alice")

            # Verify Redshift was called
            mock_psycopg2_connect.assert_called_with(
                host="redshift-host.example.com",
                database="test_db",
                user="test_user",
                password="test_password",
                port=5439,
            )

            # Verify query was executed (once for CTE generation, once for actual query)
            self.assertEqual(self.mock_cursor.execute.call_count, 1)

            # Verify CTE was created properly
            execute_call = self.mock_cursor.execute.call_args[0][0]
            self.assertIn("SELECT id, name FROM", execute_call)
            self.assertIn("WHERE id = 1", execute_call)

        finally:
            # Restore original decorator
            pytest_plugin._sql_test_decorator = original_decorator_instance


if __name__ == "__main__":
    unittest.main()
