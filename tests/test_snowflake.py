"""Tests for Snowflake adapter and query transformations."""

import unittest
from unittest import mock

import pandas as pd

from sql_testing_library.adapters.snowflake import SnowflakeAdapter
from sql_testing_library.core import SQLTester, mock_table


class TestSnowflakeBasic(unittest.TestCase):
    """Basic tests for Snowflake adapter integration."""

    @mock.patch("snowflake.connector.connect")
    def test_simple_query(self, mock_snowflake_connect):
        """Test a simple query using the Snowflake adapter."""
        # Mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Mock execute response - first for the CTE query, then for table cleanup
        def execute_side_effect(query):
            if "WITH data AS" in query:
                mock_cursor.description = [("id",), ("name",), ("active",)]
                mock_cursor.fetchall.return_value = [
                    (1, "Alice", True),
                    (2, "Bob", False),
                ]
            else:
                mock_cursor.description = None
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = execute_side_effect

        # Create test data
        class User:
            def __init__(self, id: int, name: str, active: bool):
                self.id = id
                self.name = name
                self.active = active

        users = [
            User(1, "Alice", True),
            User(2, "Bob", False),
        ]

        # Create adapter and tester
        adapter = SnowflakeAdapter(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
        )

        tester = SQLTester(adapter)

        # Define a simple query using CTE
        query = """
        WITH data AS (
            SELECT * FROM users
        )
        SELECT id, name, active FROM data
        """

        # Create mock table
        users_table = mock_table(users)

        # Run test
        result = tester.run(query, {"users": users_table}, use_cte=True)

        # Verify query execution
        self.assertTrue(mock_cursor.execute.called)
        query_arg = mock_cursor.execute.call_args_list[0][0][0]
        self.assertIn("WITH data AS", query_arg)

        # Check result format
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.columns), ["id", "name", "active"])

    @mock.patch("snowflake.connector.connect")
    def test_cte_transformation(self, mock_snowflake_connect):
        """Test CTE transformation with the Snowflake adapter."""
        # Mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Mock execute response
        def execute_side_effect(query):
            if "WITH" in query:
                mock_cursor.description = [("total_active",)]
                mock_cursor.fetchall.return_value = [(1,)]
            else:
                mock_cursor.description = None
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = execute_side_effect

        # Create test data
        class User:
            def __init__(self, id: int, name: str, active: bool):
                self.id = id
                self.name = name
                self.active = active

        users = [
            User(1, "Alice", True),
            User(2, "Bob", False),
        ]

        # Create adapter and tester
        adapter = SnowflakeAdapter(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
        )

        tester = SQLTester(adapter)

        # Define query that counts active users
        query = """
        SELECT COUNT(*) AS total_active
        FROM users
        WHERE active = TRUE
        """

        # Create mock table
        users_table = mock_table(users)

        # Run test with CTE transformation
        result = tester.run(query, {"users": users_table}, use_cte=True)

        # Verify a CTE query was executed
        self.assertTrue(mock_cursor.execute.called)
        query_arg = mock_cursor.execute.call_args_list[0][0][0]
        self.assertIn("WITH", query_arg)
        self.assertIn("users", query_arg)

        # Check result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(list(result.columns), ["total_active"])
        self.assertEqual(result.iloc[0]["total_active"], 1)


if __name__ == "__main__":
    unittest.main()
