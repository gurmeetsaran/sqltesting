"""Tests for using the SQL testing library with DuckDB."""

import unittest
from dataclasses import dataclass
from datetime import date

from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._adapters.duckdb import DuckDBAdapter
from sql_testing_library._core import SQLTestFramework
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class User:
    """Test user data class."""

    id: int
    name: str
    email: str
    active: bool
    created_at: date


@dataclass
class ComplexUser:
    """Test user data class with complex types."""

    id: int
    name: str
    metadata: dict  # Will be converted to MAP
    scores: list  # Will be converted to array


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


class ComplexUsersMockTable(BaseMockTable):
    """Mock table for complex user data."""

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "complex_users"


class TestDuckDBIntegration(unittest.TestCase):
    """Test DuckDB integration with the SQL testing library."""

    def setUp(self):
        """Set up test environment."""
        self.adapter = DuckDBAdapter(database=":memory:")
        self.framework = SQLTestFramework(self.adapter)

    def test_duckdb_basic_query(self):
        """Test basic DuckDB query execution."""

        # Mock data
        users_data = [
            User(
                id=1,
                name="Alice Johnson",
                email="alice@example.com",
                active=True,
                created_at=date(2023, 1, 15),
            ),
            User(
                id=2,
                name="Bob Smith",
                email="bob@example.com",
                active=False,
                created_at=date(2023, 2, 20),
            ),
        ]

        # SQL query to test
        sql_query = """
        SELECT id, name
        FROM test_db.users
        WHERE active = true
        """

        # Create test case
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(users_data)],
            default_namespace="test_db",
            result_class=dict,
        )

        # Execute test using framework
        result = self.framework.run_test(test_case)

        # Verify result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Alice Johnson")

    def test_duckdb_array_types(self):
        """Test DuckDB with simple array operations."""

        users_data = [
            User(
                id=1,
                name="Alice",
                email="alice@example.com",
                active=True,
                created_at=date(2023, 1, 15),
            ),
        ]

        # SQL query with array creation
        sql_query = """
        SELECT
            id,
            name,
            [id, id * 2, id * 3] as multipliers
        FROM test_db.users
        WHERE id = 1
        """

        # Create test case
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(users_data)],
            default_namespace="test_db",
            result_class=dict,
        )

        # Execute test and check result
        result = self.framework.run_test(test_case)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Alice")

    def test_duckdb_cte_execution_mode(self):
        """Test DuckDB with CTE execution mode."""

        users_data = [
            User(
                id=1,
                name="Alice",
                email="alice@example.com",
                active=True,
                created_at=date(2023, 1, 15),
            ),
        ]

        # SQL with CTE
        sql_query = """
        WITH active_users AS (
            SELECT id, name, email
            FROM test_db.users
            WHERE active = true
        )
        SELECT * FROM active_users
        """

        # Create test case with CTE execution mode (default is CTE)
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(users_data)],
            default_namespace="test_db",
            result_class=dict,
        )

        # Execute test
        result = self.framework.run_test(test_case)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Alice")

    def test_duckdb_physical_tables_execution_mode(self):
        """Test DuckDB with physical tables execution mode."""

        users_data = [
            User(
                id=1,
                name="Alice",
                email="alice@example.com",
                active=True,
                created_at=date(2023, 1, 15),
            ),
            User(
                id=2,
                name="Bob",
                email="bob@example.com",
                active=False,
                created_at=date(2023, 2, 20),
            ),
        ]

        # SQL query
        sql_query = """
        SELECT COUNT(*) as total_users
        FROM test_db.users
        """

        # Create test case with physical tables execution mode
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(users_data)],
            default_namespace="test_db",
            use_physical_tables=True,
            result_class=dict,
        )

        # Execute test
        result = self.framework.run_test(test_case)
        self.assertEqual(result[0]["total_users"], 2)

    def test_duckdb_decorator_simple(self):
        """Test DuckDB decorator functionality is available (simple test)."""
        # Just test that the decorator exists and can be imported
        self.assertTrue(callable(sql_test))

    def test_duckdb_struct_types(self):
        """Test DuckDB STRUCT type functionality."""

        # SQL query that creates and queries struct types
        sql_query = """
        SELECT
            id,
            {'name': name, 'active': active} as user_info,
            [id, id * 2, id * 3] as multipliers
        FROM test_db.users
        WHERE id = 1
        """

        users_data = [
            User(
                id=1,
                name="Alice",
                email="alice@example.com",
                active=True,
                created_at=date(2023, 1, 15),
            ),
        ]

        # Create test case
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(users_data)],
            default_namespace="test_db",
            result_class=dict,
        )

        # Execute test
        result = self.framework.run_test(test_case)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        # Check struct access
        user_info = result[0]["user_info"]
        self.assertEqual(user_info["name"], "Alice")
        self.assertEqual(user_info["active"], True)

    def test_duckdb_analytical_functions(self):
        """Test DuckDB analytical functions."""

        users_data = [
            User(
                id=1,
                name="Alice",
                email="alice@example.com",
                active=True,
                created_at=date(2023, 1, 15),
            ),
            User(
                id=2,
                name="Bob",
                email="bob@example.com",
                active=True,
                created_at=date(2023, 2, 20),
            ),
            User(
                id=3,
                name="Charlie",
                email="charlie@example.com",
                active=False,
                created_at=date(2023, 3, 25),
            ),
        ]

        # SQL with analytical functions
        sql_query = """
        SELECT
            name,
            active,
            ROW_NUMBER() OVER (ORDER BY id) as row_num,
            COUNT(*) OVER (PARTITION BY active) as active_count
        FROM test_db.users
        ORDER BY id
        """

        # Create test case
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(users_data)],
            default_namespace="test_db",
            result_class=dict,
        )

        # Execute test
        result = self.framework.run_test(test_case)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["row_num"], 1)
        self.assertEqual(result[0]["active_count"], 2)  # 2 active users
        self.assertEqual(result[2]["active_count"], 1)  # 1 inactive user

    def test_duckdb_adapter_properties(self):
        """Test DuckDB adapter properties and configuration."""
        # Test that adapter is properly configured
        self.assertEqual(self.adapter.database, ":memory:")
        self.assertEqual(self.adapter.__class__.__name__, "DuckDBAdapter")

        # Test connection works by executing a simple query
        import duckdb

        result = duckdb.connect(":memory:").execute("SELECT 1 as test").fetchdf()
        self.assertEqual(result.iloc[0]["test"], 1)

    def test_duckdb_file_database(self):
        """Test DuckDB with file-based database."""
        import os
        import tempfile

        # Create temporary file path
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            db_path = tmp_file.name

        # Remove the file so DuckDB can create it properly
        os.unlink(db_path)

        try:
            # Create adapter with file database
            file_adapter = DuckDBAdapter(database=db_path)
            file_framework = SQLTestFramework(file_adapter)

            users_data = [
                User(
                    id=1,
                    name="Alice",
                    email="alice@example.com",
                    active=True,
                    created_at=date(2023, 1, 15),
                ),
            ]

            test_case = TestCase(
                query="SELECT COUNT(*) as total FROM test_db.users",
                mock_tables=[UsersMockTable(users_data)],
                default_namespace="test_db",
                use_physical_tables=True,
                result_class=dict,
            )

            result = file_framework.run_test(test_case)
            self.assertEqual(result[0]["total"], 1)

        finally:
            # Clean up
            try:
                os.unlink(db_path)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()
