"""Integration tests for | None syntax support across all database adapters.

This test file specifically tests the modern Python 3.10+ syntax (X | None)
alongside the traditional Optional[X] syntax to ensure both work correctly.
"""

import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


# Only run these tests on Python 3.10+ where | None syntax is supported
PYTHON_310_PLUS = sys.version_info >= (3, 10)
pytestmark = pytest.mark.skipif(
    not PYTHON_310_PLUS, reason="Python 3.10+ required for | None syntax"
)


# Traditional dataclass with Optional syntax
@dataclass
class UserOptional:
    """User with traditional Optional syntax."""

    user_id: int
    name: str
    email: Optional[str] = None
    age: Optional[int] = None
    balance: Optional[Decimal] = None
    last_login: Optional[datetime] = None


# Pydantic model with | None syntax (Python 3.10+)
# We use exec to create this to avoid syntax errors on Python 3.9
if PYTHON_310_PLUS:
    exec(
        """
class UserPipeNone(BaseModel):
    \"\"\"User with modern | None syntax.\"\"\"

    user_id: int
    name: str
    email: str | None = None
    age: int | None = None
    balance: Decimal | None = None
    last_login: datetime | None = None


class UserResultPipeNone(BaseModel):
    \"\"\"Result with modern | None syntax.\"\"\"

    user_id: int
    name: str
    email: str | None
    age_group: str | None
    total_balance: Decimal | None
""",
        globals(),
    )


# Traditional result class with Optional
class UserResultOptional(BaseModel):
    """Result with traditional Optional syntax."""

    user_id: int
    name: str
    email: Optional[str]
    age_group: Optional[str]
    total_balance: Optional[Decimal]


class UsersOptionalMockTable(BaseMockTable):
    """Mock table for users with Optional syntax."""

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "users_optional"


class UsersPipeNoneMockTable(BaseMockTable):
    """Mock table for users with | None syntax."""

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "users_pipe_none"


@pytest.mark.integration
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestDuckDBUnionSyntax:
    """Test | None syntax with DuckDB adapter."""

    def test_optional_syntax_works(self, use_physical_tables):
        """Verify traditional Optional syntax still works."""

        @sql_test(
            adapter_type="duckdb",
            mock_tables=[
                UsersOptionalMockTable(
                    [
                        UserOptional(1, "Alice", "alice@example.com", 30, Decimal("1000.00")),
                        UserOptional(2, "Bob", None, 25, None),  # NULL values
                        UserOptional(3, "Carol", "carol@example.com", None, Decimal("500.00")),
                    ]
                )
            ],
            result_class=UserResultOptional,
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        CASE
                            WHEN age IS NULL THEN NULL
                            WHEN age < 25 THEN 'young'
                            WHEN age < 35 THEN 'adult'
                            ELSE 'senior'
                        END as age_group,
                        balance as total_balance
                    FROM test_db.users_optional
                """,
                default_namespace="test_db",
            )

        results = query_users()
        assert len(results) == 3
        assert results[0].email == "alice@example.com"
        assert results[1].email is None
        assert results[2].age_group is None

    def test_pipe_none_syntax_works(self, use_physical_tables):
        """Verify modern | None syntax works."""

        @sql_test(
            adapter_type="duckdb",
            mock_tables=[
                UsersPipeNoneMockTable(
                    [
                        globals()["UserPipeNone"](
                            user_id=1,
                            name="Alice",
                            email="alice@example.com",
                            age=30,
                            balance=Decimal("1000.00"),
                        ),
                        globals()["UserPipeNone"](
                            user_id=2, name="Bob", email=None, age=25, balance=None
                        ),
                        globals()["UserPipeNone"](
                            user_id=3,
                            name="Carol",
                            email="carol@example.com",
                            age=None,
                            balance=Decimal("500.00"),
                        ),
                    ]
                )
            ],
            result_class=globals()["UserResultPipeNone"],
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        CASE
                            WHEN age IS NULL THEN NULL
                            WHEN age < 25 THEN 'young'
                            WHEN age < 35 THEN 'adult'
                            ELSE 'senior'
                        END as age_group,
                        balance as total_balance
                    FROM test_db.users_pipe_none
                """,
                default_namespace="test_db",
            )

        results = query_users()
        assert len(results) == 3
        assert results[0].email == "alice@example.com"
        assert results[1].email is None
        assert results[2].age_group is None
        # Verify the | None syntax result is of the correct type
        assert type(results[0]).__name__ == "UserResultPipeNone"


@pytest.mark.integration
@pytest.mark.bigquery
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestBigQueryUnionSyntax:
    """Test | None syntax with BigQuery adapter."""

    def test_pipe_none_syntax_works(self, use_physical_tables):
        """Verify modern | None syntax works with BigQuery."""

        class BigQueryUsersTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test-project.test_dataset"

            def get_table_name(self) -> str:
                return "users"

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                BigQueryUsersTable(
                    [
                        globals()["UserPipeNone"](
                            user_id=1, name="Alice", email="alice@example.com", age=30
                        ),
                        globals()["UserPipeNone"](user_id=2, name="Bob", email=None, age=None),
                    ]
                )
            ],
            result_class=globals()["UserResultPipeNone"],
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        CASE
                            WHEN age IS NULL THEN NULL
                            WHEN age < 35 THEN 'adult'
                            ELSE 'senior'
                        END as age_group,
                        CAST(0 AS NUMERIC) as total_balance
                    FROM `test-project.test_dataset.users`
                """,
                default_namespace="test-project.test_dataset",
            )

        results = query_users()
        assert len(results) == 2
        assert results[1].email is None
        assert results[0].age_group == "adult"


@pytest.mark.integration
@pytest.mark.snowflake
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestSnowflakeUnionSyntax:
    """Test | None syntax with Snowflake adapter."""

    def test_pipe_none_syntax_works(self, use_physical_tables):
        """Verify modern | None syntax works with Snowflake."""

        class SnowflakeUsersTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "TEST_DB.TEST_SCHEMA"

            def get_table_name(self) -> str:
                return "USERS"

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[
                SnowflakeUsersTable(
                    [
                        globals()["UserPipeNone"](
                            user_id=1, name="Alice", email="alice@example.com"
                        ),
                        globals()["UserPipeNone"](user_id=2, name="Bob", email=None),
                    ]
                )
            ],
            result_class=globals()["UserResultPipeNone"],
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        NULL as age_group,
                        NULL as total_balance
                    FROM TEST_DB.TEST_SCHEMA.USERS
                """,
                default_namespace="TEST_DB.TEST_SCHEMA",
            )

        results = query_users()
        assert len(results) == 2
        assert results[0].email == "alice@example.com"
        assert results[1].email is None


@pytest.mark.integration
@pytest.mark.redshift
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestRedshiftUnionSyntax:
    """Test | None syntax with Redshift adapter."""

    def test_pipe_none_syntax_works(self, use_physical_tables):
        """Verify modern | None syntax works with Redshift."""

        class RedshiftUsersTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "users"

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                RedshiftUsersTable(
                    [
                        globals()["UserPipeNone"](
                            user_id=1, name="Alice", email="alice@example.com"
                        ),
                        globals()["UserPipeNone"](user_id=2, name="Bob", email=None),
                    ]
                )
            ],
            result_class=globals()["UserResultPipeNone"],
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        NULL as age_group,
                        NULL as total_balance
                    FROM test_db.users
                """,
                default_namespace="test_db",
            )

        results = query_users()
        assert len(results) == 2
        assert results[1].email is None


@pytest.mark.integration
@pytest.mark.athena
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestAthenaUnionSyntax:
    """Test | None syntax with Athena adapter."""

    def test_pipe_none_syntax_works(self, use_physical_tables):
        """Verify modern | None syntax works with Athena."""

        class AthenaUsersTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "users"

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                AthenaUsersTable(
                    [
                        globals()["UserPipeNone"](
                            user_id=1, name="Alice", email="alice@example.com"
                        ),
                        globals()["UserPipeNone"](user_id=2, name="Bob", email=None),
                    ]
                )
            ],
            result_class=globals()["UserResultPipeNone"],
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        NULL as age_group,
                        NULL as total_balance
                    FROM test_db.users
                """,
                default_namespace="test_db",
            )

        results = query_users()
        assert len(results) == 2
        assert results[0].email == "alice@example.com"


@pytest.mark.integration
@pytest.mark.trino
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestTrinoUnionSyntax:
    """Test | None syntax with Trino adapter."""

    def test_pipe_none_syntax_works(self, use_physical_tables):
        """Verify modern | None syntax works with Trino."""

        class TrinoUsersTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "memory.default"

            def get_table_name(self) -> str:
                return "users"

        @sql_test(
            adapter_type="trino",
            mock_tables=[
                TrinoUsersTable(
                    [
                        globals()["UserPipeNone"](
                            user_id=1, name="Alice", email="alice@example.com"
                        ),
                        globals()["UserPipeNone"](user_id=2, name="Bob", email=None),
                    ]
                )
            ],
            result_class=globals()["UserResultPipeNone"],
            use_physical_tables=use_physical_tables,
        )
        def query_users():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        email,
                        NULL as age_group,
                        NULL as total_balance
                    FROM memory.default.users
                """,
                default_namespace="memory.default",
            )

        results = query_users()
        assert len(results) == 2
        assert results[1].email is None
