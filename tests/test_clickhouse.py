"""Tests for using the SQL testing library with ClickHouse.

These tests need a live ClickHouse server. They are auto-skipped when
`clickhouse-connect` isn't installed or when no server is reachable at
the configured host/port. Configure via env vars:

    CLICKHOUSE_HOST      (default: localhost)
    CLICKHOUSE_PORT      (default: 8123)
    CLICKHOUSE_USERNAME  (default: default)
    CLICKHOUSE_PASSWORD  (default: empty)
    CLICKHOUSE_DATABASE  (default: default)
"""

import os
import unittest
from dataclasses import dataclass
from datetime import date

import pytest
from pydantic import BaseModel


clickhouse_connect = pytest.importorskip("clickhouse_connect")

from sql_testing_library import TestCase, sql_test  # noqa: E402
from sql_testing_library._adapters.clickhouse import ClickHouseAdapter  # noqa: E402
from sql_testing_library._core import SQLTestFramework  # noqa: E402
from sql_testing_library._mock_table import BaseMockTable  # noqa: E402


CH_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("CLICKHOUSE_USERNAME", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")
CH_DB = os.environ.get("CLICKHOUSE_DATABASE", "default")


def _clickhouse_reachable() -> bool:
    try:
        c = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB
        )
        c.query("SELECT 1")
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not _clickhouse_reachable(),
        reason=f"ClickHouse not reachable at {CH_HOST}:{CH_PORT}",
    ),
]


@dataclass
class User:
    id: int
    name: str
    email: str
    active: bool
    created_at: date


class UserResult(BaseModel):
    id: int
    name: str


class UsersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return CH_DB

    def get_table_name(self) -> str:
        return "users"


class TestClickHouseIntegration(unittest.TestCase):
    """Smoke tests exercising the framework end-to-end against ClickHouse."""

    def setUp(self):
        self.adapter = ClickHouseAdapter(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASS,
            database=CH_DB,
        )
        self.framework = SQLTestFramework(self.adapter)

    def _users(self):
        return [
            User(1, "Alice Johnson", "alice@example.com", True, date(2023, 1, 15)),
            User(2, "Bob Smith", "bob@example.com", False, date(2023, 2, 20)),
        ]

    def test_basic_query_cte_mode(self):
        sql_query = f"""
        SELECT id, name
        FROM {CH_DB}.users
        WHERE active = TRUE
        """
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(self._users())],
            default_namespace=CH_DB,
            result_class=dict,
        )
        result = self.framework.run_test(test_case)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Alice Johnson")

    def test_physical_tables_execution_mode(self):
        sql_query = f"""
        SELECT count() AS total_users FROM {CH_DB}.users
        """
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(self._users())],
            default_namespace=CH_DB,
            use_physical_tables=True,
            result_class=dict,
        )
        result = self.framework.run_test(test_case)
        self.assertEqual(result[0]["total_users"], 2)

    def test_array_functions(self):
        sql_query = f"""
        SELECT
            id,
            name,
            arrayJoin([id, id * 2, id * 3]) AS multiplier
        FROM {CH_DB}.users
        WHERE id = 1
        ORDER BY multiplier
        """
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(self._users())],
            default_namespace=CH_DB,
            result_class=dict,
        )
        result = self.framework.run_test(test_case)
        self.assertEqual([r["multiplier"] for r in result], [1, 2, 3])

    def test_analytical_functions(self):
        sql_query = f"""
        SELECT
            name,
            active,
            row_number() OVER (ORDER BY id) AS row_num,
            count() OVER (PARTITION BY active) AS active_count
        FROM {CH_DB}.users
        ORDER BY id
        """
        test_case = TestCase(
            query=sql_query,
            mock_tables=[UsersMockTable(self._users())],
            default_namespace=CH_DB,
            result_class=dict,
        )
        result = self.framework.run_test(test_case)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["row_num"], 1)

    def test_adapter_properties(self):
        self.assertEqual(self.adapter.get_sqlglot_dialect(), "clickhouse")
        self.assertIsNone(self.adapter.get_query_size_limit())
        # Direct query works via the underlying client
        result = self.adapter.client.query("SELECT 1 AS x").result_rows
        self.assertEqual(result, [(1,)])

    def test_decorator_is_callable(self):
        self.assertTrue(callable(sql_test))


if __name__ == "__main__":
    unittest.main()
