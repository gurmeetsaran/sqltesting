"""
Examples of improved usage patterns for SQL tests.
"""
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
import unittest

from pydantic import BaseModel

from sql_testing_library import sql_test, TestCase
from sql_testing_library.mock_table import BaseMockTable


# Define dataclasses for test data
@dataclass
class User:
    user_id: int
    name: str
    email: str
    created_date: date


@dataclass
class Order:
    order_id: int
    user_id: int
    amount: Decimal
    order_date: date


# Define result classes (Pydantic models)
class UserOrderSummary(BaseModel):
    user_id: int
    name: str
    total_orders: int
    total_amount: Decimal


# Define mock table classes
class UsersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "bigquery-public-data.analytics_db"

    def get_table_name(self) -> str:
        return "users"


class OrdersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "bigquery-public-data.analytics_db"

    def get_table_name(self) -> str:
        return "orders"


# Test data
test_users = [
    User(1, "Alice", "alice@example.com", date(2023, 1, 1)),
    User(2, "Bob", "bob@example.com", date(2023, 1, 2)),
    User(3, "Charlie", "charlie@example.com", date(2023, 1, 3))
]

test_orders = [
    Order(101, 1, Decimal("100.00"), date(2023, 2, 1)),
    Order(102, 1, Decimal("150.00"), date(2023, 2, 2)),
    Order(103, 2, Decimal("200.00"), date(2023, 2, 3)),
    Order(104, 3, Decimal("75.00"), date(2023, 2, 4))
]


class TestImprovedPatterns(unittest.TestCase):
    """Test class demonstrating different usage patterns for SQL tests."""

    def test_pattern_1_decorator_only(self):
        """Pattern 1: Providing all settings in the decorator."""
        
        @sql_test(
            mock_tables=[
                UsersMockTable(test_users),
                OrdersMockTable(test_orders)
            ],
            result_class=UserOrderSummary
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM bigquery-public-data.analytics_db.users u
                    LEFT JOIN bigquery-public-data.analytics_db.orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database="analytics_db"
            )
        
        results = execute_test()
        # Assertions
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].user_id, 1)
        self.assertEqual(results[0].total_orders, 2)
        self.assertEqual(results[0].total_amount, Decimal("250.00"))

    def test_pattern_2_testcase_only(self):
        """Pattern 2: Providing all settings in the TestCase (require empty decorator)."""

        @sql_test()  # Empty decorator
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM bigquery-public-data.analytics_db.users u
                    LEFT JOIN bigquery-public-data.analytics_db.orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database="analytics_db",
                mock_tables=[
                    UsersMockTable(test_users),
                    OrdersMockTable(test_orders)
                ],
                result_class=UserOrderSummary
            )

        results = execute_test()

        # Assertions
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].user_id, 1)
        self.assertEqual(results[0].total_orders, 2)
        self.assertEqual(results[0].total_amount, Decimal("250.00"))

    def test_pattern_3_mix_and_match(self):
        """Pattern 3: Mix and match decorator and TestCase values."""

        @sql_test(
            mock_tables=[
                UsersMockTable(test_users),
                OrdersMockTable(test_orders)
            ]
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM bigquery-public-data.analytics_db.users u
                    LEFT JOIN bigquery-public-data.analytics_db.orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database="analytics_db",
                # We define result_class here instead of in decorator
                result_class=UserOrderSummary
            )

        results = execute_test()

        # Assertions
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].user_id, 1)
        self.assertEqual(results[0].total_orders, 2)
        self.assertEqual(results[0].total_amount, Decimal("250.00"))