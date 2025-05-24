"""
Example SQL tests using the library.
"""

import unittest
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library.exceptions import MockTableNotFoundError
from sql_testing_library.mock_table import BaseMockTable


# Define dataclasses for test data
@dataclass
class User:
    user_id: int
    name: str
    email: str
    created_date: date

    @classmethod
    def fake(cls):
        """Generate fake user data."""
        import random

        return cls(
            user_id=random.randint(1, 10000),
            name=f"User{random.randint(1, 1000)}",
            email=f"user{random.randint(1, 1000)}@example.com",
            created_date=date(2023, random.randint(1, 12), random.randint(1, 28)),
        )


@dataclass
class Order:
    order_id: int
    user_id: int
    amount: Decimal
    order_date: date

    @classmethod
    def fake(cls):
        """Generate fake order data."""
        import random

        return cls(
            order_id=random.randint(10000, 99999),
            user_id=random.randint(1, 100),
            amount=Decimal(f"{random.uniform(10, 500):.2f}"),
            order_date=date(2023, random.randint(1, 12), random.randint(1, 28)),
        )


# Define result classes (Pydantic models)
class UserOrderSummary(BaseModel):
    user_id: int
    name: str
    total_orders: int
    total_amount: Decimal


class MonthlyRevenue(BaseModel):
    month: int
    revenue: Decimal


class UserWithOptionalEmail(BaseModel):
    user_id: int
    name: str
    email: Optional[str] = None


class TypeTestResult(BaseModel):
    user_id: int
    name: str
    created_year: int
    total_amount: Decimal
    is_active: bool


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


# Test cases with assertions
class TestUserAnalytics(unittest.TestCase):
    """A collection of SQL test cases for user analytics."""

    def test_user_order_summary(self):
        """Test that calculates order summary per user."""

        # Define the SQL test
        @sql_test(
            mock_tables=[
                UsersMockTable(
                    [
                        User(1, "Alice", "alice@example.com", date(2023, 1, 1)),
                        User(2, "Bob", "bob@example.com", date(2023, 1, 2)),
                        User(3, "Charlie", "charlie@example.com", date(2023, 1, 3)),
                    ]
                ),
                OrdersMockTable(
                    [
                        Order(101, 1, Decimal("100.00"), date(2023, 2, 1)),
                        Order(102, 1, Decimal("150.00"), date(2023, 2, 2)),
                        Order(103, 2, Decimal("200.00"), date(2023, 2, 3)),
                        Order(104, 3, Decimal("75.00"), date(2023, 2, 4)),
                    ]
                ),
            ],
            result_class=UserOrderSummary,
            adapter_type="bigquery",
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
                    JOIN bigquery-public-data.analytics_db.orders o
                        ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database="analytics_db",
            )

        # Execute the test and get results
        results = execute_test()

        # Assertions on the results
        assert len(results) == 3, f"Expected 3 users, got {len(results)}"

        # Check Alice's summary (user_id=1)
        alice = results[0]
        assert alice.user_id == 1
        assert alice.name == "Alice"
        assert alice.total_orders == 2
        assert alice.total_amount == Decimal("250.00")

        # Check Bob's summary (user_id=2)
        bob = results[1]
        assert bob.user_id == 2
        assert bob.name == "Bob"
        assert bob.total_orders == 1
        assert bob.total_amount == Decimal("200.00")

        # Check Charlie's summary (user_id=3)
        charlie = results[2]
        assert charlie.user_id == 3
        assert charlie.name == "Charlie"
        assert charlie.total_orders == 1
        assert charlie.total_amount == Decimal("75.00")

    def test_monthly_revenue(self):
        """Test monthly revenue calculation with specific test data."""
        # Use specific test data instead of random for predictable assertions
        test_orders = [
            Order(101, 1, Decimal("100.00"), date(2023, 1, 15)),  # January
            Order(102, 2, Decimal("200.00"), date(2023, 1, 20)),  # January
            Order(103, 3, Decimal("150.00"), date(2023, 2, 10)),  # February
            Order(104, 4, Decimal("300.00"), date(2023, 2, 25)),  # February
            Order(105, 5, Decimal("75.00"), date(2023, 3, 5)),  # March
        ]

        @sql_test(
            mock_tables=[OrdersMockTable(test_orders)], result_class=MonthlyRevenue
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        EXTRACT(MONTH FROM order_date) as month,
                        SUM(amount) as revenue
                    FROM bigquery-public-data.analytics_db.orders
                    GROUP BY EXTRACT(MONTH FROM order_date)
                    ORDER BY month
                """,
                execution_database="analytics_db",
                adapter_type="bigquery",
            )

        # Execute and verify results
        results = execute_test()

        # Should have 3 months
        assert len(results) == 3, f"Expected 3 months, got {len(results)}"

        # Verify January (month=1)
        january = results[0]
        assert january.month == 1
        assert january.revenue == Decimal("300.00")  # 100 + 200

        # Verify February (month=2)
        february = results[1]
        assert february.month == 2
        assert february.revenue == Decimal("450.00")  # 150 + 300

        # Verify March (month=3)
        march = results[2]
        assert march.month == 3
        assert march.revenue == Decimal("75.00")

    def test_single_user_query(self):
        """Test querying a single user."""

        @sql_test(
            mock_tables=[
                UsersMockTable(
                    [User(42, "Test User", "test@example.com", date(2023, 1, 1))]
                )
            ],
            result_class=UserOrderSummary,
            use_physical_tables=True,  # Test physical tables option
            adapter_type="bigquery",
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        0 as total_orders,
                        0 as total_amount
                    FROM bigquery-public-data.analytics_db.users
                    WHERE user_id = 42
                """,
                execution_database="test_db",  # Different database
            )

        results = execute_test()

        # Verify single result
        assert len(results) == 1
        user = results[0]
        assert user.user_id == 42
        assert user.name == "Test User"
        assert user.total_orders == 0
        assert user.total_amount == Decimal("0")

    def test_empty_result_set(self):
        """Test handling of empty result sets."""

        @sql_test(
            mock_tables=[
                UsersMockTable(
                    [User(1, "Alice", "alice@example.com", date(2023, 1, 1))]
                )
            ],
            result_class=UserOrderSummary,
            adapter_type="bigquery",
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        0 as total_orders,
                        0 as total_amount
                    FROM bigquery-public-data.analytics_db.users
                    WHERE user_id = 999  -- No matching user
                """,
                execution_database="analytics_db",
            )

        results = execute_test()

        # Should return empty list
        assert len(results) == 0
        assert results == []

    def test_null_handling(self):
        """Test handling of NULL values in results."""

        @sql_test(
            mock_tables=[
                UsersMockTable(
                    [
                        User(1, "Alice", "alice@example.com", date(2023, 1, 1)),
                        User(2, "Bob", "bob@example.com", date(2023, 1, 2)),
                    ]
                )
            ],
            result_class=UserWithOptionalEmail,
            adapter_type="bigquery",
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        user_id,
                        name,
                        CASE
                            WHEN user_id = 1 THEN email
                            ELSE NULL
                        END as email
                    FROM bigquery-public-data.analytics_db.users
                    ORDER BY user_id
                """,
                execution_database="analytics_db",
            )

        results = execute_test()

        assert len(results) == 2

        # Alice should have email
        alice = results[0]
        assert alice.user_id == 1
        assert alice.name == "Alice"
        assert alice.email == "alice@example.com"

        # Bob should have null email
        bob = results[1]
        assert bob.user_id == 2
        assert bob.name == "Bob"
        assert bob.email is None

    def test_type_conversion(self):
        """Test various data type conversions."""

        @sql_test(
            mock_tables=[
                UsersMockTable(
                    [User(1, "Alice", "alice@example.com", date(2023, 1, 1))]
                ),
                OrdersMockTable([Order(101, 1, Decimal("123.45"), date(2023, 2, 1))]),
            ],
            result_class=TypeTestResult,
            adapter_type="bigquery",
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.name,
                        EXTRACT(YEAR FROM u.created_date) as created_year,
                        o.amount as total_amount,
                        TRUE as is_active
                    FROM bigquery-public-data.analytics_db.users u
                    JOIN bigquery-public-data.analytics_db.orders o
                        ON u.user_id = o.user_id
                """,
                execution_database="analytics_db",
            )

        results = execute_test()

        assert len(results) == 1
        result = results[0]

        # Verify types are properly converted
        assert isinstance(result.user_id, int)
        assert isinstance(result.name, str)
        assert isinstance(result.created_year, int)
        assert isinstance(result.total_amount, Decimal)
        assert isinstance(result.is_active, bool)

        # Verify values
        assert result.user_id == 1
        assert result.name == "Alice"
        assert result.created_year == 2023
        assert result.total_amount == Decimal("123.45")
        assert result.is_active is True

    def test_missing_mock_table_error(self):
        """Test that missing mock tables raise appropriate errors."""

        @sql_test(
            mock_tables=[
                # Only provide users table, but query needs orders too
                UsersMockTable(
                    [User(1, "Alice", "alice@example.com", date(2023, 1, 1))]
                )
            ],
            result_class=UserOrderSummary,
            adapter_type="bigquery",
        )
        def execute_test():
            return TestCase(
                query="""
                    SELECT u.user_id, u.name, COUNT(o.order_id) as total_orders
                    FROM bigquery-public-data.analytics_db.users u
                    JOIN bigquery-public-data.analytics_db.orders o
                        ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                """,
                execution_database="analytics_db",
            )

        # Should raise MockTableNotFoundError
        with self.assertRaises(MockTableNotFoundError):
            execute_test()
