"""Integration tests for BigQuery adapter with pytest configuration."""

import os
import unittest
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._exceptions import MockTableNotFoundError
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class User:
    """Test user data class for integration tests."""

    user_id: int
    name: str
    email: str
    created_date: date
    is_premium: bool = False
    lifetime_value: Optional[Decimal] = None


@dataclass
class Order:
    """Test order data class for integration tests."""

    order_id: int
    user_id: int
    amount: Decimal
    order_date: date
    status: str = "completed"


class UserResult(BaseModel):
    """Result model for user queries."""

    user_id: int
    name: str
    email: str
    total_orders: int
    total_amount: Decimal


class UserOrderSummary(BaseModel):
    """Result model for user order summary queries."""

    user_id: int
    name: str
    total_orders: int
    total_amount: Decimal


class MonthlyRevenue(BaseModel):
    """Result model for monthly revenue aggregation."""

    month: int
    revenue: Decimal


class UserAnalyticsResult(BaseModel):
    """Result model for complex user analytics query."""

    user_id: int
    name: str
    is_premium: bool
    order_count: int
    total_spent: Decimal
    avg_order_value: Decimal
    spending_rank: int
    user_tier: str


class NullHandlingResult(BaseModel):
    """Result model for null handling query."""

    user_id: int
    name: str
    email_status: str
    lifetime_value_filled: Decimal
    value_category: str


class LargeDatasetResult(BaseModel):
    """Result model for large dataset aggregation."""

    unique_users: int
    total_orders: int
    avg_order_amount: Decimal
    max_order_amount: Decimal
    min_order_amount: Decimal


class TypeTestResult(BaseModel):
    """Result model for type conversion tests."""

    user_id: int
    name: str
    created_year: int
    total_amount: Decimal
    is_active: bool


class UserWithOptionalEmail(BaseModel):
    """Result model for optional email tests."""

    user_id: int
    name: str
    email: Optional[str] = None


class UsersMockTable(BaseMockTable):
    """Mock table for user data."""

    def get_database_name(self) -> str:
        project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
        database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
        return f"{project_id}.{database}"

    def get_table_name(self) -> str:
        return "users"


class OrdersMockTable(BaseMockTable):
    """Mock table for order data."""

    def get_database_name(self) -> str:
        project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
        database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
        return f"{project_id}.{database}"

    def get_table_name(self) -> str:
        return "orders"


@pytest.mark.integration
@pytest.mark.bigquery
class TestBigQueryIntegration(unittest.TestCase):
    """Integration tests for BigQuery adapter using pytest configuration."""

    def test_simple_user_query(self):
        """Test simple user query with BigQuery adapter."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable(
                    [
                        User(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                            Decimal("2500.00"),
                        ),
                        User(
                            2,
                            "Bob Smith",
                            "bob@example.com",
                            date(2023, 2, 20),
                            False,
                            Decimal("150.00"),
                        ),
                        User(
                            3,
                            "Carol Davis",
                            "carol@example.com",
                            date(2023, 3, 10),
                            True,
                            Decimal("3200.00"),
                        ),
                    ]
                )
            ],
            result_class=User,
        )
        def query_premium_users():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        user_id, name, email, created_date,
                        is_premium, lifetime_value
                    FROM {project_id}.{database}.users
                    WHERE is_premium = TRUE
                    ORDER BY lifetime_value DESC
                """,
                execution_database=database,
            )

        results = query_premium_users()

        assert len(results) == 2
        assert results[0].name == "Carol Davis"
        assert results[0].lifetime_value == Decimal("3200.00")
        assert results[1].name == "Alice Johnson"
        assert results[1].lifetime_value == Decimal("2500.00")

    def test_user_order_join_query(self):
        """Test join query between users and orders."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable(
                    [
                        User(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                        ),
                        User(2, "Bob Smith", "bob@example.com", date(2023, 2, 20), False),
                        User(3, "Charlie Brown", "charlie@example.com", date(2023, 1, 3), False),
                    ]
                ),
                OrdersMockTable(
                    [
                        Order(
                            101,
                            1,
                            Decimal("100.00"),
                            date(2023, 2, 1),
                            "completed",
                        ),
                        Order(
                            102,
                            1,
                            Decimal("150.00"),
                            date(2023, 2, 2),
                            "completed",
                        ),
                        Order(
                            103,
                            2,
                            Decimal("200.00"),
                            date(2023, 2, 3),
                            "completed",
                        ),
                        Order(
                            104,
                            3,
                            Decimal("75.00"),
                            date(2023, 2, 4),
                            "completed",
                        ),
                    ]
                ),
            ],
            result_class=UserOrderSummary,
        )
        def query_user_order_summary():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM {project_id}.{database}.users u
                    JOIN {project_id}.{database}.orders o
                        ON u.user_id = o.user_id
                    WHERE o.status = 'completed'
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database=database,
            )

        results = query_user_order_summary()

        assert len(results) == 3
        assert results[0].user_id == 1
        assert results[0].name == "Alice Johnson"
        assert results[0].total_orders == 2
        assert results[0].total_amount == Decimal("250.00")
        assert results[1].user_id == 2
        assert results[1].name == "Bob Smith"
        assert results[1].total_orders == 1
        assert results[1].total_amount == Decimal("200.00")

    def test_aggregation_with_date_functions(self):
        """Test aggregation queries with date functions."""

        test_orders = [
            Order(101, 1, Decimal("100.00"), date(2023, 1, 15), "completed"),
            Order(102, 2, Decimal("200.00"), date(2023, 1, 20), "completed"),
            Order(103, 3, Decimal("150.00"), date(2023, 2, 10), "completed"),
            Order(104, 4, Decimal("300.00"), date(2023, 2, 25), "completed"),
            Order(105, 5, Decimal("75.00"), date(2023, 3, 5), "completed"),
        ]

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[OrdersMockTable(test_orders)],
            result_class=MonthlyRevenue,
        )
        def query_monthly_revenue():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        EXTRACT(MONTH FROM order_date) as month,
                        SUM(amount) as revenue
                    FROM {project_id}.{database}.orders
                    WHERE status = 'completed'
                    GROUP BY EXTRACT(MONTH FROM order_date)
                    ORDER BY month
                """,
                execution_database=database,
            )

        results = query_monthly_revenue()

        assert len(results) == 3

        # January (month=1)
        january = results[0]
        assert january.month == 1
        assert january.revenue == Decimal("300.00")  # 100 + 200

        # February (month=2)
        february = results[1]
        assert february.month == 2
        assert february.revenue == Decimal("450.00")  # 150 + 300

        # March (month=3)
        march = results[2]
        assert march.month == 3
        assert march.revenue == Decimal("75.00")

    @pytest.mark.slow
    def test_complex_analytical_query(self):
        """Test complex analytical query with window functions."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable(
                    [
                        User(
                            1,
                            "Alice",
                            "alice@example.com",
                            date(2023, 1, 1),
                            True,
                            Decimal("1000"),
                        ),
                        User(
                            2,
                            "Bob",
                            "bob@example.com",
                            date(2023, 1, 15),
                            False,
                            Decimal("500"),
                        ),
                        User(
                            3,
                            "Carol",
                            "carol@example.com",
                            date(2023, 2, 1),
                            True,
                            Decimal("1500"),
                        ),
                    ]
                ),
                OrdersMockTable(
                    [
                        Order(1, 1, Decimal("100"), date(2023, 3, 1), "completed"),
                        Order(2, 1, Decimal("200"), date(2023, 3, 15), "completed"),
                        Order(3, 2, Decimal("50"), date(2023, 3, 10), "completed"),
                        Order(4, 3, Decimal("300"), date(2023, 3, 20), "completed"),
                        Order(5, 3, Decimal("250"), date(2023, 4, 1), "completed"),
                    ]
                ),
            ],
            result_class=UserAnalyticsResult,
        )
        def query_user_analytics():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    WITH user_metrics AS (
                        SELECT
                            u.user_id,
                            u.name,
                            u.is_premium,
                            u.lifetime_value,
                            COUNT(o.order_id) as order_count,
                            SUM(o.amount) as total_spent,
                            AVG(o.amount) as avg_order_value,
                            ROW_NUMBER() OVER (ORDER BY SUM(o.amount) DESC)
                                as spending_rank
                        FROM {project_id}.{database}.users u
                        LEFT JOIN {project_id}.{database}.orders o
                            ON u.user_id = o.user_id AND o.status = 'completed'
                        GROUP BY u.user_id, u.name, u.is_premium, u.lifetime_value
                    )
                    SELECT
                        user_id,
                        name,
                        is_premium,
                        order_count,
                        total_spent,
                        avg_order_value,
                        spending_rank,
                        CASE
                            WHEN spending_rank <= 1 THEN 'Top Spender'
                            WHEN spending_rank <= 2 THEN 'High Spender'
                            ELSE 'Regular Spender'
                        END as user_tier
                    FROM user_metrics
                    ORDER BY spending_rank
                """,
                execution_database=database,
            )

        results = query_user_analytics()

        assert len(results) == 3

        # Top spender should be Carol (order_count=2, total_spent=550)
        top_spender = results[0]
        assert top_spender.name == "Carol"
        assert top_spender.total_spent == Decimal("550")
        assert top_spender.user_tier == "Top Spender"

    def test_null_handling_and_edge_cases(self):
        """Test handling of NULL values and edge cases."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable(
                    [
                        User(
                            1,
                            "Alice",
                            "alice@example.com",
                            date(2023, 1, 1),
                            True,
                            None,
                        ),
                        User(
                            2,
                            "Bob",
                            "bob@example.com",
                            date(2023, 1, 15),
                            False,
                            Decimal("0"),
                        ),
                        User(3, "Carol", "", date(2023, 2, 1), True, Decimal("1500.50")),
                    ]
                )
            ],
            result_class=NullHandlingResult,
        )
        def query_null_handling():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        user_id,
                        name,
                        CASE
                            WHEN email = '' THEN 'No Email'
                            ELSE email
                        END as email_status,
                        COALESCE(lifetime_value, 0) as lifetime_value_filled,
                        CASE
                            WHEN lifetime_value IS NULL THEN 'Unknown'
                            WHEN lifetime_value = 0 THEN 'Zero'
                            ELSE 'Has Value'
                        END as value_category
                    FROM {project_id}.{database}.users
                    ORDER BY user_id
                """,
                execution_database=database,
            )

        results = query_null_handling()

        assert len(results) == 3

        # Alice - NULL lifetime_value
        assert results[0].lifetime_value_filled == Decimal("0")
        assert results[0].value_category == "Unknown"

        # Bob - Zero lifetime_value
        assert results[1].lifetime_value_filled == Decimal("0")
        assert results[1].value_category == "Zero"

        # Carol - Empty email, has lifetime_value
        assert results[2].email_status == "No Email"
        assert results[2].lifetime_value_filled == Decimal("1500.50")
        assert results[2].value_category == "Has Value"

    def test_type_conversion(self):
        """Test various data type conversions."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable([User(1, "Alice", "alice@example.com", date(2023, 1, 1))]),
                OrdersMockTable([Order(101, 1, Decimal("123.45"), date(2023, 2, 1))]),
            ],
            result_class=TypeTestResult,
        )
        def query_type_conversion():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        u.user_id,
                        u.name,
                        EXTRACT(YEAR FROM u.created_date) as created_year,
                        o.amount as total_amount,
                        TRUE as is_active
                    FROM {project_id}.{database}.users u
                    JOIN {project_id}.{database}.orders o
                        ON u.user_id = o.user_id
                """,
                execution_database=database,
            )

        results = query_type_conversion()

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

    def test_empty_result_set(self):
        """Test handling of empty result sets."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[UsersMockTable([User(1, "Alice", "alice@example.com", date(2023, 1, 1))])],
            result_class=UserOrderSummary,
        )
        def query_empty_results():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        user_id,
                        name,
                        0 as total_orders,
                        0 as total_amount
                    FROM {project_id}.{database}.users
                    WHERE user_id = 999  -- No matching user
                """,
                execution_database=database,
            )

        results = query_empty_results()

        # Should return empty list
        assert len(results) == 0
        assert results == []

    def test_missing_mock_table_error(self):
        """Test that missing mock tables raise appropriate errors."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                # Only provide users table, but query needs orders too
                UsersMockTable([User(1, "Alice", "alice@example.com", date(2023, 1, 1))])
            ],
            result_class=UserOrderSummary,
        )
        def query_missing_table():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT u.user_id, u.name, COUNT(o.order_id) as total_orders
                    FROM {project_id}.{database}.users u
                    JOIN {project_id}.{database}.orders o
                        ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                """,
                execution_database=database,
            )

        # Should raise MockTableNotFoundError
        with self.assertRaises(MockTableNotFoundError):
            query_missing_table()

    def test_single_user_query_with_physical_tables(self):
        """Test querying a single user with physical tables option."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable([User(42, "Test User", "test@example.com", date(2023, 1, 1))])
            ],
            result_class=UserOrderSummary,
            use_physical_tables=True,  # Test physical tables option
        )
        def query_single_user():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        user_id,
                        name,
                        0 as total_orders,
                        0 as total_amount
                    FROM {project_id}.{database}.users
                    WHERE user_id = 42
                """,
                execution_database="test_db",  # Different database
            )

        results = query_single_user()

        # Verify single result
        assert len(results) == 1
        user = results[0]
        assert user.user_id == 42
        assert user.name == "Test User"
        assert user.total_orders == 0
        assert user.total_amount == Decimal("0")

    def test_pattern_decorator_only(self):
        """Test Pattern 1: Providing all settings in the decorator."""

        test_users = [
            User(1, "Alice", "alice@example.com", date(2023, 1, 1)),
            User(2, "Bob", "bob@example.com", date(2023, 1, 2)),
            User(3, "Charlie", "charlie@example.com", date(2023, 1, 3)),
        ]

        test_orders = [
            Order(101, 1, Decimal("100.00"), date(2023, 2, 1)),
            Order(102, 1, Decimal("150.00"), date(2023, 2, 2)),
            Order(103, 2, Decimal("200.00"), date(2023, 2, 3)),
            Order(104, 3, Decimal("75.00"), date(2023, 2, 4)),
        ]

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[UsersMockTable(test_users), OrdersMockTable(test_orders)],
            result_class=UserOrderSummary,
        )
        def query_decorator_pattern():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM {project_id}.{database}.users u
                    LEFT JOIN {project_id}.{database}.orders o
                        ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database=database,
            )

        results = query_decorator_pattern()

        # Assertions
        assert len(results) == 3
        assert results[0].user_id == 1
        assert results[0].total_orders == 2
        assert results[0].total_amount == Decimal("250.00")

    def test_pattern_testcase_only(self):
        """Test Pattern 2: Providing all settings in the TestCase (require empty decorator)."""

        test_users = [
            User(1, "Alice", "alice@example.com", date(2023, 1, 1)),
            User(2, "Bob", "bob@example.com", date(2023, 1, 2)),
            User(3, "Charlie", "charlie@example.com", date(2023, 1, 3)),
        ]

        test_orders = [
            Order(101, 1, Decimal("100.00"), date(2023, 2, 1)),
            Order(102, 1, Decimal("150.00"), date(2023, 2, 2)),
            Order(103, 2, Decimal("200.00"), date(2023, 2, 3)),
            Order(104, 3, Decimal("75.00"), date(2023, 2, 4)),
        ]

        @sql_test()  # Empty decorator
        def query_testcase_pattern():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM {project_id}.{database}.users u
                    LEFT JOIN {project_id}.{database}.orders o
                        ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database=database,
                mock_tables=[UsersMockTable(test_users), OrdersMockTable(test_orders)],
                result_class=UserOrderSummary,
                adapter_type="bigquery",
            )

        results = query_testcase_pattern()

        # Assertions
        assert len(results) == 3
        assert results[0].user_id == 1
        assert results[0].total_orders == 2
        assert results[0].total_amount == Decimal("250.00")

    def test_pattern_mix_and_match(self):
        """Test Pattern 3: Mix and match decorator and TestCase values."""

        test_users = [
            User(1, "Alice", "alice@example.com", date(2023, 1, 1)),
            User(2, "Bob", "bob@example.com", date(2023, 1, 2)),
            User(3, "Charlie", "charlie@example.com", date(2023, 1, 3)),
        ]

        test_orders = [
            Order(101, 1, Decimal("100.00"), date(2023, 2, 1)),
            Order(102, 1, Decimal("150.00"), date(2023, 2, 2)),
            Order(103, 2, Decimal("200.00"), date(2023, 2, 3)),
            Order(104, 3, Decimal("75.00"), date(2023, 2, 4)),
        ]

        @sql_test(mock_tables=[UsersMockTable(test_users), OrdersMockTable(test_orders)])
        def query_mixed_pattern():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        u.user_id,
                        u.name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.amount) as total_amount
                    FROM {project_id}.{database}.users u
                    LEFT JOIN {project_id}.{database}.orders o
                        ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.name
                    ORDER BY u.user_id
                """,
                execution_database=database,
                # We define result_class here instead of in decorator
                result_class=UserOrderSummary,
                adapter_type="bigquery",
            )

        results = query_mixed_pattern()

        # Assertions
        assert len(results) == 3
        assert results[0].user_id == 1
        assert results[0].total_orders == 2
        assert results[0].total_amount == Decimal("250.00")


@pytest.mark.integration
@pytest.mark.bigquery
@pytest.mark.slow
class TestBigQueryPerformance:
    """Performance-related integration tests for BigQuery."""

    def test_large_dataset_simulation(self):
        """Test with simulated large dataset."""

        # Generate larger test dataset
        users = [
            User(
                i,
                f"User {i}",
                f"user{i}@example.com",
                date(2023, 1, 1),
                i % 3 == 0,
                Decimal(str(i * 100)),
            )
            for i in range(1, 51)  # 50 users
        ]

        orders = [
            Order(
                i,
                (i % 50) + 1,
                Decimal(str((i * 25) % 500 + 50)),
                date(2023, 3, (i % 28) + 1),
                "completed",
            )
            for i in range(1, 101)  # 100 orders
        ]

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UsersMockTable(users),
                OrdersMockTable(orders),
            ],
            result_class=LargeDatasetResult,
        )
        def query_large_dataset():
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "bigquery-public-data")
            database = os.getenv("BIGQUERY_DATABASE", "analytics_db")
            return TestCase(
                query=f"""
                    SELECT
                        COUNT(DISTINCT u.user_id) as unique_users,
                        COUNT(o.order_id) as total_orders,
                        AVG(o.amount) as avg_order_amount,
                        MAX(o.amount) as max_order_amount,
                        MIN(o.amount) as min_order_amount
                    FROM {project_id}.{database}.users u
                    INNER JOIN {project_id}.{database}.orders o ON u.user_id = o.user_id
                    WHERE u.is_premium = TRUE
                """,
                execution_database=database,
            )

        results = query_large_dataset()

        assert len(results) == 1
        result = results[0]

        # Verify aggregation results
        assert result.unique_users > 0
        assert result.total_orders > 0
        assert result.avg_order_amount > Decimal("0")
        assert result.max_order_amount >= result.min_order_amount
