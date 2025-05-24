"""Integration tests for Athena adapter with pytest configuration."""

import os
import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library.mock_table import BaseMockTable


@dataclass
class Customer:
    """Test customer data class for integration tests."""

    customer_id: int
    name: str
    email: str
    signup_date: date
    is_premium: bool
    lifetime_value: Optional[Decimal] = None


@dataclass
class Order:
    """Test order data class for integration tests."""

    order_id: int
    customer_id: int
    order_date: datetime
    amount: Decimal
    status: str


class CustomerResult(BaseModel):
    """Result model for customer queries."""

    customer_id: int
    name: str
    email: str
    total_orders: int
    total_amount: Decimal


class OrderSummaryResult(BaseModel):
    """Result model for order summary queries."""

    customer_id: int
    customer_name: str
    order_count: int
    total_amount: Decimal


class MonthlySalesResult(BaseModel):
    """Result model for monthly sales aggregation."""

    order_year: int
    order_month: int
    order_count: int
    total_sales: Decimal


class CustomerAnalyticsResult(BaseModel):
    """Result model for complex customer analytics query."""

    customer_id: int
    name: str
    is_premium: bool
    order_count: int
    total_spent: Decimal
    avg_order_value: Decimal
    spending_rank: int
    customer_tier: str


class NullHandlingResult(BaseModel):
    """Result model for null handling query."""

    customer_id: int
    name: str
    email_status: str
    lifetime_value_filled: Decimal
    value_category: str


class LargeDatasetResult(BaseModel):
    """Result model for large dataset aggregation."""

    unique_customers: int
    total_orders: int
    avg_order_amount: Decimal
    max_order_amount: Decimal
    min_order_amount: Decimal


class CustomerMockTable(BaseMockTable):
    """Mock table for customer data."""

    def get_database_name(self) -> str:
        return os.getenv("AWS_ATHENA_DATABASE", "test_db")

    def get_table_name(self) -> str:
        return "customers"


class OrderMockTable(BaseMockTable):
    """Mock table for order data."""

    def get_database_name(self) -> str:
        return os.getenv("AWS_ATHENA_DATABASE", "test_db")

    def get_table_name(self) -> str:
        return "orders"


@pytest.mark.integration
@pytest.mark.athena
class TestAthenaIntegration(unittest.TestCase):
    """Integration tests for Athena adapter using pytest configuration."""

    def test_simple_customer_query(self):
        """Test simple customer query with Athena adapter."""

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                CustomerMockTable(
                    [
                        Customer(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                            Decimal("2500.00"),
                        ),
                        Customer(
                            2,
                            "Bob Smith",
                            "bob@example.com",
                            date(2023, 2, 20),
                            False,
                            Decimal("150.00"),
                        ),
                        Customer(
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
            result_class=Customer,
        )
        def query_premium_customers():
            return TestCase(
                query="""
                    SELECT
                        customer_id, name, email, signup_date,
                        is_premium, lifetime_value
                    FROM customers
                    WHERE is_premium = TRUE
                    ORDER BY lifetime_value DESC
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_premium_customers()

        assert len(results) == 2
        assert results[0].name == "Carol Davis"
        assert results[0].lifetime_value == Decimal("3200.00")
        assert results[1].name == "Alice Johnson"
        assert results[1].lifetime_value == Decimal("2500.00")

    def test_customer_order_join_query(self):
        """Test join query between customers and orders."""

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                CustomerMockTable(
                    [
                        Customer(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                        ),
                        Customer(
                            2, "Bob Smith", "bob@example.com", date(2023, 2, 20), False
                        ),
                    ]
                ),
                OrderMockTable(
                    [
                        Order(
                            101,
                            1,
                            datetime(2023, 3, 1, 10, 30),
                            Decimal("299.99"),
                            "completed",
                        ),
                        Order(
                            102,
                            1,
                            datetime(2023, 3, 15, 14, 45),
                            Decimal("149.99"),
                            "completed",
                        ),
                        Order(
                            103,
                            2,
                            datetime(2023, 3, 20, 9, 15),
                            Decimal("79.99"),
                            "completed",
                        ),
                        Order(
                            104,
                            1,
                            datetime(2023, 4, 1, 16, 20),
                            Decimal("199.99"),
                            "pending",
                        ),
                    ]
                ),
            ],
            result_class=OrderSummaryResult,
        )
        def query_customer_order_summary():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name as customer_name,
                        COUNT(o.order_id) as order_count,
                        SUM(o.amount) as total_amount
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    ORDER BY total_amount DESC
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_customer_order_summary()

        assert len(results) == 2
        assert results[0].customer_name == "Alice Johnson"
        assert results[0].order_count == 2
        assert results[0].total_amount == Decimal("449.98")
        assert results[1].customer_name == "Bob Smith"
        assert results[1].order_count == 1
        assert results[1].total_amount == Decimal("79.99")

    def test_aggregation_with_date_functions(self):
        """Test aggregation queries with date functions."""

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                OrderMockTable(
                    [
                        Order(
                            101,
                            1,
                            datetime(2023, 1, 15, 10, 30),
                            Decimal("299.99"),
                            "completed",
                        ),
                        Order(
                            102,
                            1,
                            datetime(2023, 1, 20, 14, 45),
                            Decimal("149.99"),
                            "completed",
                        ),
                        Order(
                            103,
                            2,
                            datetime(2023, 2, 10, 9, 15),
                            Decimal("79.99"),
                            "completed",
                        ),
                        Order(
                            104,
                            2,
                            datetime(2023, 2, 25, 16, 20),
                            Decimal("199.99"),
                            "completed",
                        ),
                        Order(
                            105,
                            1,
                            datetime(2023, 3, 5, 11, 10),
                            Decimal("89.99"),
                            "completed",
                        ),
                    ]
                )
            ],
            result_class=MonthlySalesResult,
        )
        def query_monthly_sales():
            return TestCase(
                query="""
                    SELECT
                        EXTRACT(YEAR FROM order_date) as order_year,
                        EXTRACT(MONTH FROM order_date) as order_month,
                        COUNT(*) as order_count,
                        SUM(amount) as total_sales
                    FROM orders
                    WHERE status = 'completed'
                    GROUP BY
                        EXTRACT(YEAR FROM order_date),
                        EXTRACT(MONTH FROM order_date)
                    ORDER BY order_year, order_month
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_monthly_sales()

        assert len(results) == 3

        # January 2023
        jan_result = next(r for r in results if r.order_month == 1)
        assert jan_result.order_count == 2
        assert jan_result.total_sales == Decimal("449.98")

        # February 2023
        feb_result = next(r for r in results if r.order_month == 2)
        assert feb_result.order_count == 2
        assert feb_result.total_sales == Decimal("279.98")

    @pytest.mark.slow
    def test_complex_analytical_query(self):
        """Test complex analytical query with window functions."""

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                CustomerMockTable(
                    [
                        Customer(
                            1,
                            "Alice",
                            "alice@example.com",
                            date(2023, 1, 1),
                            True,
                            Decimal("1000"),
                        ),
                        Customer(
                            2,
                            "Bob",
                            "bob@example.com",
                            date(2023, 1, 15),
                            False,
                            Decimal("500"),
                        ),
                        Customer(
                            3,
                            "Carol",
                            "carol@example.com",
                            date(2023, 2, 1),
                            True,
                            Decimal("1500"),
                        ),
                    ]
                ),
                OrderMockTable(
                    [
                        Order(1, 1, datetime(2023, 3, 1), Decimal("100"), "completed"),
                        Order(2, 1, datetime(2023, 3, 15), Decimal("200"), "completed"),
                        Order(3, 2, datetime(2023, 3, 10), Decimal("50"), "completed"),
                        Order(4, 3, datetime(2023, 3, 20), Decimal("300"), "completed"),
                        Order(5, 3, datetime(2023, 4, 1), Decimal("250"), "completed"),
                    ]
                ),
            ],
            result_class=CustomerAnalyticsResult,
        )
        def query_customer_analytics():
            return TestCase(
                query="""
                    WITH customer_metrics AS (
                        SELECT
                            c.customer_id,
                            c.name,
                            c.is_premium,
                            c.lifetime_value,
                            COUNT(o.order_id) as order_count,
                            SUM(o.amount) as total_spent,
                            AVG(o.amount) as avg_order_value,
                            ROW_NUMBER() OVER (ORDER BY SUM(o.amount) DESC)
                                as spending_rank
                        FROM customers c
                        LEFT JOIN orders o
                            ON c.customer_id = o.customer_id AND o.status = 'completed'
                        GROUP BY c.customer_id, c.name, c.is_premium, c.lifetime_value
                    )
                    SELECT
                        customer_id,
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
                        END as customer_tier
                    FROM customer_metrics
                    ORDER BY spending_rank
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_customer_analytics()

        assert len(results) == 3

        # Top spender should be Carol (order_count=2, total_spent=550)
        top_spender = results[0]
        assert top_spender.name == "Carol"
        assert top_spender.total_spent == Decimal("550")
        assert top_spender.customer_tier == "Top Spender"

    def test_null_handling_and_edge_cases(self):
        """Test handling of NULL values and edge cases."""

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                CustomerMockTable(
                    [
                        Customer(
                            1,
                            "Alice",
                            "alice@example.com",
                            date(2023, 1, 1),
                            True,
                            None,
                        ),
                        Customer(
                            2,
                            "Bob",
                            "bob@example.com",
                            date(2023, 1, 15),
                            False,
                            Decimal("0"),
                        ),
                        Customer(
                            3, "Carol", "", date(2023, 2, 1), True, Decimal("1500.50")
                        ),
                    ]
                )
            ],
            result_class=NullHandlingResult,
        )
        def query_null_handling():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
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
                    FROM customers
                    ORDER BY customer_id
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
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


@pytest.mark.integration
@pytest.mark.athena
@pytest.mark.slow
class TestAthenaPerformance:
    """Performance-related integration tests for Athena."""

    def test_large_dataset_simulation(self):
        """Test with simulated large dataset."""

        # Generate larger test dataset
        customers = [
            Customer(
                i,
                f"Customer {i}",
                f"customer{i}@example.com",
                date(2023, 1, 1),
                i % 3 == 0,
                Decimal(str(i * 100)),
            )
            for i in range(1, 51)  # 50 customers
        ]

        orders = [
            Order(
                i,
                (i % 50) + 1,
                datetime(2023, 3, i % 28 + 1),
                Decimal(str((i * 25) % 500 + 50)),
                "completed",
            )
            for i in range(1, 101)  # 100 orders
        ]

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                CustomerMockTable(customers),
                OrderMockTable(orders),
            ],
            result_class=LargeDatasetResult,
        )
        def query_large_dataset():
            return TestCase(
                query="""
                    SELECT
                        COUNT(DISTINCT c.customer_id) as unique_customers,
                        COUNT(o.order_id) as total_orders,
                        AVG(o.amount) as avg_order_amount,
                        MAX(o.amount) as max_order_amount,
                        MIN(o.amount) as min_order_amount
                    FROM customers c
                    INNER JOIN orders o ON c.customer_id = o.customer_id
                    WHERE c.is_premium = TRUE
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_large_dataset()

        assert len(results) == 1
        result = results[0]

        # Verify aggregation results
        assert result.unique_customers > 0
        assert result.total_orders > 0
        assert result.avg_order_amount > Decimal("0")
        assert result.max_order_amount >= result.min_order_amount
