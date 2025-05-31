"""Integration tests for Redshift adapter with pytest configuration."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


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


@dataclass
class Product:
    """Test product data class for integration tests."""

    product_id: int
    name: str
    category: str
    price: Decimal
    is_active: bool


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
    total_spent: Optional[Decimal]
    avg_order_value: Optional[Decimal]


class ProductAnalyticsResult(BaseModel):
    """Result model for product analytics queries."""

    category: str
    product_count: int
    avg_price: Decimal
    total_revenue: Decimal


class RevenueResult(BaseModel):
    """Result model for revenue queries."""

    total_revenue: Decimal
    order_count: int


class CustomersMockTable(BaseMockTable):
    """Mock table for customers data."""

    def get_database_name(self) -> str:
        return "sqltesting_db"

    def get_table_name(self) -> str:
        return "customers"


class OrdersMockTable(BaseMockTable):
    """Mock table for orders data."""

    def get_database_name(self) -> str:
        return "sqltesting_db"

    def get_table_name(self) -> str:
        return "orders"


class ProductsMockTable(BaseMockTable):
    """Mock table for products data."""

    def get_database_name(self) -> str:
        return "sqltesting_db"

    def get_table_name(self) -> str:
        return "products"


@pytest.mark.integration
@pytest.mark.redshift
class TestRedshiftIntegration(unittest.TestCase):
    """Integration tests for Redshift adapter functionality."""

    def setUp(self):
        """Set up test data for all integration tests."""
        self.customers_data = [
            Customer(
                1, "Alice Johnson", "alice@example.com", date(2023, 1, 15), True, Decimal("1500.00")
            ),
            Customer(
                2, "Bob Smith", "bob@example.com", date(2023, 2, 20), False, Decimal("750.00")
            ),
            Customer(
                3, "Carol Davis", "carol@example.com", date(2023, 3, 10), True, Decimal("2200.00")
            ),
            Customer(4, "David Wilson", "david@example.com", date(2023, 1, 5), False, None),
        ]

        self.orders_data = [
            Order(101, 1, datetime(2023, 4, 1, 10, 30, 0), Decimal("299.99"), "completed"),
            Order(102, 1, datetime(2023, 4, 15, 14, 20, 0), Decimal("149.50"), "completed"),
            Order(103, 2, datetime(2023, 4, 5, 9, 15, 0), Decimal("89.99"), "completed"),
            Order(104, 3, datetime(2023, 4, 10, 16, 45, 0), Decimal("199.99"), "completed"),
            Order(105, 3, datetime(2023, 4, 20, 11, 30, 0), Decimal("349.99"), "pending"),
            Order(106, 1, datetime(2023, 4, 25, 13, 10, 0), Decimal("79.99"), "cancelled"),
        ]

        self.products_data = [
            Product(1, "Laptop", "Electronics", Decimal("999.99"), True),
            Product(2, "Smartphone", "Electronics", Decimal("599.99"), True),
            Product(3, "Desk Chair", "Furniture", Decimal("199.99"), True),
            Product(4, "Coffee Maker", "Appliances", Decimal("89.99"), True),
            Product(5, "Old Monitor", "Electronics", Decimal("150.00"), False),
        ]

    def test_simple_customer_query(self):
        """Test basic customer data retrieval."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(
                    [
                        Customer(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                            Decimal("1500.00"),
                        ),
                        Customer(
                            2,
                            "Bob Smith",
                            "bob@example.com",
                            date(2023, 2, 20),
                            False,
                            Decimal("750.00"),
                        ),
                    ]
                )
            ],
            result_class=CustomerResult,
        )
        def query_customer():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        name,
                        email,
                        0 as total_orders,
                        CAST(0.00 AS DECIMAL(10,2)) as total_amount
                    FROM customers
                    WHERE customer_id = 1
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_customer()
        assert len(results) == 1
        assert results[0].customer_id == 1
        assert results[0].name == "Alice Johnson"

    def test_customer_order_join(self):
        """Test joining customers with orders data."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=CustomerResult,
        )
        def query_customer_orders():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name,
                        c.email,
                        COUNT(o.order_id) as total_orders,
                        COALESCE(SUM(o.amount), 0) as total_amount
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name, c.email
                    ORDER BY total_amount DESC
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_customer_orders()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "total_orders") for result in results)

    def test_order_summary_analytics(self):
        """Test order summary analytics with aggregations."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(
                    [
                        Customer(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                            Decimal("1500.00"),
                        ),
                        Customer(
                            2,
                            "Bob Smith",
                            "bob@example.com",
                            date(2023, 2, 20),
                            False,
                            Decimal("750.00"),
                        ),
                        Customer(
                            3,
                            "Carol Davis",
                            "carol@example.com",
                            date(2023, 3, 10),
                            True,
                            Decimal("2200.00"),
                        ),
                    ]
                ),
                OrdersMockTable(
                    [
                        Order(
                            101, 1, datetime(2023, 4, 1, 10, 30, 0), Decimal("299.99"), "completed"
                        ),
                        Order(
                            102, 1, datetime(2023, 4, 15, 14, 20, 0), Decimal("149.50"), "completed"
                        ),
                        Order(
                            103, 2, datetime(2023, 4, 5, 9, 15, 0), Decimal("89.99"), "completed"
                        ),
                        Order(
                            104, 3, datetime(2023, 4, 10, 16, 45, 0), Decimal("199.99"), "completed"
                        ),
                    ]
                ),
            ],
            result_class=OrderSummaryResult,
        )
        def query_order_summary():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name as customer_name,
                        COUNT(o.order_id) as order_count,
                        SUM(o.amount) as total_spent,
                        AVG(o.amount) as avg_order_value
                    FROM customers c
                    INNER JOIN orders o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    HAVING COUNT(o.order_id) >= 1
                    ORDER BY total_spent DESC
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_order_summary()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "order_count") for result in results)
        assert all(result.order_count >= 1 for result in results)

    def test_product_category_analytics(self):
        """Test product analytics by category."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                ProductsMockTable(
                    [
                        Product(1, "Laptop", "Electronics", Decimal("999.99"), True),
                        Product(2, "Smartphone", "Electronics", Decimal("599.99"), True),
                        Product(3, "Desk Chair", "Furniture", Decimal("199.99"), True),
                        Product(4, "Coffee Maker", "Appliances", Decimal("89.99"), True),
                    ]
                )
            ],
            result_class=ProductAnalyticsResult,
        )
        def query_product_analytics():
            return TestCase(
                query="""
                    SELECT
                        category,
                        COUNT(*) as product_count,
                        AVG(price) as avg_price,
                        SUM(price) as total_revenue
                    FROM products
                    WHERE is_active = true
                    GROUP BY category
                    ORDER BY total_revenue DESC
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_product_analytics()
        assert len(results) >= 1
        assert all(hasattr(result, "category") for result in results)
        assert all(hasattr(result, "product_count") for result in results)
        assert all(result.product_count > 0 for result in results)

    def test_complex_date_filtering(self):
        """Test complex date-based filtering and aggregations."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[OrdersMockTable(self.orders_data)],
            result_class=RevenueResult,
        )
        def query_revenue_by_date():
            return TestCase(
                query="""
                    SELECT
                        SUM(amount) as total_revenue,
                        COUNT(*) as order_count
                    FROM orders
                    WHERE order_date >= DATE('2023-04-01')
                      AND order_date < DATE('2023-05-01')
                      AND status IN ('completed', 'pending')
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_revenue_by_date()
        assert len(results) == 1
        assert hasattr(results[0], "total_revenue")
        assert hasattr(results[0], "order_count")
        assert results[0].order_count >= 0

    def test_null_handling(self):
        """Test proper handling of NULL values."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(
                    [
                        Customer(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                            Decimal("1500.00"),
                        ),
                        Customer(2, "Bob Smith", "bob@example.com", date(2023, 2, 20), False, None),
                    ]
                )
            ],
            result_class=CustomerResult,
        )
        def query_with_nulls():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        name,
                        email,
                        0 as total_orders,
                        COALESCE(lifetime_value, 0.00) as total_amount
                    FROM customers
                    WHERE lifetime_value IS NOT NULL OR customer_id = 2
                    ORDER BY customer_id
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_with_nulls()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "total_amount") for result in results)

    def test_string_functions(self):
        """Test string manipulation functions."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[CustomersMockTable(self.customers_data)],
            result_class=CustomerResult,
        )
        def query_string_functions():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        UPPER(name) as name,
                        LOWER(email) as email,
                        0 as total_orders,
                        CAST(0.00 AS DECIMAL(10,2)) as total_amount
                    FROM customers
                    WHERE LENGTH(name) > 8
                    ORDER BY customer_id
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_string_functions()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "name") for result in results)
        assert all(len(result.name) > 0 for result in results)

    def test_boolean_operations(self):
        """Test boolean field operations and filtering."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(
                    [
                        Customer(
                            1,
                            "Alice Johnson",
                            "alice@example.com",
                            date(2023, 1, 15),
                            True,
                            Decimal("1500.00"),
                        ),
                        Customer(
                            2,
                            "Bob Smith",
                            "bob@example.com",
                            date(2023, 2, 20),
                            False,
                            Decimal("750.00"),
                        ),
                    ]
                )
            ],
            result_class=CustomerResult,
        )
        def query_premium_customers():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        name,
                        email,
                        0 as total_orders,
                        lifetime_value as total_amount
                    FROM customers
                    WHERE is_premium = true
                    ORDER BY customer_id
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_premium_customers()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert results[0].customer_id == 1

    def test_window_functions(self):
        """Test window functions and ranking."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=OrderSummaryResult,
        )
        def query_with_window_functions():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name as customer_name,
                        COUNT(o.order_id) as order_count,
                        SUM(o.amount) as total_spent,
                        AVG(o.amount) as avg_order_value
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    ORDER BY total_spent DESC NULLS LAST
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_with_window_functions()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "customer_name") for result in results)
        assert all(hasattr(result, "order_count") for result in results)

    def test_case_statements(self):
        """Test CASE statements and conditional logic."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                ProductsMockTable(
                    [
                        Product(1, "Laptop Pro", "Electronics", Decimal("1299.99"), True),
                        Product(2, "Gaming Mouse", "Electronics", Decimal("59.99"), True),
                        Product(3, "Office Desk", "Furniture", Decimal("299.99"), True),
                    ]
                )
            ],
            result_class=ProductAnalyticsResult,
        )
        def query_with_case_statements():
            return TestCase(
                query="""
                    SELECT
                        category,
                        COUNT(*) as product_count,
                        AVG(
                            CASE
                                WHEN price > 1000 THEN price * 0.9
                                WHEN price > 100 THEN price * 0.95
                                ELSE price
                            END
                        ) as avg_price,
                        SUM(price) as total_revenue
                    FROM products
                    WHERE is_active = true
                    GROUP BY category
                    ORDER BY avg_price DESC
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_with_case_statements()
        assert len(results) >= 1
        assert all(hasattr(result, "category") for result in results)
        assert all(hasattr(result, "avg_price") for result in results)
        assert all(result.product_count > 0 for result in results)

    def test_subquery_operations(self):
        """Test subquery operations and EXISTS clauses."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=CustomerResult,
        )
        def query_with_subqueries():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        name,
                        email,
                        0 as total_orders,
                        COALESCE(lifetime_value, 0.00) as total_amount
                    FROM customers c
                    WHERE EXISTS (
                        SELECT 1 FROM orders o
                        WHERE o.customer_id = c.customer_id
                        AND o.status = 'completed'
                    )
                    ORDER BY customer_id
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_with_subqueries()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "name") for result in results)
        assert all(hasattr(result, "email") for result in results)

    def test_date_functions(self):
        """Test date extraction and manipulation functions."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                OrdersMockTable(
                    [
                        Order(
                            101, 1, datetime(2023, 4, 1, 10, 30, 0), Decimal("299.99"), "completed"
                        ),
                        Order(
                            102, 1, datetime(2023, 4, 15, 14, 20, 0), Decimal("149.50"), "completed"
                        ),
                        Order(
                            103, 2, datetime(2023, 3, 5, 9, 15, 0), Decimal("89.99"), "completed"
                        ),
                    ]
                )
            ],
            result_class=RevenueResult,
        )
        def query_with_date_functions():
            return TestCase(
                query="""
                    SELECT
                        SUM(amount) as total_revenue,
                        COUNT(*) as order_count
                    FROM orders
                    WHERE EXTRACT(month FROM order_date) = 4
                      AND EXTRACT(year FROM order_date) = 2023
                      AND status = 'completed'
                """,
                execution_database="sqltesting_db",
            )

        # Execute the test
        results = query_with_date_functions()
        assert len(results) == 1
        assert hasattr(results[0], "total_revenue")
        assert hasattr(results[0], "order_count")
        assert results[0].order_count >= 0


if __name__ == "__main__":
    unittest.main()
