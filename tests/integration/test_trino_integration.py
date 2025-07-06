"""Integration tests for Trino adapter with pytest configuration."""

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
    in_stock: bool


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
    total_spent: Decimal
    avg_order_value: Decimal


class ProductAnalyticsResult(BaseModel):
    """Result model for product analytics queries."""

    category: str
    product_count: int
    avg_price: Decimal


class CustomersMockTable(BaseMockTable):
    """Mock table for customers data."""

    def get_database_name(self) -> str:
        return "memory.default"

    def get_table_name(self) -> str:
        return "customers"


class OrdersMockTable(BaseMockTable):
    """Mock table for orders data."""

    def get_database_name(self) -> str:
        return "memory.default"

    def get_table_name(self) -> str:
        return "orders"


class ProductsMockTable(BaseMockTable):
    """Mock table for products data."""

    def get_database_name(self) -> str:
        return "memory.default"

    def get_table_name(self) -> str:
        return "products"


# Test data that will be reused across multiple tests
@pytest.mark.integration
@pytest.mark.trino
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestTrinoIntegration:
    """Integration tests for Trino adapter using real database connections."""

    def setup_method(self):
        """Set up test data for integration tests."""
        self.customers_data = [
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
                Decimal("2250.00"),
            ),
        ]

        self.orders_data = [
            Order(101, 1, datetime(2023, 3, 1, 10, 30, 0), Decimal("299.99"), "completed"),
            Order(102, 1, datetime(2023, 3, 15, 14, 20, 0), Decimal("149.99"), "completed"),
            Order(103, 2, datetime(2023, 4, 5, 9, 15, 0), Decimal("99.99"), "pending"),
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

    def test_simple_customer_query(self, use_physical_tables):
        """Test basic customer data retrieval."""

        @sql_test(
            adapter_type="trino",
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
                    FROM customers WHERE customer_id = 1
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_customer()
        assert len(results) == 1
        assert results[0].customer_id == 1
        assert results[0].name == "Alice Johnson"

    def test_customer_order_join(self, use_physical_tables):
        """Test joining customers with orders data."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=OrderSummaryResult,
        )
        def query_customer_orders():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name as customer_name,
                        COUNT(o.order_id) as order_count,
                        COALESCE(SUM(o.amount), CAST(0.00 AS DECIMAL(10,2))) as total_spent,
                        COALESCE(AVG(o.amount), CAST(0.00 AS DECIMAL(10,2))) as avg_order_value
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    ORDER BY total_spent DESC
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_customer_orders()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "order_count") for result in results)

    def test_date_functions(self, use_physical_tables):
        """Test date functions and filtering."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[CustomersMockTable(self.customers_data)],
            result_class=CustomerResult,
        )
        def query_recent_customers():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        name,
                        email,
                        0 as total_orders,
                        CAST(0.00 AS DECIMAL(10,2)) as total_amount
                    FROM customers
                    WHERE signup_date >= DATE '2023-02-01'
                    ORDER BY signup_date DESC
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_recent_customers()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)

    def test_complex_date_filtering(self, use_physical_tables):
        """Test complex date operations and filtering."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[OrdersMockTable(self.orders_data)],
            result_class=OrderSummaryResult,
        )
        def query_revenue_by_date():
            return TestCase(
                query="""
                    SELECT
                        1 as customer_id,
                        'Revenue Analysis' as customer_name,
                        COUNT(*) as order_count,
                        SUM(amount) as total_spent,
                        AVG(amount) as avg_order_value
                    FROM orders
                    WHERE DATE(order_date) >= DATE '2023-04-01'
                    AND status IN ('completed', 'pending')
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_revenue_by_date()
        assert len(results) == 1
        assert hasattr(results[0], "total_spent")
        assert hasattr(results[0], "order_count")
        assert results[0].order_count >= 0

    def test_null_handling(self, use_physical_tables):
        """Test proper handling of NULL values."""

        @sql_test(
            adapter_type="trino",
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
                            None,
                        ),
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
                        COALESCE(lifetime_value, CAST(0.00 AS DECIMAL(10,2))) as total_amount
                    FROM customers
                    WHERE lifetime_value IS NOT NULL OR customer_id = 2
                    ORDER BY customer_id
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_with_nulls()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert all(hasattr(result, "total_amount") for result in results)

    def test_string_functions(self, use_physical_tables):
        """Test string manipulation functions."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[CustomersMockTable(self.customers_data)],
            result_class=CustomerResult,
        )
        def query_string_operations():
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
                    ORDER BY name
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_string_operations()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)

    def test_aggregation_functions(self, use_physical_tables):
        """Test aggregation and grouping operations."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[ProductsMockTable(self.products_data)],
            result_class=ProductAnalyticsResult,
        )
        def query_product_analytics():
            return TestCase(
                query="""
                    SELECT
                        category,
                        COUNT(*) as product_count,
                        AVG(price) as avg_price
                    FROM products
                    WHERE in_stock = true
                    GROUP BY category
                    HAVING COUNT(*) >= 1
                    ORDER BY avg_price DESC
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_product_analytics()
        assert len(results) >= 1
        assert all(hasattr(result, "category") for result in results)
        assert all(hasattr(result, "avg_price") for result in results)

    def test_boolean_operations(self, use_physical_tables):
        """Test boolean logic and conditions."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[CustomersMockTable(self.customers_data)],
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
                        CAST(0.00 AS DECIMAL(10,2)) as total_amount
                    FROM customers
                    WHERE is_premium = true
                    AND lifetime_value > CAST(1000.00 AS DECIMAL(10,2))
                    ORDER BY lifetime_value DESC
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_premium_customers()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)
        assert results[0].customer_id == 3

    def test_window_functions(self, use_physical_tables):
        """Test window functions and ranking."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=OrderSummaryResult,
        )
        def query_customer_ranking():
            return TestCase(
                query="""
                    WITH customer_totals AS (
                        SELECT
                            c.customer_id,
                            c.name as customer_name,
                            COUNT(o.order_id) as order_count,
                            COALESCE(SUM(o.amount), CAST(0.00 AS DECIMAL(10,2))) as total_spent,
                            COALESCE(AVG(o.amount), CAST(0.00 AS DECIMAL(10,2))) as avg_order_value
                        FROM customers c
                        LEFT JOIN orders o ON c.customer_id = o.customer_id
                        GROUP BY c.customer_id, c.name
                    )
                    SELECT
                        customer_id,
                        customer_name,
                        order_count,
                        total_spent,
                        avg_order_value
                    FROM customer_totals
                    WHERE total_spent > CAST(0.00 AS DECIMAL(10,2))
                    ORDER BY total_spent DESC
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_customer_ranking()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)

    def test_case_statements(self, use_physical_tables):
        """Test CASE statements and conditional logic."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[ProductsMockTable(self.products_data)],
            result_class=ProductAnalyticsResult,
        )
        def query_with_case_statements():
            return TestCase(
                query="""
                    SELECT
                        CASE
                            WHEN price > CAST(500.00 AS DECIMAL(10,2)) THEN 'High-End'
                            WHEN price > CAST(200.00 AS DECIMAL(10,2)) THEN 'Mid-Range'
                            ELSE 'Budget'
                        END as category,
                        COUNT(*) as product_count,
                        AVG(price) as avg_price
                    FROM products
                    GROUP BY
                        CASE
                            WHEN price > CAST(500.00 AS DECIMAL(10,2)) THEN 'High-End'
                            WHEN price > CAST(200.00 AS DECIMAL(10,2)) THEN 'Mid-Range'
                            ELSE 'Budget'
                        END
                    ORDER BY avg_price DESC
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_with_case_statements()
        assert len(results) >= 1
        assert all(hasattr(result, "category") for result in results)
        assert all(hasattr(result, "avg_price") for result in results)
        assert all(result.product_count > 0 for result in results)

    def test_subquery_operations(self, use_physical_tables):
        """Test subquery operations and EXISTS clauses."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=CustomerResult,
        )
        def query_customers_with_orders():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name,
                        c.email,
                        0 as total_orders,
                        CAST(0.00 AS DECIMAL(10,2)) as total_amount
                    FROM customers c
                    WHERE EXISTS (
                        SELECT 1
                        FROM orders o
                        WHERE o.customer_id = c.customer_id
                        AND o.status = 'completed'
                    )
                    ORDER BY c.customer_id
                """,
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        # Execute the test
        results = query_customers_with_orders()
        assert len(results) >= 1
        assert all(hasattr(result, "customer_id") for result in results)

    def test_unqualified_table_names_with_default_namespace(self, use_physical_tables):
        """Test how default_namespace resolves unqualified table names to mock tables.

        This test demonstrates the key role of default_namespace for Trino:
        - Query uses unqualified table names: 'customers' and 'orders'
        - default_namespace='memory.default' qualifies them to:
          'memory.default.customers' and 'memory.default.orders'
        - These qualified names must match mock table get_qualified_name() values
        """

        test_customers = [
            Customer(
                1,
                "Alice",
                "alice@example.com",
                date(2023, 1, 1),
                True,
                Decimal("1000.00"),
            ),
            Customer(2, "Bob", "bob@example.com", date(2023, 1, 2), False, Decimal("500.00")),
        ]

        test_orders = [
            Order(101, 1, datetime(2023, 2, 1, 10, 0, 0), Decimal("200.00"), "completed"),
            Order(102, 2, datetime(2023, 2, 2, 11, 0, 0), Decimal("150.00"), "completed"),
        ]

        @sql_test(
            adapter_type="trino",
            mock_tables=[
                CustomersMockTable(test_customers),
                OrdersMockTable(test_orders),
            ],
            result_class=OrderSummaryResult,
        )
        def query_unqualified_tables():
            return TestCase(
                # Note: SQL uses unqualified table names 'customers' and 'orders'
                query="""
                    SELECT
                        c.customer_id,
                        c.name as customer_name,
                        COUNT(o.order_id) as order_count,
                        SUM(o.amount) as total_spent,
                        AVG(o.amount) as avg_order_value
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    ORDER BY c.customer_id
                """,
                # default_namespace provides the namespace to qualify table names
                # 'customers' becomes 'memory.default.customers'
                # 'orders' becomes 'memory.default.orders'
                default_namespace="memory.default",
                use_physical_tables=use_physical_tables,
            )

        results = query_unqualified_tables()

        # Assertions - verifies that unqualified 'customers' and 'orders' tables
        # were correctly resolved to 'memory.default.customers' and 'memory.default.orders'
        # and matched with mock tables
        assert len(results) == 2
        assert results[0].customer_id == 1
        assert results[0].customer_name == "Alice"
        assert results[0].order_count == 1
        assert results[0].total_spent == Decimal("200.00")
        assert results[1].customer_id == 2
        assert results[1].customer_name == "Bob"
        assert results[1].order_count == 1
        assert results[1].total_spent == Decimal("150.00")
