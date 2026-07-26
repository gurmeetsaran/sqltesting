"""Integration tests for ClickHouse adapter with pytest configuration.

These tests use the `@sql_test(adapter_type="clickhouse", ...)` decorator
and require a live ClickHouse server (configured via pytest.ini's
`[sql_testing.clickhouse]` section).
"""

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
    customer_id: int
    name: str
    email: str
    signup_date: date
    is_premium: bool
    lifetime_value: Optional[Decimal] = None


@dataclass
class Order:
    order_id: int
    customer_id: int
    order_date: datetime
    amount: Decimal
    status: str


@dataclass
class Product:
    product_id: int
    name: str
    category: str
    price: Decimal
    in_stock: bool


class CustomerResult(BaseModel):
    customer_id: int
    name: str
    email: str
    total_orders: int
    total_amount: Decimal


class OrderSummaryResult(BaseModel):
    customer_id: int
    customer_name: str
    order_count: int
    total_spent: Decimal
    avg_order_value: Decimal


class ProductAnalyticsResult(BaseModel):
    category: str
    product_count: int
    avg_price: Decimal


class CustomersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "default"

    def get_table_name(self) -> str:
        return "customers"


class OrdersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "default"

    def get_table_name(self) -> str:
        return "orders"


class ProductsMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "default"

    def get_table_name(self) -> str:
        return "products"


@pytest.mark.integration
@pytest.mark.clickhouse
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_table_mode"]
)
class TestClickHouseIntegration:
    """Integration tests for ClickHouse adapter using real database connections."""

    def setup_method(self):
        self.customers_data = [
            Customer(
                1, "Alice Johnson", "alice@example.com", date(2023, 1, 15), True, Decimal("1500.00")
            ),
            Customer(
                2, "Bob Smith", "bob@example.com", date(2023, 2, 20), False, Decimal("750.00")
            ),
            Customer(
                3, "Carol Davis", "carol@example.com", date(2023, 3, 10), True, Decimal("2250.00")
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
        @sql_test(
            adapter_type="clickhouse",
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
                        CAST(0 AS Int64) AS total_orders,
                        CAST('0.00' AS Decimal(38, 9)) AS total_amount
                    FROM customers WHERE customer_id = 1
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_customer()
        assert len(results) == 1
        assert results[0].customer_id == 1
        assert results[0].name == "Alice Johnson"

    def test_customer_order_join(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=OrderSummaryResult,
            parallel_table_creation=False,  # clickhouse-connect client is not thread-safe
        )
        def query_customer_orders():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id AS customer_id,
                        c.name AS customer_name,
                        count(o.order_id) AS order_count,
                        sum(o.amount) AS total_spent,
                        avg(o.amount) AS avg_order_value
                    FROM customers AS c
                    LEFT JOIN orders AS o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    ORDER BY total_spent DESC
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_customer_orders()
        assert len(results) >= 1
        assert all(hasattr(r, "customer_id") for r in results)

    def test_date_filtering(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
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
                        CAST(0 AS Int64) AS total_orders,
                        CAST('0.00' AS Decimal(38, 9)) AS total_amount
                    FROM customers
                    WHERE signup_date >= toDate('2023-02-01')
                    ORDER BY signup_date DESC
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_recent_customers()
        assert len(results) >= 1

    def test_null_handling(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
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
                        CAST(0 AS Int64) AS total_orders,
                        ifNull(lifetime_value, CAST('0.00' AS Decimal(38, 9))) AS total_amount
                    FROM customers
                    WHERE lifetime_value IS NOT NULL OR customer_id = 2
                    ORDER BY customer_id
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_with_nulls()
        assert len(results) == 2
        assert results[1].total_amount == Decimal("0.000000000")

    def test_string_functions(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[CustomersMockTable(self.customers_data)],
            result_class=CustomerResult,
        )
        def query_string_ops():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        upper(name) AS name,
                        lower(email) AS email,
                        CAST(0 AS Int64) AS total_orders,
                        CAST('0.00' AS Decimal(38, 9)) AS total_amount
                    FROM customers
                    WHERE length(name) > 8
                    ORDER BY name
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_string_ops()
        assert len(results) >= 1

    def test_aggregation(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[ProductsMockTable(self.products_data)],
            result_class=ProductAnalyticsResult,
        )
        def query_product_analytics():
            return TestCase(
                query="""
                    SELECT
                        category,
                        count() AS product_count,
                        avg(price) AS avg_price
                    FROM products
                    WHERE in_stock = TRUE
                    GROUP BY category
                    HAVING count() >= 1
                    ORDER BY avg_price DESC
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_product_analytics()
        assert len(results) >= 1
        assert all(r.product_count > 0 for r in results)

    def test_boolean_operations(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
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
                        CAST(0 AS Int64) AS total_orders,
                        CAST('0.00' AS Decimal(38, 9)) AS total_amount
                    FROM customers
                    WHERE is_premium = TRUE
                      AND lifetime_value > CAST('1000.00' AS Decimal(38, 9))
                    ORDER BY lifetime_value DESC
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_premium_customers()
        assert len(results) >= 1
        assert results[0].customer_id == 3

    def test_window_functions(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=OrderSummaryResult,
            parallel_table_creation=False,  # clickhouse-connect client is not thread-safe
        )
        def query_customer_ranking():
            return TestCase(
                query="""
                    WITH customer_totals AS (
                        SELECT
                            c.customer_id AS customer_id,
                            c.name AS customer_name,
                            count(o.order_id) AS order_count,
                            sum(o.amount) AS total_spent,
                            avg(o.amount) AS avg_order_value
                        FROM customers AS c
                        LEFT JOIN orders AS o ON c.customer_id = o.customer_id
                        GROUP BY c.customer_id, c.name
                    )
                    SELECT
                        customer_id,
                        customer_name,
                        order_count,
                        total_spent,
                        avg_order_value
                    FROM customer_totals
                    WHERE total_spent > CAST('0.00' AS Decimal(38, 9))
                    ORDER BY total_spent DESC
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_customer_ranking()
        assert len(results) >= 1

    def test_case_statements(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[ProductsMockTable(self.products_data)],
            result_class=ProductAnalyticsResult,
        )
        def query_case():
            return TestCase(
                query="""
                    SELECT
                        CASE
                            WHEN price > CAST('500.00' AS Decimal(38, 9)) THEN 'High-End'
                            WHEN price > CAST('200.00' AS Decimal(38, 9)) THEN 'Mid-Range'
                            ELSE 'Budget'
                        END AS category,
                        count() AS product_count,
                        avg(price) AS avg_price
                    FROM products
                    GROUP BY
                        CASE
                            WHEN price > CAST('500.00' AS Decimal(38, 9)) THEN 'High-End'
                            WHEN price > CAST('200.00' AS Decimal(38, 9)) THEN 'Mid-Range'
                            ELSE 'Budget'
                        END
                    ORDER BY avg_price DESC
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_case()
        assert len(results) >= 1
        assert all(r.product_count > 0 for r in results)

    def test_subquery_operations(self, use_physical_tables):
        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[
                CustomersMockTable(self.customers_data),
                OrdersMockTable(self.orders_data),
            ],
            result_class=CustomerResult,
            parallel_table_creation=False,  # clickhouse-connect client is not thread-safe
        )
        def query_customers_with_orders():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id AS customer_id,
                        c.name AS name,
                        c.email AS email,
                        CAST(0 AS Int64) AS total_orders,
                        CAST('0.00' AS Decimal(38, 9)) AS total_amount
                    FROM customers AS c
                    WHERE c.customer_id IN (
                        SELECT DISTINCT customer_id FROM orders WHERE status = 'completed'
                    )
                    ORDER BY c.customer_id
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_customers_with_orders()
        assert len(results) >= 1

    def test_unqualified_table_names_with_default_namespace(self, use_physical_tables):
        test_customers = [
            Customer(1, "Alice", "alice@example.com", date(2023, 1, 1), True, Decimal("1000.00")),
            Customer(2, "Bob", "bob@example.com", date(2023, 1, 2), False, Decimal("500.00")),
        ]

        test_orders = [
            Order(101, 1, datetime(2023, 2, 1, 10, 0, 0), Decimal("200.00"), "completed"),
            Order(102, 2, datetime(2023, 2, 2, 11, 0, 0), Decimal("150.00"), "completed"),
        ]

        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[
                CustomersMockTable(test_customers),
                OrdersMockTable(test_orders),
            ],
            result_class=OrderSummaryResult,
            parallel_table_creation=False,  # clickhouse-connect client is not thread-safe
        )
        def query_unqualified():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id AS customer_id,
                        c.name AS customer_name,
                        count(o.order_id) AS order_count,
                        sum(o.amount) AS total_spent,
                        avg(o.amount) AS avg_order_value
                    FROM customers AS c
                    LEFT JOIN orders AS o ON c.customer_id = o.customer_id
                    WHERE o.status = 'completed'
                    GROUP BY c.customer_id, c.name
                    ORDER BY c.customer_id
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_unqualified()
        assert len(results) == 2
        assert results[0].customer_id == 1
        assert results[0].customer_name == "Alice"
        assert results[0].order_count == 1
        assert results[0].total_spent == Decimal("200.000000000")
        assert results[1].customer_id == 2
        assert results[1].customer_name == "Bob"
        assert results[1].total_spent == Decimal("150.000000000")

    def test_array_operations(self, use_physical_tables):
        """ClickHouse-specific: array literals and functions."""

        @sql_test(
            adapter_type="clickhouse",
            mock_tables=[CustomersMockTable(self.customers_data)],
            result_class=CustomerResult,
        )
        def query_with_arrays():
            return TestCase(
                query="""
                    SELECT
                        customer_id,
                        name,
                        email,
                        CAST(0 AS Int64) AS total_orders,
                        CAST('0.00' AS Decimal(38, 9)) AS total_amount
                    FROM customers
                    WHERE has([1, 3], customer_id)
                    ORDER BY customer_id
                """,
                default_namespace="default",
                use_physical_tables=use_physical_tables,
            )

        results = query_with_arrays()
        assert len(results) == 2
        assert [r.customer_id for r in results] == [1, 3]
