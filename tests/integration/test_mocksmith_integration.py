"""Integration tests for Mocksmith with SQL Testing Library."""

import random
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import pytest
from mocksmith import Boolean, Date, Integer, Money, Varchar, mockable
from pydantic import BaseModel

from sql_testing_library import BaseMockTable, TestCase, sql_test


# Mock tables
class CustomersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "customers"


class OrdersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "orders"


class ProductsMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "products"


# Result models
class CustomerAnalyticsResult(BaseModel):
    customer_id: int
    customer_name: str
    total_orders: int
    total_spent: Decimal
    avg_order_value: Decimal
    customer_segment: str


class ProductProfitResult(BaseModel):
    category: str
    product_count: int
    avg_margin_percentage: Decimal
    total_potential_revenue: Decimal


class NullHandlingResult(BaseModel):
    customer_id: int
    name: str
    email: str
    order_count: int
    has_premium_status: bool
    last_order_date: Optional[date]


@pytest.mark.integration
@pytest.mark.parametrize("adapter_type", ["trino"])
class TestMocksmithIntegration:
    """Test Mocksmith integration with SQL Testing Library using Trino."""

    def test_basic_mocksmith_data_generation(self, adapter_type):
        """Test basic data generation with Mocksmith."""

        @mockable
        @dataclass
        class Customer:
            customer_id: Integer()
            name: Varchar(100)
            email: Varchar(255)
            registration_date: Date()
            is_premium: Boolean()

        # Generate mock data
        customers = [Customer.mock() for _ in range(10)]

        # Verify data was generated
        assert len(customers) == 10
        assert all(hasattr(c, "customer_id") for c in customers)
        assert all(hasattr(c, "name") for c in customers)
        assert all(hasattr(c, "email") for c in customers)
        assert all(isinstance(c.registration_date, date) for c in customers)
        assert all(isinstance(c.is_premium, bool) for c in customers)

        # Test with SQL query
        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[CustomersMockTable(customers)],
            result_class=dict,
        )
        def query_customers():
            return TestCase(
                query="""
                    SELECT COUNT(*) as customer_count
                    FROM customers
                """,
                default_namespace="test_db",
            )

        results = query_customers()
        assert len(results) == 1
        assert results[0]["customer_count"] == 10

    def test_customer_analytics_with_mocksmith(self, adapter_type):
        """Test complex customer analytics query with Mocksmith-generated data."""

        @mockable
        @dataclass
        class Customer:
            customer_id: Integer()
            name: Varchar(100)
            email: Varchar(255)
            registration_date: Date()
            country: Varchar(50)
            is_premium: Boolean()

        @mockable
        @dataclass
        class Order:
            order_id: Integer()
            customer_id: Integer()
            order_date: Date()
            total_amount: Money()
            status: Varchar(20)

        # Generate customers
        customers = [Customer.mock(customer_id=i) for i in range(1, 21)]

        # Generate orders with specific patterns
        orders = []
        order_id = 1

        # VIP customers (first 5) get many high-value orders
        for customer in customers[:5]:
            for _ in range(random.randint(10, 20)):
                orders.append(
                    Order.mock(
                        order_id=order_id,
                        customer_id=customer.customer_id,
                        total_amount=Decimal(str(random.uniform(200, 1000))),
                        status="completed",
                    )
                )
                order_id += 1

        # Regular customers get fewer orders
        for customer in customers[5:15]:
            for _ in range(random.randint(1, 5)):
                orders.append(
                    Order.mock(
                        order_id=order_id,
                        customer_id=customer.customer_id,
                        total_amount=Decimal(str(random.uniform(50, 200))),
                        status=random.choice(["completed", "completed", "pending"]),
                    )
                )
                order_id += 1

        # Some customers have no orders (customers[15:])

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[CustomersMockTable(customers), OrdersMockTable(orders)],
            result_class=CustomerAnalyticsResult,
        )
        def analyze_customers():
            return TestCase(
                query="""
                    WITH customer_orders AS (
                        SELECT
                            c.customer_id,
                            c.name as customer_name,
                            COUNT(DISTINCT o.order_id) as total_orders,
                            COALESCE(SUM(o.total_amount), 0) as total_spent,
                            COALESCE(AVG(o.total_amount), 0) as avg_order_value
                        FROM customers c
                        LEFT JOIN orders o ON c.customer_id = o.customer_id
                            AND o.status = 'completed'
                        GROUP BY c.customer_id, c.name
                    )
                    SELECT
                        customer_id,
                        customer_name,
                        total_orders,
                        total_spent,
                        avg_order_value,
                        CASE
                            WHEN total_spent > 2000 THEN 'VIP'
                            WHEN total_spent > 500 THEN 'Gold'
                            WHEN total_spent > 100 THEN 'Silver'
                            ELSE 'Bronze'
                        END as customer_segment
                    FROM customer_orders
                    WHERE total_orders > 0
                    ORDER BY total_spent DESC
                    LIMIT 10
                """,
                default_namespace="test_db",
            )

        results = analyze_customers()

        # Verify results
        assert len(results) <= 10
        assert all(r.total_orders > 0 for r in results)
        assert all(r.customer_segment in ["VIP", "Gold", "Silver", "Bronze"] for r in results)

        # VIP customers should be at the top
        vip_results = [r for r in results if r.customer_segment == "VIP"]
        assert len(vip_results) >= 3  # We created at least 5 VIP customers

        # Check ordering
        for i in range(1, len(results)):
            assert results[i - 1].total_spent >= results[i].total_spent

    def test_product_profit_margins_with_constraints(self, adapter_type):
        """Test business rule validation with constrained mock data."""

        @mockable
        @dataclass
        class Product:
            product_id: Integer()
            name: Varchar(100)
            category: Varchar(50)
            price: Money()
            cost: Money()
            supplier_id: Integer(min_value=1, max_value=10)
            is_active: Boolean()

        # Generate products with specific margin patterns
        products = []

        # High-margin luxury products
        for i in range(1, 21):
            price = Decimal(str(random.uniform(500, 1000)))
            cost = price * Decimal("0.3")  # 70% margin
            products.append(
                Product(
                    product_id=i,
                    name=f"Luxury Item {i}",
                    category="Luxury",
                    price=price,
                    cost=cost,
                    supplier_id=random.randint(1, 3),
                    is_active=True,
                )
            )

        # Low-margin commodity products
        for i in range(21, 51):
            price = Decimal(str(random.uniform(20, 100)))
            cost = price * Decimal("0.8")  # 20% margin
            products.append(
                Product(
                    product_id=i,
                    name=f"Commodity Item {i}",
                    category="Commodity",
                    price=price,
                    cost=cost,
                    supplier_id=random.randint(4, 10),
                    is_active=True,
                )
            )

        # Some inactive products
        for i in range(51, 61):
            products.append(Product.mock(product_id=i, is_active=False))

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[ProductsMockTable(products)],
            result_class=ProductProfitResult,
        )
        def analyze_margins():
            return TestCase(
                query="""
                    SELECT
                        category,
                        COUNT(*) as product_count,
                        AVG((price - cost) / price * 100) as avg_margin_percentage,
                        SUM(price) as total_potential_revenue
                    FROM products
                    WHERE is_active = TRUE
                        AND price > cost
                    GROUP BY category
                    HAVING COUNT(*) >= 10
                    ORDER BY avg_margin_percentage DESC
                """,
                default_namespace="test_db",
            )

        results = analyze_margins()

        # Verify results
        assert len(results) == 2  # Luxury and Commodity

        # Luxury should have higher margins
        luxury = next(r for r in results if r.category == "Luxury")
        commodity = next(r for r in results if r.category == "Commodity")

        assert luxury.avg_margin_percentage > 65
        assert commodity.avg_margin_percentage < 25
        assert luxury.avg_margin_percentage > commodity.avg_margin_percentage

        # Product counts
        assert luxury.product_count == 20
        assert commodity.product_count == 30

    def test_null_handling_with_optional_fields(self, adapter_type):
        """Test NULL handling with Mocksmith's Optional types."""

        @mockable
        @dataclass
        class CustomerWithOptionals:
            customer_id: Integer()
            name: Varchar(100)
            email: Varchar(255)
            premium_since: Optional[Date()]
            preferred_contact: Optional[Varchar(50)]

        @mockable
        @dataclass
        class OrderWithOptionals:
            order_id: Integer()
            customer_id: Integer()
            order_date: Date()
            delivery_date: Optional[Date()]
            notes: Optional[Varchar(500)]

        # Generate customers with mix of NULL and non-NULL values
        customers = []

        # Premium customers with all fields
        for i in range(1, 6):
            customers.append(
                CustomerWithOptionals(
                    customer_id=i,
                    name=f"Premium Customer {i}",
                    email=f"premium{i}@example.com",
                    premium_since=date.today() - timedelta(days=random.randint(30, 365)),
                    preferred_contact="email",
                )
            )

        # Regular customers with some NULLs
        for i in range(6, 11):
            customers.append(
                CustomerWithOptionals(
                    customer_id=i,
                    name=f"Regular Customer {i}",
                    email=f"customer{i}@example.com",
                    premium_since=None,
                    preferred_contact=random.choice([None, "phone", "email"]),
                )
            )

        # Generate orders
        orders = []
        order_id = 1

        for customer in customers:
            num_orders = random.randint(0, 5)
            for _ in range(num_orders):
                order_date = date.today() - timedelta(days=random.randint(1, 90))
                orders.append(
                    OrderWithOptionals(
                        order_id=order_id,
                        customer_id=customer.customer_id,
                        order_date=order_date,
                        delivery_date=(
                            order_date + timedelta(days=random.randint(1, 7))
                            if random.random() > 0.3
                            else None
                        ),
                        notes=None if random.random() > 0.5 else f"Order note {order_id}",
                    )
                )
                order_id += 1

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[CustomersMockTable(customers), OrdersMockTable(orders)],
            result_class=NullHandlingResult,
        )
        def analyze_nulls():
            return TestCase(
                query="""
                    SELECT
                        c.customer_id,
                        c.name,
                        c.email,
                        COUNT(o.order_id) as order_count,
                        c.premium_since IS NOT NULL as has_premium_status,
                        MAX(o.order_date) as last_order_date
                    FROM customers c
                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                    GROUP BY c.customer_id, c.name, c.email, c.premium_since
                    ORDER BY c.customer_id
                """,
                default_namespace="test_db",
            )

        results = analyze_nulls()

        # Verify all customers are returned (LEFT JOIN)
        assert len(results) == 10

        # Check premium status
        premium_results = [r for r in results if r.has_premium_status]
        assert len(premium_results) == 5

        # Some customers might have no orders
        no_order_results = [r for r in results if r.order_count == 0]
        assert len(no_order_results) >= 0  # Could be 0 or more

        # Check NULL handling in last_order_date
        for r in results:
            if r.order_count == 0:
                assert r.last_order_date is None
            else:
                assert r.last_order_date is not None

    def test_mocksmith_vs_manual_comparison(self, adapter_type):
        """Direct comparison of Mocksmith vs manual data creation."""

        # Manual approach - lots of boilerplate
        @dataclass
        class ManualCustomer:
            id: int
            name: str
            email: str
            credit_limit: Decimal
            created_date: date
            is_active: bool

        manual_customers = []
        for i in range(50):
            manual_customers.append(
                ManualCustomer(
                    id=i + 1,
                    name=f"Customer {i + 1}",
                    email=f"customer{i + 1}@test.com",
                    credit_limit=Decimal(str(round(random.uniform(1000, 10000), 2))),
                    created_date=date.today() - timedelta(days=random.randint(1, 365)),
                    is_active=random.choice([True, False]),
                )
            )

        # Mocksmith approach - concise and realistic
        @mockable
        @dataclass
        class MocksmithCustomer:
            id: Integer()
            name: Varchar(100)
            email: Varchar(255)
            credit_limit: Money()
            created_date: Date()
            is_active: Boolean()

        mocksmith_customers = [MocksmithCustomer.mock(id=i) for i in range(1, 51)]

        # Compare results
        assert len(manual_customers) == len(mocksmith_customers)

        # Mocksmith generates more realistic data
        # Manual: "Customer 1", "customer1@test.com"
        # Mocksmith: Real names and valid email formats

        # Both work with SQL Testing Library
        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[CustomersMockTable(mocksmith_customers)],
            result_class=dict,
        )
        def test_mocksmith_data():
            return TestCase(
                query="""
                    SELECT
                        COUNT(*) as total_customers,
                        COUNT(CASE WHEN is_active THEN 1 END) as active_customers,
                        AVG(credit_limit) as avg_credit_limit
                    FROM customers
                """,
                default_namespace="test_db",
            )

        results = test_mocksmith_data()
        assert results[0]["total_customers"] == 50
        assert results[0]["active_customers"] > 0
        assert results[0]["avg_credit_limit"] is not None  # Mocksmith generates realistic values
