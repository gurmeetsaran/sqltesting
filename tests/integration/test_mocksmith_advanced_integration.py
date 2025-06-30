"""Advanced integration tests for Mocksmith with SQL Testing Library.

This file tests enums, builder pattern, and documents any mocksmith limitations.
"""

import random
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional

import pytest
from mocksmith import Boolean, Date, Integer, MockBuilder, Money, Varchar, mockable
from pydantic import BaseModel

from sql_testing_library import BaseMockTable, TestCase, sql_test


# Enums for testing
class CustomerStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    VIP = "vip"


class OrderStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    BANK_TRANSFER = "bank_transfer"
    CASH = "cash"


class ProductCategory(str, Enum):
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    FOOD = "food"
    BOOKS = "books"
    HOME = "home"
    SPORTS = "sports"


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
class CustomerSegmentResult(BaseModel):
    customer_id: int
    name: str
    status: str
    total_spent: Decimal
    order_count: int
    segment: str
    preferred_payment: Optional[str]


class ProductAnalysisResult(BaseModel):
    category: str
    product_count: int
    avg_price: Decimal
    total_inventory_value: Decimal
    popular_in_category: bool


@pytest.mark.integration
@pytest.mark.parametrize("adapter_type", ["trino"])
class TestMocksmithAdvancedIntegration:
    """Test advanced Mocksmith features with SQL Testing Library."""

    def test_enum_support_with_mocksmith(self, adapter_type):
        """Test enum fields with Mocksmith.

        FINDING: Enum support works when using direct Python enum types
        without mocksmith type wrappers.
        """

        @mockable
        @dataclass
        class Customer:
            customer_id: Integer()
            name: Varchar(100)
            email: Varchar(255)
            status: CustomerStatus  # Direct enum support works!
            registration_date: Date()
            credit_score: Integer(min_value=300, max_value=850)

        # Generate customers - mocksmith automatically handles enum values!
        customers = []
        for i in range(1, 51):
            customer = Customer.mock(customer_id=i)
            customers.append(customer)

        # Verify mocksmith returns actual enum instances
        assert all(isinstance(c.status, CustomerStatus) for c in customers)

        # Create a simple wrapper to convert enum to value for SQL
        @dataclass
        class CustomerSQL:
            customer_id: int
            name: str
            email: str
            status: str  # Enum value as string
            registration_date: date
            credit_score: int

        # Convert for SQL usage
        customers_sql = [
            CustomerSQL(
                customer_id=c.customer_id,
                name=c.name,
                email=c.email,
                status=c.status.value,  # Convert enum to value
                registration_date=c.registration_date,
                credit_score=c.credit_score,
            )
            for c in customers
        ]

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[CustomersMockTable(customers_sql)],
            result_class=dict,
        )
        def query_by_status():
            return TestCase(
                query="""
                    SELECT
                        status,
                        COUNT(*) as customer_count,
                        AVG(credit_score) as avg_credit_score
                    FROM customers
                    GROUP BY status
                    ORDER BY customer_count DESC
                """,
                default_namespace="test_db",
            )

        results = query_by_status()

        # All status values should be valid enum values (stored as strings in DB)
        for row in results:
            assert row["status"] in [s.value for s in CustomerStatus]
            assert row["customer_count"] > 0

    def test_builder_pattern_with_scenarios(self, adapter_type):
        """Test Mocksmith's builder pattern for complex test scenarios."""

        @mockable
        @dataclass
        class Order:
            order_id: Integer()
            customer_id: Integer()
            order_date: Date()
            # ISSUE: Money type doesn't support min_value/max_value constraints
            # Would be nice: total_amount: Money(min_value="10", max_value="10000")
            total_amount: Money()  # Can generate negative values!
            status: Varchar(20)
            payment_method: Varchar(20)
            shipping_cost: Money()
            discount_amount: Money()

        # Create builder for different scenarios
        # ISSUE: MockBuilder API is not intuitive - must use with_values().build()
        # Would be nice: order_builder.build(order_id=1, ...) or order_builder.mock(order_id=1, ...)

        # Scenario 1: High-value completed orders
        high_value_orders = []
        for i in range(1, 21):
            order = (
                MockBuilder(Order)
                .with_values(
                    order_id=i,
                    customer_id=random.randint(1, 10),
                    status=OrderStatus.COMPLETED.value,
                    payment_method=PaymentMethod.CREDIT_CARD.value,
                    # ISSUE: Have to manually ensure positive values
                    total_amount=Decimal(str(random.uniform(500, 2000))),
                    shipping_cost=Decimal("0"),  # Free shipping for high-value
                    discount_amount=Decimal(str(random.uniform(50, 200))),
                )
                .build()
            )
            high_value_orders.append(order)

        # Scenario 2: Cancelled/refunded orders
        cancelled_orders = []
        for i in range(21, 31):
            order = (
                MockBuilder(Order)
                .with_values(
                    order_id=i,
                    customer_id=random.randint(11, 20),
                    status=random.choice([OrderStatus.CANCELLED.value, OrderStatus.REFUNDED.value]),
                    payment_method=random.choice(list(PaymentMethod)).value,
                    total_amount=Decimal(str(random.uniform(10, 500))),
                    shipping_cost=Decimal(str(random.uniform(5, 25))),
                    discount_amount=Decimal("0"),
                )
                .build()
            )
            cancelled_orders.append(order)

        # Scenario 3: Regular orders with various payment methods
        regular_orders = []
        for i in range(31, 81):
            # For mixed random/specific data, use Order.mock() directly
            order = Order.mock(
                order_id=i,
                customer_id=random.randint(1, 50),
                status=random.choice(
                    [
                        OrderStatus.PENDING.value,
                        OrderStatus.PROCESSING.value,
                        OrderStatus.COMPLETED.value,
                    ]
                ),
                payment_method=random.choice(list(PaymentMethod)).value,
            )
            # ISSUE: Need to fix potentially negative amounts
            if order.total_amount < 0:
                order.total_amount = abs(order.total_amount)
            if order.shipping_cost < 0:
                order.shipping_cost = abs(order.shipping_cost)
            if order.discount_amount < 0:
                order.discount_amount = Decimal("0")

            regular_orders.append(order)

        all_orders = high_value_orders + cancelled_orders + regular_orders

        @sql_test(
            adapter_type=adapter_type, mock_tables=[OrdersMockTable(all_orders)], result_class=dict
        )
        def analyze_payment_methods():
            return TestCase(
                query="""
                    SELECT
                        payment_method,
                        COUNT(*) as order_count,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_count,
                        AVG(total_amount) as avg_order_value,
                        SUM(total_amount) as total_revenue,
                        AVG(shipping_cost) as avg_shipping_cost
                    FROM orders
                    WHERE total_amount > 0  -- Filter out any negative values
                    GROUP BY payment_method
                    ORDER BY total_revenue DESC
                """,
                default_namespace="test_db",
            )

        results = analyze_payment_methods()

        # Verify payment methods
        for row in results:
            assert row["payment_method"] in [pm.value for pm in PaymentMethod]
            assert row["avg_order_value"] > 0

    def test_complex_builder_with_relationships(self, adapter_type):
        """Test builder pattern with related entities and constraints."""

        @mockable
        @dataclass
        class Product:
            product_id: Integer()
            name: Varchar(100)
            category: Varchar(50)
            # ISSUE: Would be nice to have Decimal type with constraints
            # price: Decimal(min_value="0.01", max_value="9999.99", decimal_places=2)
            price: Money()
            cost: Money()
            stock_quantity: Integer(min_value=0, max_value=1000)
            is_featured: Boolean()
            # ISSUE: No built-in URL type
            # image_url: HttpUrl()  # Would be nice
            image_url: Varchar(255)

        @mockable
        @dataclass
        class CustomerWithPreferences:
            customer_id: Integer()
            name: Varchar(100)
            status: Varchar(20)
            preferred_categories: Varchar(255)  # JSON array as string
            # ISSUE: No native array/list support in mocksmith types
            # preferred_categories: List[Varchar(50)]  # Would be nice
            lifetime_value: Money()
            last_order_date: Date()

        # Create products by category
        products = []
        for category in ProductCategory:
            # Featured products in each category
            for i in range(3):
                product = (
                    MockBuilder(Product)
                    .with_values(
                        product_id=len(products) + 1,
                        name=f"Featured {category.value} Product {i + 1}",
                        category=category.value,
                        price=Decimal(str(random.uniform(50, 500))),
                        cost=Decimal(str(random.uniform(20, 200))),
                        stock_quantity=random.randint(50, 200),
                        is_featured=True,
                        image_url=f"https://example.com/products/{category.value}_{i + 1}.jpg",
                    )
                    .build()
                )
                # Ensure price > cost
                if product.cost >= product.price:
                    product.cost = product.price * Decimal("0.6")
                products.append(product)

            # Regular products
            for _ in range(7):
                product = Product.mock(
                    product_id=len(products) + 1, category=category.value, is_featured=False
                )
                # Fix negative values
                product.price = abs(product.price) if product.price < 0 else product.price
                product.cost = abs(product.cost) if product.cost < 0 else product.cost
                # Ensure minimum price
                if product.price < Decimal("1"):
                    product.price = Decimal(str(random.uniform(10, 100)))
                # Ensure profit margin
                if product.cost >= product.price:
                    product.cost = product.price * Decimal("0.7")
                products.append(product)

        # Create customers with category preferences
        customers = []
        for i in range(1, 31):
            # Randomly select 1-3 preferred categories
            preferred = random.sample(list(ProductCategory), random.randint(1, 3))
            preferred_str = ",".join([c.value for c in preferred])

            customer = CustomerWithPreferences.mock(
                customer_id=i,
                status=random.choice(list(CustomerStatus)).value,
                preferred_categories=f"[{preferred_str}]",
            )
            # Fix potentially negative lifetime value
            if customer.lifetime_value < 0:
                customer.lifetime_value = abs(customer.lifetime_value)
            customers.append(customer)

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[ProductsMockTable(products), CustomersMockTable(customers)],
            result_class=ProductAnalysisResult,
        )
        def analyze_product_categories():
            return TestCase(
                query="""
                    SELECT
                        category,
                        COUNT(*) as product_count,
                        AVG(price) as avg_price,
                        SUM(price * stock_quantity) as total_inventory_value,
                        SUM(CASE WHEN is_featured THEN 1 ELSE 0 END) > 0 as popular_in_category
                    FROM products
                    WHERE price > 0 AND cost > 0 AND price > cost
                    GROUP BY category
                    ORDER BY total_inventory_value DESC
                """,
                default_namespace="test_db",
            )

        results = analyze_product_categories()

        # Should have all categories
        assert len(results) == len(ProductCategory)
        for row in results:
            assert row.category in [c.value for c in ProductCategory]
            assert row.product_count > 0
            assert row.avg_price > 0
            assert row.popular_in_category  # Each category has featured products

    def test_mocksmith_limitations_documentation(self, adapter_type):
        """Document and test workarounds for Mocksmith limitations."""

        # Issues found during testing:
        issues = """
        MOCKSMITH IMPROVEMENT SUGGESTIONS:

        1. ENUM SUPPORT: ✅ WORKS!
           - Current: Direct enum support with Python type annotations
           - Usage: status: MyEnum (not status: Varchar())
           - Note: Returns enum instances, need .value for SQL storage

        2. MONEY TYPE CONSTRAINTS:
           - Current: Money() can generate negative values
           - Desired: Money(min_value="0", max_value="10000")
           - Workaround: Manual abs() or Decimal with checks

        3. DECIMAL TYPE: ✅ WORKS!
           - Current: Native Decimal type supported
           - Usage: balance: Decimal
           - Note: For constraints use Money() with validation

        4. ARRAY/LIST SUPPORT: ✅ WORKS!
           - Current: Full list support with type annotations
           - Usage: tags: List[str] or ids: List[Integer()]
           - Note: Generates lists with random length and values

        5. URL/EMAIL VALIDATION: ✅ WORKS!
           - Current: Use Pydantic types directly
           - Usage: email: EmailStr, website: HttpUrl
           - Also: IPvAnyAddress, UUID4, etc.

        6. JSON/DICT TYPE: ✅ WORKS!
           - Current: Native Dict support
           - Usage: metadata: Dict[str, Any]
           - Note: Convert to JSON string for SQL storage

        7. CONSTRAINED STRINGS:
           - Current: Varchar(n) only limits length
           - Desired: Varchar(pattern=r'^[A-Z]{2}$') for regex
           - Workaround: Manual validation after generation

        8. FOREIGN KEY RELATIONSHIPS:
           - Current: Manual ID assignment
           - Desired: ForeignKey(Customer) or Reference(Customer.id)
           - Workaround: Integer() with manual ID management

        9. CONDITIONAL GENERATION:
           - Current: All fields independent
           - Desired: Field dependencies (if status='vip' then discount > 20)
           - Workaround: Post-generation modification

        10. BUILDER IMPROVEMENTS:
            - Current: MockBuilder.build() vs mock() confusing
            - Desired: Clear distinction or single method with mode parameter
            - Note: Both work but unclear when to use which
        """

        # Test example showing workarounds
        @mockable
        @dataclass
        class RealWorldExample:
            # Enum workaround
            status: Varchar(20)  # Would prefer: Enum(CustomerStatus)

            # Money constraints workaround
            price: Money()  # Would prefer: Money(min_value="0", max_value="1000")

            # List workaround
            tags: Varchar(500)  # Would prefer: List[Varchar(50)]

            # URL workaround
            website: Varchar(255)  # Would prefer: HttpUrl()

            # JSON workaround
            metadata: Varchar(1000)  # Would prefer: Json() or Dict()

        # Generate with workarounds
        examples = []
        for i in range(10):
            example = RealWorldExample.mock()

            # Fix enum
            example.status = random.choice(list(CustomerStatus)).value

            # Fix money
            example.price = abs(example.price)

            # Fix list (JSON array)
            tags = [f"tag{j}" for j in range(random.randint(1, 5))]
            example.tags = f"[{','.join(tags)}]"

            # Fix URL
            example.website = f"https://example{i}.com"

            # Fix JSON
            example.metadata = f'{{"key{i}": "value{i}"}}'

            examples.append(example)

        # Verify workarounds work with SQL
        @sql_test(
            adapter_type=adapter_type, mock_tables=[CustomersMockTable(examples)], result_class=dict
        )
        def test_workarounds():
            return TestCase(
                query="""
                    SELECT
                        COUNT(*) as total,
                        COUNT(DISTINCT status) as unique_statuses,
                        AVG(price) as avg_price
                    FROM customers
                    WHERE price >= 0
                """,
                default_namespace="test_db",
            )

        results = test_workarounds()
        assert results[0]["total"] == 10
        assert results[0]["avg_price"] >= 0

        # Print issues for visibility
        print(issues)
