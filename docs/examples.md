---
layout: default
title: Examples
nav_order: 6
---

# Examples
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Basic Examples

### Simple Query Test

Testing a basic SELECT query with filtering:

```python
from dataclasses import dataclass
from sql_testing_library import sql_test, TestCase
from sql_testing_library.mock_table import BaseMockTable
from pydantic import BaseModel

@dataclass
class Employee:
    id: int
    name: str
    department: str
    salary: float
    active: bool

class EmployeesMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "company_db"

    def get_table_name(self) -> str:
        return "employees"

class ActiveEmployeeResult(BaseModel):
    name: str
    department: str

@sql_test(
    mock_tables=[
        EmployeesMockTable([
            Employee(1, "Alice", "Engineering", 100000, True),
            Employee(2, "Bob", "Sales", 80000, False),
            Employee(3, "Charlie", "Engineering", 95000, True),
            Employee(4, "Diana", "HR", 85000, True)
        ])
    ],
    result_class=ActiveEmployeeResult
)
def test_active_employees():
    return TestCase(
        query="""
            SELECT name, department
            FROM employees
            WHERE active = true
            ORDER BY name
        """,
        default_namespace="company_db"
    )
```

### JOIN Query Test

Testing queries with multiple tables:

```python
@dataclass
class Order:
    order_id: int
    user_id: int
    product_id: int
    quantity: int
    total: float

@dataclass
class Product:
    product_id: int
    name: str
    category: str
    price: float

class OrdersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "sales_db"

    def get_table_name(self) -> str:
        return "orders"

class ProductsMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "sales_db"

    def get_table_name(self) -> str:
        return "products"

class OrderDetailResult(BaseModel):
    order_id: int
    product_name: str
    quantity: int
    total: float

@sql_test(
    mock_tables=[
        OrdersMockTable([
            Order(1, 101, 1, 2, 39.98),
            Order(2, 102, 2, 1, 999.99),
            Order(3, 101, 3, 3, 89.97)
        ]),
        ProductsMockTable([
            Product(1, "Mouse", "Electronics", 19.99),
            Product(2, "Laptop", "Electronics", 999.99),
            Product(3, "Notebook", "Stationery", 29.99)
        ])
    ],
    result_class=OrderDetailResult
)
def test_order_details():
    return TestCase(
        query="""
            SELECT
                o.order_id,
                p.name as product_name,
                o.quantity,
                o.total
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.total > 50
            ORDER BY o.order_id
        """,
        default_namespace="sales_db"
    )
```

## Advanced Examples

### Working with Arrays

Testing queries with array data types:

```python
from typing import List
from decimal import Decimal

@dataclass
class Customer:
    customer_id: int
    name: str
    tags: List[str]
    purchase_amounts: List[Decimal]

class CustomersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "analytics.customer_data"

    def get_table_name(self) -> str:
        return "customers"

class CustomerTagResult(BaseModel):
    name: str
    tag_count: int
    total_purchases: Decimal

@sql_test(
    mock_tables=[
        CustomersMockTable([
            Customer(
                1,
                "Alice",
                ["vip", "frequent", "email"],
                [Decimal("99.99"), Decimal("149.50"), Decimal("79.99")]
            ),
            Customer(
                2,
                "Bob",
                ["new"],
                [Decimal("29.99")]
            ),
            Customer(
                3,
                "Charlie",
                ["vip", "phone"],
                [Decimal("299.99"), Decimal("199.99")]
            )
        ])
    ],
    result_class=CustomerTagResult
)
def test_customer_analytics():
    return TestCase(
        query="""
            SELECT
                name,
                ARRAY_LENGTH(tags) as tag_count,
                (SELECT SUM(amount) FROM UNNEST(purchase_amounts) as amount) as total_purchases
            FROM customers
            WHERE 'vip' IN UNNEST(tags)
            ORDER BY name
        """,
        default_namespace="analytics.customer_data"
    )
```

### Date and Time Operations

Working with temporal data:

```python
from datetime import date, datetime

@dataclass
class Event:
    event_id: int
    event_name: str
    event_date: date
    created_at: datetime
    duration_hours: float

class EventsMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "events_db"

    def get_table_name(self) -> str:
        return "events"

class EventSummaryResult(BaseModel):
    month: str
    event_count: int
    total_hours: float

@sql_test(
    mock_tables=[
        EventsMockTable([
            Event(1, "Conference", date(2024, 1, 15), datetime(2024, 1, 1, 10, 30), 8.0),
            Event(2, "Workshop", date(2024, 1, 20), datetime(2024, 1, 5, 14, 0), 4.0),
            Event(3, "Seminar", date(2024, 2, 10), datetime(2024, 2, 1, 9, 0), 3.0),
            Event(4, "Training", date(2024, 2, 25), datetime(2024, 2, 15, 13, 30), 6.0)
        ])
    ],
    result_class=EventSummaryResult
)
def test_event_summary():
    return TestCase(
        query="""
            SELECT
                FORMAT_DATE('%Y-%m', event_date) as month,
                COUNT(*) as event_count,
                SUM(duration_hours) as total_hours
            FROM events
            WHERE event_date >= '2024-01-01'
            GROUP BY month
            ORDER BY month
        """,
        default_namespace="events_db"
    )
```

### Window Functions

Testing analytical queries with window functions:

```python
@dataclass
class Sale:
    sale_id: int
    salesperson: str
    region: str
    amount: Decimal
    sale_date: date

class SalesMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "sales_analytics"

    def get_table_name(self) -> str:
        return "sales"

class SalesRankResult(BaseModel):
    salesperson: str
    region: str
    amount: Decimal
    region_rank: int
    total_rank: int

@sql_test(
    mock_tables=[
        SalesMockTable([
            Sale(1, "Alice", "North", Decimal("5000"), date(2024, 1, 1)),
            Sale(2, "Bob", "North", Decimal("7000"), date(2024, 1, 2)),
            Sale(3, "Charlie", "South", Decimal("6000"), date(2024, 1, 3)),
            Sale(4, "Diana", "South", Decimal("8000"), date(2024, 1, 4)),
            Sale(5, "Eve", "North", Decimal("4500"), date(2024, 1, 5))
        ])
    ],
    result_class=SalesRankResult
)
def test_sales_ranking():
    return TestCase(
        query="""
            SELECT
                salesperson,
                region,
                amount,
                RANK() OVER (PARTITION BY region ORDER BY amount DESC) as region_rank,
                RANK() OVER (ORDER BY amount DESC) as total_rank
            FROM sales
            WHERE sale_date >= '2024-01-01'
            ORDER BY total_rank
        """,
        default_namespace="sales_analytics"
    )
```

## Database-Specific Examples

### BigQuery-Specific Features

Using BigQuery SQL features in your queries (note: the library creates CTEs using UNION ALL, not STRUCT):

```python
# BigQuery adapter test
@sql_test(
    adapter_type="bigquery",
    mock_tables=[products_mock],
    result_class=dict
)
def test_bigquery_structs():
    return TestCase(
        query="""
            SELECT
                STRUCT(
                    product_id as id,
                    name,
                    ARRAY[
                        STRUCT('color' as attribute, 'red' as value),
                        STRUCT('size' as attribute, 'large' as value)
                    ] as attributes
                ) as product_info
            FROM products
            WHERE category = 'Electronics'
        """,
        default_namespace="my-project.my_dataset"
    )
```

### Athena/Trino Array Operations

Using Presto/Trino SQL array functions:

```python
# Athena adapter test
@sql_test(
    adapter_type="athena",
    mock_tables=[events_mock],
    result_class=dict
)
def test_athena_arrays():
    return TestCase(
        query="""
            SELECT
                event_name,
                CARDINALITY(attendees) as attendee_count,
                FILTER(attendees, x -> x LIKE '%@company.com') as company_attendees
            FROM events
            WHERE CONTAINS(attendees, 'alice@company.com')
        """,
        default_namespace="analytics_db"
    )
```

### Redshift JSON Handling

Working with JSON in Redshift:

```python
# Redshift adapter test
@sql_test(
    adapter_type="redshift",
    mock_tables=[user_events_mock],
    result_class=dict
)
def test_redshift_json():
    return TestCase(
        query="""
            SELECT
                user_id,
                JSON_EXTRACT_PATH_TEXT(event_data, 'action') as action,
                JSON_EXTRACT_PATH_TEXT(event_data, 'timestamp')::timestamp as event_time
            FROM user_events
            WHERE JSON_EXTRACT_PATH_TEXT(event_data, 'action') = 'purchase'
        """,
        default_namespace="events_db"
    )
```

## Testing Patterns

### Testing CTEs

Breaking down complex queries:

```python
@sql_test(
    mock_tables=[transactions_mock, customers_mock],
    result_class=CustomerMetricsResult
)
def test_customer_metrics():
    return TestCase(
        query="""
            WITH monthly_totals AS (
                SELECT
                    customer_id,
                    DATE_TRUNC('month', transaction_date) as month,
                    SUM(amount) as monthly_total
                FROM transactions
                GROUP BY customer_id, month
            ),
            customer_stats AS (
                SELECT
                    customer_id,
                    AVG(monthly_total) as avg_monthly_spend,
                    MAX(monthly_total) as max_monthly_spend,
                    COUNT(DISTINCT month) as active_months
                FROM monthly_totals
                GROUP BY customer_id
            )
            SELECT
                c.name,
                cs.avg_monthly_spend,
                cs.max_monthly_spend,
                cs.active_months
            FROM customer_stats cs
            JOIN customers c ON cs.customer_id = c.customer_id
            WHERE cs.active_months >= 3
            ORDER BY cs.avg_monthly_spend DESC
        """,
        default_namespace="analytics_db"
    )
```

### Testing Error Cases

Validating business logic:

```python
@sql_test(
    mock_tables=[
        InventoryMockTable([
            {"product_id": 1, "quantity": 5, "min_stock": 10},
            {"product_id": 2, "quantity": 15, "min_stock": 10},
            {"product_id": 3, "quantity": 0, "min_stock": 5}
        ])
    ],
    result_class=LowStockAlert
)
def test_low_stock_alerts():
    return TestCase(
        query="""
            SELECT
                product_id,
                quantity,
                min_stock,
                CASE
                    WHEN quantity = 0 THEN 'OUT_OF_STOCK'
                    WHEN quantity < min_stock THEN 'LOW_STOCK'
                    ELSE 'NORMAL'
                END as stock_status
            FROM inventory
            WHERE quantity < min_stock
            ORDER BY quantity ASC
        """,
        default_namespace="inventory_db"
    )
```

### Dynamic Test Generation

Creating parameterized tests:

```python
import pytest

test_cases = [
    ("North", 2),  # region, expected_count
    ("South", 1),
    ("East", 0)
]

@pytest.mark.parametrize("region,expected_count", test_cases)
def test_sales_by_region(region, expected_count):
    @sql_test(
        mock_tables=[
            SalesMockTable([
                Sale(1, "Alice", "North", 5000, date(2024, 1, 1)),
                Sale(2, "Bob", "North", 7000, date(2024, 1, 2)),
                Sale(3, "Charlie", "South", 6000, date(2024, 1, 3))
            ])
        ],
        result_class=dict
    )
    def _test():
        return TestCase(
            query=f"""
                SELECT COUNT(*) as count
                FROM sales
                WHERE region = '{region}'
            """,
            default_namespace="sales_db"
        )

    results = _test()
    assert results[0]['count'] == expected_count
```

## Best Practices

### 1. Organize Mock Tables

Create a separate module for mock tables:

```python
# tests/mocks/tables.py
from sql_testing_library.mock_table import BaseMockTable

class BaseCompanyMockTable(BaseMockTable):
    """Base class for all company mock tables"""
    def get_database_name(self) -> str:
        return "company_db.public"

class UsersMockTable(BaseCompanyMockTable):
    def get_table_name(self) -> str:
        return "users"

class OrdersMockTable(BaseCompanyMockTable):
    def get_table_name(self) -> str:
        return "orders"
```

### 2. Reusable Test Data

Create factory functions for test data:

```python
# tests/factories.py
from datetime import date, timedelta
from decimal import Decimal

def create_test_orders(count: int, start_date: date):
    """Generate test orders with sequential dates"""
    return [
        Order(
            order_id=i,
            user_id=100 + (i % 5),
            amount=Decimal(str(50 + i * 10)),
            order_date=start_date + timedelta(days=i)
        )
        for i in range(1, count + 1)
    ]
```

### 3. Test Complex Business Logic

Focus on testing SQL logic, not just syntax:

```python
@sql_test(
    mock_tables=[
        OrdersMockTable(create_test_orders(100, date(2024, 1, 1))),
        CustomersMockTable(create_test_customers())
    ],
    result_class=CustomerLTVResult
)
def test_customer_lifetime_value():
    """Test LTV calculation includes all business rules"""
    return TestCase(
        query=load_sql_file("queries/customer_ltv.sql"),
        default_namespace="analytics_db",
        description="Verify LTV calculation handles refunds, discounts, and loyalty tiers"
    )
```

### 4. Document Expected Results

Make tests self-documenting:

```python
def test_quarterly_revenue():
    """
    Test quarterly revenue aggregation.

    Expected results:
    - Q1 2024: $15,000 (3 orders)
    - Q2 2024: $22,000 (4 orders)
    - Excludes cancelled orders
    - Applies regional tax rates
    """
    # Test implementation...
```
