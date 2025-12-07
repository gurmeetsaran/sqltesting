---
layout: default
title: Examples - SQL Unit Testing with Python
nav_order: 3
description: "Real-world examples of SQL unit testing with Python. Learn how to test BigQuery, Snowflake, Redshift, Athena queries with mock data using pytest."
---

# SQL Testing Examples
{: .no_toc }

Real-world examples of SQL unit testing for BigQuery, Snowflake, Athena, and other cloud databases using Python and pytest.
{: .fs-6 .fw-300 }

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

### Working with Maps (BigQuery/Athena/Trino/Redshift)

Map types are supported for BigQuery, Athena, Trino, and Redshift adapters:

```python
from typing import Dict, Optional

@dataclass
class UserPreferences:
    user_id: int
    username: str
    settings: Dict[str, str]           # String to string map
    scores: Dict[str, int]             # String to integer map
    metadata: Optional[Dict[str, str]] # Optional map

class UserPreferencesMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "analytics"

    def get_table_name(self) -> str:
        return "user_preferences"

# Mock data with maps
mock_data = [
    UserPreferences(
        user_id=1,
        username="alice",
        settings={"theme": "dark", "notifications": "enabled", "language": "en"},
        scores={"level1": 100, "level2": 85, "level3": 92},
        metadata={"source": "mobile", "version": "2.1"}
    ),
    UserPreferences(
        user_id=2,
        username="bob",
        settings={"theme": "light", "notifications": "disabled"},
        scores={"level1": 95},
        metadata=None  # NULL map
    ),
    UserPreferences(
        user_id=3,
        username="charlie",
        settings={},  # Empty map
        scores={"level1": 88, "level2": 90},
        metadata={}
    )
]

# Query using map operations
test_case = TestCase(
    query="""
        SELECT
            username,
            settings['theme'] as theme_preference,
            settings['notifications'] as notifications_enabled,
            MAP_KEYS(scores) as completed_levels,
            MAP_VALUES(scores) as level_scores,
            CARDINALITY(settings) as settings_count,
            CASE
                WHEN metadata IS NULL THEN 'No metadata'
                WHEN CARDINALITY(metadata) = 0 THEN 'Empty metadata'
                ELSE metadata['source']
            END as data_source
        FROM user_preferences
        WHERE settings['theme'] IS NOT NULL
        ORDER BY username
    """,
    default_namespace="analytics"
)

# BigQuery-specific map query (stored as JSON)
bigquery_test_case = TestCase(
    query="""
        SELECT
            username,
            JSON_EXTRACT_SCALAR(settings, '$.theme') as theme_preference,
            JSON_EXTRACT_SCALAR(settings, '$.notifications') as notifications_enabled,
            JSON_EXTRACT_SCALAR(metadata, '$.source') as data_source,
            CASE
                WHEN metadata IS NULL THEN 'No metadata'
                WHEN metadata = '{}' THEN 'Empty metadata'
                ELSE 'Has metadata'
            END as metadata_status
        FROM user_preferences
        WHERE JSON_EXTRACT_SCALAR(settings, '$.theme') IS NOT NULL
        ORDER BY username
    """,
    default_namespace="my-project.analytics"
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

### Working with Structs (Athena/Trino/BigQuery)

Struct types are supported for Athena, Trino, and BigQuery adapters using dataclasses or Pydantic models:

```python
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal

@dataclass
class Address:
    street: str
    city: str
    state: str
    zip_code: str

@dataclass
class Employee:
    employee_id: int
    name: str
    salary: Decimal
    address: Address  # Nested struct
    is_active: bool

class EmployeesMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "hr_db"

    def get_table_name(self) -> str:
        return "employees"

class EmployeeLocationResult(BaseModel):
    name: str
    city: str
    state: str
    salary: Decimal

# Mock data with structs
mock_data = [
    Employee(
        employee_id=1,
        name="Alice Johnson",
        salary=Decimal("120000.50"),
        address=Address(
            street="123 Main St",
            city="New York",
            state="NY",
            zip_code="10001"
        ),
        is_active=True
    ),
    Employee(
        employee_id=2,
        name="Bob Smith",
        salary=Decimal("95000.00"),
        address=Address(
            street="456 Oak Ave",
            city="San Francisco",
            state="CA",
            zip_code="94102"
        ),
        is_active=True
    ),
    Employee(
        employee_id=3,
        name="Charlie Brown",
        salary=Decimal("105000.75"),
        address=Address(
            street="789 Pine Rd",
            city="Seattle",
            state="WA",
            zip_code="98101"
        ),
        is_active=False
    )
]

@sql_test(
    adapter_type="athena",  # or "trino" or "bigquery"
    mock_tables=[EmployeesMockTable(mock_data)],
    result_class=EmployeeLocationResult
)
def test_employee_locations():
    return TestCase(
        query="""
            SELECT
                name,
                address.city as city,
                address.state as state,
                salary
            FROM employees
            WHERE is_active = true
                AND address.state IN ('NY', 'CA')
            ORDER BY name
        """,
        default_namespace="hr_db"
    )

# Test with WHERE clause on struct fields
@sql_test(
    adapter_type="trino",
    mock_tables=[EmployeesMockTable(mock_data)],
    result_class=dict
)
def test_employees_by_city():
    return TestCase(
        query="""
            SELECT
                name,
                address.street as street_address,
                address.city || ', ' || address.state || ' ' || address.zip_code as full_address
            FROM employees
            WHERE address.city = 'New York'
        """,
        default_namespace="hr_db"
    )

# Using Pydantic models for structs
from pydantic import BaseModel

class ContactInfo(BaseModel):
    email: str
    phone: str
    preferred_contact: str

class Customer(BaseModel):
    customer_id: int
    name: str
    contact: ContactInfo
    total_purchases: Decimal

# Query with nested struct access
test_case = TestCase(
    query="""
        SELECT
            name,
            contact.email as email,
            contact.preferred_contact as contact_method,
            total_purchases
        FROM customers
        WHERE contact.preferred_contact = 'email'
            AND total_purchases > 1000
        ORDER BY total_purchases DESC
    """,
    default_namespace="sales_db"
)

# Working with lists of structs
@dataclass
class OrderItem:
    product_id: int
    quantity: int
    unit_price: Decimal

@dataclass
class Order:
    order_id: int
    customer_id: int
    items: List[OrderItem]  # List of structs
    order_date: date

# Mock data with list of structs
orders_data = [
    Order(
        order_id=1,
        customer_id=100,
        items=[
            OrderItem(product_id=1, quantity=2, unit_price=Decimal("29.99")),
            OrderItem(product_id=2, quantity=1, unit_price=Decimal("49.99")),
            OrderItem(product_id=3, quantity=3, unit_price=Decimal("19.99"))
        ],
        order_date=date(2024, 1, 15)
    ),
    Order(
        order_id=2,
        customer_id=101,
        items=[
            OrderItem(product_id=2, quantity=5, unit_price=Decimal("49.99"))
        ],
        order_date=date(2024, 1, 16)
    )
]

# Query using list of structs
@sql_test(
    adapter_type="athena",  # or "trino" or "bigquery"
    mock_tables=[OrdersMockTable(orders_data)],
    result_class=dict
)
def test_order_items_analysis():
    return TestCase(
        query="""
            SELECT
                order_id,
                customer_id,
                CARDINALITY(items) as num_items,
                -- Access first item details
                items[1].product_id as first_product_id,
                items[1].quantity as first_product_qty,
                -- Calculate total using array operations
                REDUCE(
                    items,
                    0.0,
                    (total, item) -> total + (item.quantity * item.unit_price),
                    total -> total
                ) as order_total
            FROM orders
            WHERE CARDINALITY(items) > 0
            ORDER BY order_id
        """,
        default_namespace="test_db"
    )
```

### BigQuery-Specific Features

#### BigQueryMockTable for Three-Part Naming

BigQuery uses a three-part naming scheme (`project.dataset.table`). The `BigQueryMockTable` class makes this explicit:

```python
from sql_testing_library import BigQueryMockTable
from dataclasses import dataclass

@dataclass
class User:
    user_id: int
    name: str
    email: str

# Clean three-part naming with class variables
class UsersMockTable(BigQueryMockTable):
    bigquery_project = "my-project"
    bigquery_dataset = "analytics"
    bigquery_table = "users"

class OrdersMockTable(BigQueryMockTable):
    bigquery_project = "my-project"
    bigquery_dataset = "analytics"
    bigquery_table = "orders"

# Use in tests
@sql_test(
    adapter_type="bigquery",
    mock_tables=[
        UsersMockTable([
            User(1, "Alice", "alice@example.com"),
            User(2, "Bob", "bob@example.com")
        ])
    ],
    result_class=User
)
def test_premium_users():
    return TestCase(
        query="""
            SELECT user_id, name, email
            FROM `my-project.analytics.users`
            WHERE user_id = 1
        """,
        default_namespace="analytics"
    )
```

**Avoid Repetition with Inheritance:**

```python
# Base class with shared project
class MyAnalyticsTable(BigQueryMockTable):
    bigquery_project = "my-project"
    bigquery_dataset = "analytics"

# Subclasses just set table name
class UsersTable(MyAnalyticsTable):
    bigquery_table = "users"

class OrdersTable(MyAnalyticsTable):
    bigquery_table = "orders"
```

#### BigQuery Struct Support

BigQuery now supports struct types with dataclasses and Pydantic models:

```python
# BigQuery struct support (NEW!)
@sql_test(
    adapter_type="bigquery",
    mock_tables=[EmployeesMockTable(mock_data)],  # Same mock data as above
    result_class=EmployeeLocationResult
)
def test_bigquery_struct_access():
    return TestCase(
        query="""
            SELECT
                name,
                address.city as city,
                address.state as state,
                salary
            FROM employees
            WHERE is_active = true
                AND address.state IN ('NY', 'CA')
            ORDER BY name
        """,
        default_namespace="my-project.my_dataset"
    )

# BigQuery with list of structs and array operations
@sql_test(
    adapter_type="bigquery",
    mock_tables=[OrdersMockTable(orders_data)],  # Same list of structs data
    result_class=dict
)
def test_bigquery_struct_arrays():
    return TestCase(
        query="""
            SELECT
                order_id,
                customer_id,
                ARRAY_LENGTH(items) as num_items,
                -- Access first item details (0-based indexing in BigQuery)
                items[OFFSET(0)].product_id as first_product_id,
                items[OFFSET(0)].quantity as first_product_qty,
                -- Calculate total using array aggregation
                (SELECT SUM(item.quantity * item.unit_price)
                 FROM UNNEST(items) as item) as order_total
            FROM orders
            WHERE ARRAY_LENGTH(items) > 0
            ORDER BY order_id
        """,
        default_namespace="my-project.my_dataset"
    )
```

Using BigQuery SQL features in your queries:

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

### Redshift JSON and Map Handling

Working with JSON and map types in Redshift:

```python
# Redshift adapter test with maps (using SUPER type)
@dataclass
class UserActivity:
    user_id: int
    username: str
    preferences: Dict[str, str]  # Stored as SUPER type
    activity_scores: Dict[str, int]

# Mock data with maps
mock_data = [
    UserActivity(
        user_id=1,
        username="alice",
        preferences={"theme": "dark", "language": "en"},
        activity_scores={"login": 100, "posts": 45}
    )
]

@sql_test(
    adapter_type="redshift",
    mock_tables=[UserActivityMockTable(mock_data)],
    result_class=dict
)
def test_redshift_maps():
    return TestCase(
        query="""
            SELECT
                username,
                preferences.theme as theme_pref,
                preferences.language as lang_pref,
                activity_scores.login as login_score
            FROM user_activity
            WHERE preferences.theme = 'dark'
        """,
        default_namespace="analytics_db"
    )

# Traditional JSON handling in Redshift
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

### DuckDB-Specific Features

DuckDB provides excellent support for complex data types and high-performance analytics:

```python
from dataclasses import dataclass
from typing import List, Dict
from decimal import Decimal

@dataclass
class UserActivity:
    user_id: int
    name: str
    scores: List[int]
    metadata: Dict[str, str]
    tags: List[str]

@dataclass
class Address:
    street: str
    city: str
    zip_code: str

@dataclass
class UserProfile:
    user_id: int
    name: str
    address: Address
    activity_scores: List[int]
    preferences: Dict[str, str]

class UserActivityMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "analytics_db"

    def get_table_name(self) -> str:
        return "user_activity"

class UserProfileMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "analytics_db"

    def get_table_name(self) -> str:
        return "user_profiles"

class DuckDBAnalyticsResult(BaseModel):
    user_id: int
    name: str
    avg_score: float
    city: str
    high_scorer: bool
    tag_count: int

# DuckDB Array Operations
@sql_test(
    adapter_type="duckdb",
    mock_tables=[
        UserActivityMockTable([
            UserActivity(1, "Alice", [85, 90, 78], {"role": "admin"}, ["premium", "active"]),
            UserActivity(2, "Bob", [75, 88, 91], {"role": "user"}, ["active"]),
            UserActivity(3, "Charlie", [92, 89, 95], {"role": "admin"}, ["premium", "vip"])
        ])
    ],
    result_class=DuckDBAnalyticsResult
)
def test_duckdb_array_operations():
    return TestCase(
        query="""
            SELECT
                user_id,
                name,
                list_avg(scores) as avg_score,
                'Unknown' as city,
                list_avg(scores) > 85 as high_scorer,
                len(tags) as tag_count
            FROM user_activity
            WHERE list_contains(tags, 'active')
                AND metadata['role'] = 'admin'
            ORDER BY avg_score DESC
        """,
        default_namespace="analytics_db"
    )

# DuckDB Struct Operations
@sql_test(
    adapter_type="duckdb",
    mock_tables=[
        UserProfileMockTable([
            UserProfile(1, "Alice", Address("123 Main St", "NYC", "10001"),
                       [85, 90, 78], {"theme": "dark", "notifications": "enabled"}),
            UserProfile(2, "Bob", Address("456 Oak Ave", "SF", "94105"),
                       [75, 88, 91], {"theme": "light", "notifications": "disabled"})
        ])
    ],
    result_class=dict
)
def test_duckdb_struct_operations():
    return TestCase(
        query="""
            SELECT
                user_id,
                name,
                address.city,
                address.zip_code,
                list_avg(activity_scores) as avg_score,
                preferences['theme'] as theme_preference
            FROM user_profiles
            WHERE address.city = 'NYC'
                AND preferences['notifications'] = 'enabled'
        """,
        default_namespace="analytics_db"
    )

# DuckDB Map Operations
@sql_test(
    adapter_type="duckdb",
    mock_tables=[
        UserActivityMockTable([
            UserActivity(1, "Alice", [85, 90], {"role": "admin", "team": "data"}, ["premium"]),
            UserActivity(2, "Bob", [75, 88], {"role": "user", "team": "eng"}, ["basic"])
        ])
    ],
    result_class=dict
)
def test_duckdb_map_operations():
    return TestCase(
        query="""
            SELECT
                user_id,
                name,
                metadata['role'] as user_role,
                metadata['team'] as user_team,
                map_keys(metadata) as available_keys,
                len(metadata) as metadata_count
            FROM user_activity
            WHERE metadata['role'] IN ('admin', 'user')
        """,
        default_namespace="analytics_db"
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
