# Using SQL Testing Library with Mocksmith

[Mocksmith](https://github.com/gurmeetsaran/mocksmith) is a complementary library that provides automatic mock data generation for Python classes. When combined with SQL Testing Library, it dramatically reduces the boilerplate code needed to create test data.

## Installation

```bash
# Install both libraries with required extras
pip install sql-testing-library
pip install mocksmith[mock,pydantic]

# Or as an optional dependency (coming soon)
pip install sql-testing-library[mocksmith]
```

## Quick Comparison

### Without Mocksmith (Traditional Approach)
```python
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import random

@dataclass
class Customer:
    id: int
    name: str
    email: str
    credit_limit: Decimal
    created_date: date
    is_active: bool

# Manually create test data
customers = []
for i in range(100):
    customers.append(Customer(
        id=i + 1,
        name=f"Customer {i + 1}",  # Not realistic
        email=f"customer{i + 1}@example.com",  # Generic
        credit_limit=Decimal(str(round(random.uniform(1000, 10000), 2))),
        created_date=date.today() - timedelta(days=random.randint(1, 365)),
        is_active=random.choice([True, False])
    ))
```

### With Mocksmith
```python
from mocksmith import mockable, Varchar, Integer, Money, Date, Boolean

@mockable
@dataclass
class Customer:
    id: Integer()
    name: Varchar(100)  # Generates realistic names
    email: Varchar(255)  # Generates valid emails
    credit_limit: Money(min_value="1000", max_value="10000")
    created_date: Date()
    is_active: Boolean()

# Generate realistic test data in one line!
customers = [Customer.mock() for _ in range(100)]
```

### Varchar Constraints (Mocksmith 3.0.1+)

Mocksmith now supports `startswith` and `endswith` constraints for Varchar fields:

```python
from mocksmith import mockable, Varchar, Integer

@mockable
@dataclass
class User:
    user_id: Varchar(50, startswith="user_")  # e.g., "user_12345"
    email: Varchar(100, endswith="@company.com")  # e.g., "john.doe@company.com"

@mockable
@dataclass
class Product:
    sku: Varchar(20, startswith="PROD-", endswith="-US")  # e.g., "PROD-ABC123-US"
    barcode: Varchar(13, startswith="978")  # e.g., "9781234567890"
    serial_number: Varchar(20, startswith="SN-")  # e.g., "SN-2024-ABC-123"

# Generate data with constraints
users = [User.mock() for _ in range(10)]
products = [Product.mock() for _ in range(50)]
```

## Real-World Testing Example

Here's a complete example testing a customer segmentation query:

```python
from mocksmith import mockable, Varchar, Integer, Money, Date
from sql_testing_library import sql_test, TestCase, BaseMockTable
from pydantic import BaseModel

# Define your data model with Mocksmith
@mockable
@dataclass
class Order:
    order_id: Integer()
    customer_id: Integer()
    order_date: Date()
    total_amount: Money()
    status: Varchar(20)

# Create mock table
class OrdersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "sales_db"

    def get_table_name(self) -> str:
        return "orders"

# Define expected result
class CustomerSegmentResult(BaseModel):
    customer_id: int
    total_spent: Decimal
    order_count: int
    customer_segment: str

# Write your test
@sql_test(
    mock_tables=[
        OrdersMockTable([
            Order.mock(
                customer_id=i % 20 + 1,  # 20 customers
                status="completed" if random.random() > 0.1 else "cancelled"
            ) for i in range(200)  # 200 orders
        ])
    ],
    result_class=CustomerSegmentResult
)
def test_customer_segmentation():
    return TestCase(
        query="""
            SELECT
                customer_id,
                SUM(total_amount) as total_spent,
                COUNT(*) as order_count,
                CASE
                    WHEN SUM(total_amount) > 5000 THEN 'VIP'
                    WHEN SUM(total_amount) > 1000 THEN 'Regular'
                    ELSE 'Basic'
                END as customer_segment
            FROM orders
            WHERE status = 'completed'
            GROUP BY customer_id
            HAVING COUNT(*) > 0
            ORDER BY total_spent DESC
        """,
        default_namespace="sales_db"
    )

# Run the test
results = test_customer_segmentation()

# Mocksmith generated realistic data:
# - Real customer names
# - Valid monetary values
# - Realistic date distributions
# - Proper status values
```

## Advanced Patterns

### 1. Testing Edge Cases
```python
# Create specific test scenarios
edge_case_orders = [
    # High-value orders
    Order.mock(total_amount=Decimal("10000"), status="completed"),
    # Zero-value orders
    Order.mock(total_amount=Decimal("0"), status="completed"),
    # Cancelled orders
    Order.mock(status="cancelled"),
]
```

### 2. Time-Series Testing
```python
from datetime import datetime, timedelta

# Generate orders for specific time periods
black_friday_orders = [
    Order.mock(
        order_date=datetime(2023, 11, 24).date(),
        total_amount=Decimal(str(random.uniform(100, 1000)))
    ) for _ in range(100)
]
```

### 3. Hierarchical Data
```python
@mockable
@dataclass
class Employee:
    id: Integer()
    name: Varchar(100)
    manager_id: Optional[Integer()]  # Self-referential
    department: Varchar(50)
    salary: Money()

# Create organizational hierarchy
executives = [Employee.mock(manager_id=None) for _ in range(5)]
managers = [Employee.mock(manager_id=random.choice([e.id for e in executives])) for _ in range(20)]
employees = [Employee.mock(manager_id=random.choice([m.id for m in managers])) for _ in range(100)]
```

## Benefits

1. **Less Code**: ~70% reduction in test data setup code
2. **Realistic Data**: Automatic generation of realistic names, emails, dates, etc.
3. **Type Safety**: SQL-like type annotations ensure data matches database schemas
4. **Flexible**: Easy to override specific fields for test scenarios
5. **Maintainable**: Changes to data models automatically reflected in tests

## Best Practices

1. **Use Mocksmith for bulk data generation**, manual creation for specific edge cases
2. **Leverage type constraints** (min/max values, string lengths) to match database constraints
3. **Create reusable factory functions** for common test scenarios
4. **Combine with SQL Testing Library's features** like CTEs and physical tables modes

## Resources

- [Mocksmith Documentation](https://github.com/gurmeetsaran/mocksmith)
- [SQL Testing Library Documentation](https://gurmeetsaran.github.io/sqltesting/)
- [Complete Examples](https://github.com/gurmeetsaran/sqltesting/tree/master/examples)
