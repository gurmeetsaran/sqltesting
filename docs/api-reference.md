---
layout: default
title: API Reference
nav_order: 4
---

# API Reference
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Core Classes

### SQLTestCase

A dataclass that represents a SQL test case configuration.

```python
from sql_testing_library import SQLTestCase, TestCase  # TestCase is an alias

@dataclass
class SQLTestCase(Generic[T]):
    query: str
    default_namespace: Optional[str] = None
    mock_tables: Optional[List[BaseMockTable]] = None
    result_class: Optional[Type[T]] = None
    use_physical_tables: bool = False
    description: Optional[str] = None
    adapter_type: Optional[AdapterType] = None
    log_sql: Optional[bool] = None
    execution_database: Optional[str] = None  # Deprecated
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `str` | The SQL query to test |
| `default_namespace` | `Optional[str]` | Database/schema context for unqualified table names |
| `mock_tables` | `Optional[List[BaseMockTable]]` | Mock tables with test data |
| `result_class` | `Optional[Type[T]]` | Class for deserializing results (dataclass/Pydantic) |
| `use_physical_tables` | `bool` | Force physical tables instead of CTEs (default: False) |
| `description` | `Optional[str]` | Optional test description |
| `adapter_type` | `Optional[AdapterType]` | Override default database adapter |
| `log_sql` | `Optional[bool]` | Enable/disable SQL logging for this test |

#### Example

```python
test_case = TestCase(
    query="SELECT * FROM users WHERE active = true",
    default_namespace="my_dataset",
    mock_tables=[users_mock],
    result_class=UserResult
)
```

### BaseMockTable

Abstract base class for creating mock tables with test data.

```python
from sql_testing_library.mock_table import BaseMockTable

class BaseMockTable(ABC):
    def __init__(self, data: Optional[List[Any]] = None):
        self.data = data or []

    @abstractmethod
    def get_database_name(self) -> str:
        """Return the database/schema name"""
        pass

    @abstractmethod
    def get_table_name(self) -> str:
        """Return the table name"""
        pass
```

#### Abstract Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `get_database_name()` | `str` | Database/schema name for the mock table |
| `get_table_name()` | `str` | Table name |

#### Provided Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `get_qualified_name()` | `str` | Fully qualified table name |
| `get_column_types()` | `Dict[str, Type]` | Infer column types from data |
| `to_dataframe()` | `pd.DataFrame` | Convert to pandas DataFrame |
| `get_cte_alias()` | `str` | CTE alias for query generation |

#### Example Implementation

```python
@dataclass
class Product:
    id: int
    name: str
    price: Decimal
    tags: List[str]

class ProductsMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "ecommerce.public"

    def get_table_name(self) -> str:
        return "products"

# Usage
products = ProductsMockTable([
    Product(1, "Laptop", Decimal("999.99"), ["electronics", "computers"]),
    Product(2, "Mouse", Decimal("29.99"), ["electronics", "accessories"])
])
```

### SQLTestFramework

The main framework class for executing SQL tests.

```python
from sql_testing_library import SQLTestFramework

class SQLTestFramework:
    def __init__(self, adapter: DatabaseAdapter):
        """Initialize with a database adapter"""
        self.adapter = adapter

    def run_test(self, test_case: SQLTestCase[T]) -> List[T]:
        """Execute test case and return typed results"""
```

#### Methods

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `__init__` | `adapter: DatabaseAdapter` | None | Initialize framework |
| `run_test` | `test_case: SQLTestCase[T]` | `List[T]` | Execute test and return results |

#### Example

```python
from sql_testing_library.adapters import BigQueryAdapter

# Create framework
adapter = BigQueryAdapter(project_id="my-project", dataset_id="test")
framework = SQLTestFramework(adapter)

# Run test
results = framework.run_test(test_case)
```

## Decorators

### @sql_test

The main decorator for pytest integration. This decorator automatically adds a `sql_test` pytest marker to your test functions.

```python
from sql_testing_library import sql_test

@sql_test(
    mock_tables: Optional[List[BaseMockTable]] = None,
    result_class: Optional[Type[T]] = None,
    use_physical_tables: Optional[bool] = None,
    adapter_type: Optional[AdapterType] = None,
    log_sql: Optional[bool] = None
)
```

#### Pytest Marker

Tests decorated with `@sql_test` are automatically marked with the `sql_test` pytest marker, allowing you to:

```bash
# Run only SQL tests
pytest -m sql_test

# Exclude SQL tests
pytest -m "not sql_test"

# Combine with other markers
pytest -m "sql_test and not slow"
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `mock_tables` | `Optional[List[BaseMockTable]]` | Override mock tables |
| `result_class` | `Optional[Type[T]]` | Override result class |
| `use_physical_tables` | `Optional[bool]` | Override physical tables flag |
| `adapter_type` | `Optional[AdapterType]` | Override adapter type |
| `log_sql` | `Optional[bool]` | Enable/disable SQL logging |

#### Usage Patterns

```python
# Pattern 1: All config in decorator
@sql_test(
    mock_tables=[users_mock],
    result_class=UserResult
)
def test_users():
    return TestCase(
        query="SELECT * FROM users",
        default_namespace="test_db"
    )

# Pattern 2: All config in TestCase
@sql_test()
def test_users():
    return TestCase(
        query="SELECT * FROM users",
        default_namespace="test_db",
        mock_tables=[users_mock],
        result_class=UserResult
    )

# Pattern 3: Override adapter
@sql_test(adapter_type="bigquery")
def test_bigquery_specific():
    return TestCase(...)
```

## Database Adapters

### DatabaseAdapter (Abstract Base)

Base class for all database adapters.

```python
from sql_testing_library.adapters import DatabaseAdapter

class DatabaseAdapter(ABC):
    @abstractmethod
    def get_sqlglot_dialect(self) -> str:
        """Return sqlglot dialect name"""

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results"""

    @abstractmethod
    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create temporary table from mock data"""

    @abstractmethod
    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Clean up temporary tables"""

    @abstractmethod
    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for CTE generation"""
```

### Concrete Adapters

#### BigQueryAdapter

```python
from sql_testing_library.adapters import BigQueryAdapter

adapter = BigQueryAdapter(
    project_id: str,
    dataset_id: str,
    credentials_path: Optional[str] = None,
    client: Optional[bigquery.Client] = None
)
```

#### AthenaAdapter

```python
from sql_testing_library.adapters import AthenaAdapter

adapter = AthenaAdapter(
    database: str,
    s3_output_location: str,
    region: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None
)
```

#### RedshiftAdapter

```python
from sql_testing_library.adapters import RedshiftAdapter

adapter = RedshiftAdapter(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 5439
)
```

#### TrinoAdapter

```python
from sql_testing_library.adapters import TrinoAdapter

adapter = TrinoAdapter(
    host: str,
    port: int = 8080,
    user: str,
    catalog: str = "memory",
    schema: str = "default",
    http_scheme: str = "http",
    auth: Optional[Authentication] = None
)
```

#### SnowflakeAdapter

```python
from sql_testing_library.adapters import SnowflakeAdapter

adapter = SnowflakeAdapter(
    account: str,
    user: str,
    password: str,
    database: str,
    schema: str = "PUBLIC",
    warehouse: str,
    role: Optional[str] = None
)
```

## Exceptions

All exceptions inherit from `SQLTestingError`.

### Exception Hierarchy

```python
SQLTestingError
├── MockTableNotFoundError
├── SQLParseError
├── QuerySizeLimitExceeded
└── TypeConversionError
```

### MockTableNotFoundError

Raised when a required mock table is not provided.

```python
from sql_testing_library.exceptions import MockTableNotFoundError

try:
    framework.run_test(test_case)
except MockTableNotFoundError as e:
    print(f"Missing mock table: {e}")
```

### SQLParseError

Raised when SQL parsing fails.

```python
from sql_testing_library.exceptions import SQLParseError

try:
    framework.run_test(test_case)
except SQLParseError as e:
    print(f"Invalid SQL: {e}")
```

### QuerySizeLimitExceeded

Raised when CTE query exceeds database size limits.

```python
from sql_testing_library.exceptions import QuerySizeLimitExceeded

# Library automatically falls back to physical tables
# or you can handle manually:
try:
    framework.run_test(test_case)
except QuerySizeLimitExceeded:
    test_case.use_physical_tables = True
    framework.run_test(test_case)
```

### TypeConversionError

Raised during result deserialization.

```python
from sql_testing_library.exceptions import TypeConversionError

try:
    results = framework.run_test(test_case)
except TypeConversionError as e:
    print(f"Type mismatch: {e}")
```

## Type System

### Supported Python Types

| Python Type | SQL Type Support | Notes |
|-------------|------------------|-------|
| `str` | VARCHAR/STRING | Universal support |
| `int` | INTEGER/BIGINT | 64-bit integers |
| `float` | FLOAT/DOUBLE | Double precision |
| `bool` | BOOLEAN | True/False |
| `date` | DATE | From datetime module |
| `datetime` | TIMESTAMP | With timezone support |
| `Decimal` | DECIMAL/NUMERIC | Arbitrary precision |
| `None` | NULL | Null values |
| `List[T]` | ARRAY | Arrays of supported types |
| `Dict[K, V]` | MAP | Maps (Athena/Trino only) |
| `Optional[T]` | Nullable | Union[T, None] |
| `dataclass` | STRUCT/ROW | Structs (Athena/Trino only) |
| `Pydantic model` | STRUCT/ROW | Structs (Athena/Trino only) |

### Type Conversion

The library automatically handles type conversions between Python and SQL:

```python
from decimal import Decimal
from datetime import date, datetime
from typing import List, Dict, Optional

@dataclass
class ComplexData:
    id: int
    amount: Decimal
    created_at: datetime
    tags: List[str]
    notes: Optional[str]

# Map types (Athena/Trino only)
@dataclass
class MapData:
    user_id: int
    preferences: Dict[str, str]      # MAP(VARCHAR, VARCHAR)
    scores: Dict[str, int]           # MAP(VARCHAR, INTEGER/BIGINT)
    attributes: Dict[int, str]       # MAP(INTEGER/BIGINT, VARCHAR)
    optional_map: Optional[Dict[str, str]]  # Nullable MAP

# Struct types (Athena/Trino only)
@dataclass
class Address:
    street: str
    city: str
    zip_code: str

@dataclass
class Person:
    name: str
    age: int
    address: Address  # Nested struct

# Pydantic models also work
from pydantic import BaseModel

class AddressPydantic(BaseModel):
    street: str
    city: str
    zip_code: str

class PersonPydantic(BaseModel):
    name: str
    age: int
    address: AddressPydantic
```

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SQL_TESTING_PROJECT_ROOT` | Override config file search path | `/path/to/project` |

### Configuration Files

The library searches for configuration in this order:
1. `pytest.ini`
2. `setup.cfg`
3. `tox.ini`

### Configuration Sections

```ini
[sql_testing]
adapter = bigquery  # Default adapter

[sql_testing.bigquery]
# BigQuery-specific settings

[sql_testing.athena]
# Athena-specific settings

# ... other adapters
```

## Advanced Usage

### Custom Type Converters

Extend type conversion for custom types:

```python
class MyCustomType:
    def __init__(self, value: str):
        self.value = value

    def to_sql_value(self) -> str:
        return f"'{self.value}'"
```

### Dynamic Mock Tables

Generate mock data programmatically:

```python
class DynamicMockTable(BaseMockTable):
    def __init__(self, row_count: int):
        data = [
            {"id": i, "value": f"test_{i}"}
            for i in range(row_count)
        ]
        super().__init__(data)
```

### Testing CTEs

Test individual CTEs in complex queries:

```python
@sql_test(mock_tables=[base_table])
def test_cte_logic():
    return TestCase(
        query="""
        WITH aggregated AS (
            SELECT category, SUM(amount) as total
            FROM transactions
            GROUP BY category
        )
        SELECT * FROM aggregated WHERE total > 100
        """,
        default_namespace="test_db",
        result_class=CategoryTotal
    )
```

## Best Practices

1. **Use Type Hints**: Always specify result_class for type safety
2. **Mock Realistically**: Use production-like data structures
3. **Test Edge Cases**: Include nulls, empty arrays, boundary values
4. **Organize Tests**: Group related tests in classes
5. **Document Complex Queries**: Add descriptions to test cases
6. **Version Control**: Track schema changes in mock tables
