# SQL Testing Library

A powerful Python framework for unit testing SQL queries with mock data injection across BigQuery, Snowflake, Redshift, Athena, and Trino.

[![Unit Tests](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml)
[![Athena Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/athena-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/athena-integration.yml)
[![BigQuery Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/bigquery-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/bigquery-integration.yml)
[![Redshift Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/redshift-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/redshift-integration.yml)
[![Trino Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/trino-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/trino-integration.yml)
[![Snowflake Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/snowflake-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/snowflake-integration.yml)
[![GitHub license](https://img.shields.io/github/license/gurmeetsaran/sqltesting)](https://github.com/gurmeetsaran/sqltesting/blob/master/LICENSE)
[![Pepy Total Downloads](https://img.shields.io/pepy/dt/sql-testing-library?label=PyPI%20Downloads)](https://pepy.tech/projects/sql-testing-library)
[![codecov](https://codecov.io/gh/gurmeetsaran/sqltesting/branch/master/graph/badge.svg?token=CN3G5X5ZA5)](https://codecov.io/gh/gurmeetsaran/sqltesting)
![python version](https://img.shields.io/badge/python-3.9%2B-yellowgreen)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://gurmeetsaran.github.io/sqltesting/)

## 🎯 Motivation

SQL testing in data engineering can be challenging, especially when working with large datasets and complex queries across multiple database platforms. This library was born from real-world production needs at scale, addressing the pain points of:

- **Fragile Integration Tests**: Traditional tests that depend on live data break when data changes
- **Slow Feedback Loops**: Running tests against full datasets takes too long for CI/CD
- **Database Engine Upgrades**: UDF semantics and SQL behavior change between database versions, causing silent production failures
- **Database Lock-in**: Tests written for one database don't work on another
- **Complex Setup**: Each database requires different mocking strategies and tooling

For more details on our journey and the engineering challenges we solved, read the full story: [**"Our Journey to Building a Scalable SQL Testing Library for Athena"**](https://eng.wealthfront.com/2025/04/07/our-journey-to-building-a-scalable-sql-testing-library-for-athena/)

## 🚀 Key Use Cases

### Data Engineering Teams
- **ETL Pipeline Testing**: Validate data transformations with controlled input data
- **Data Quality Assurance**: Test data validation rules and business logic in SQL
- **Schema Migration Testing**: Ensure queries work correctly after schema changes
- **Database Engine Upgrades**: Catch breaking changes in SQL UDF semantics across database versions before they hit production
- **Cross-Database Compatibility**: Write tests once, run on multiple database platforms

### Analytics Teams
- **Report Validation**: Test analytical queries with known datasets to verify results
- **A/B Test Analysis**: Validate statistical calculations and business metrics
- **Dashboard Backend Testing**: Ensure dashboard queries return expected data structures

### DevOps & CI/CD
- **Fast Feedback**: Run comprehensive SQL tests in seconds, not minutes
- **Isolated Testing**: Tests don't interfere with production data or other tests
- **Cost Optimization**: Reduce cloud database costs by avoiding large dataset queries in tests

## Features

- **Multi-Database Support**: Test SQL across BigQuery, Athena, Redshift, Trino, and Snowflake
- **Mock Data Injection**: Use Python dataclasses for type-safe test data
- **CTE or Physical Tables**: Automatic fallback for query size limits
- **Type-Safe Results**: Deserialize results to Pydantic models
- **Pytest Integration**: Seamless testing with `@sql_test` decorator
- **SQL Logging**: Comprehensive SQL logging with formatted output, error traces, and temp table queries

## Data Types Support

The library supports different data types across database engines. All checkmarks indicate comprehensive test coverage with verified functionality.

### Primitive Types

| Data Type | Python Type | BigQuery | Athena | Redshift | Trino | Snowflake |
|-----------|-------------|----------|--------|----------|-------|-----------|
| **String** | `str` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Integer** | `int` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Float** | `float` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Boolean** | `bool` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Date** | `date` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Datetime** | `datetime` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Decimal** | `Decimal` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Optional** | `Optional[T]` | ✅ | ✅ | ✅ | ✅ | ✅ |

### Complex Types

| Data Type | Python Type | BigQuery | Athena | Redshift | Trino | Snowflake |
|-----------|-------------|----------|--------|----------|-------|-----------|
| **String Array** | `List[str]` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Integer Array** | `List[int]` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Decimal Array** | `List[Decimal]` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Optional Array** | `Optional[List[T]]` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Map/Dict** | `Dict[K, V]` | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Struct/Record** | `dataclass` | ❌ | ✅ | ❌ | ✅ | ❌ |
| **Nested Arrays** | `List[List[T]]` | ❌ | ❌ | ❌ | ❌ | ❌ |

### Database-Specific Notes

- **BigQuery**: NULL arrays become empty arrays `[]`; uses scientific notation for large decimals; dict/map types stored as JSON strings; struct types not yet supported
- **Athena**: 256KB query size limit; supports arrays and maps using `ARRAY[]` and `MAP(ARRAY[], ARRAY[])` syntax; supports struct types using `ROW` with named fields (dataclasses and Pydantic models)
- **Redshift**: Arrays and maps implemented via SUPER type (JSON parsing); 16MB query size limit; struct types not yet supported
- **Trino**: Memory catalog for testing; excellent decimal precision; supports arrays, maps, and struct types using `ROW` with named fields (dataclasses and Pydantic models)
- **Snowflake**: Column names normalized to lowercase; 1MB query size limit; dict/map types implemented via VARIANT type (JSON parsing); struct types not yet supported

## Execution Modes Support

The library supports two execution modes for mock data injection. **CTE Mode is the default** and is automatically used unless Physical Tables mode is explicitly requested or required due to query size limits.

| Execution Mode | Description | BigQuery | Athena | Redshift | Trino | Snowflake |
|----------------|-------------|----------|--------|----------|-------|-----------|
| **CTE Mode** | Mock data injected as Common Table Expressions | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Physical Tables** | Mock data created as temporary tables | ✅ | ✅ | ✅ | ✅ | ✅ |

### Execution Mode Details

#### **CTE Mode (Default)**
- **Default Behavior**: Used automatically for all tests unless overridden
- **Data Injection**: Mock data is injected as Common Table Expressions (CTEs) within the SQL query
- **No Physical Objects**: No actual tables are created in the database
- **Memory-Based**: All data exists only for the duration of the query execution
- **Compatibility**: Works with all database engines
- **Query Size**: Subject to database-specific query size limits (see table below)

#### **Physical Tables Mode**
- **When Used**: Automatically activated when CTE queries exceed size limits, or explicitly requested with `use_physical_tables=True`
- **Table Creation**: Creates actual temporary tables in the database with mock data
- **Table Types by Database**:

| Database | Table Type | Schema/Location | Cleanup Method | Cleanup Timing |
|----------|------------|-----------------|----------------|-----------------|
| **BigQuery** | Standard tables | Project dataset | Library executes `client.delete_table()` | After each test |
| **Athena** | External tables | S3-backed external tables | Library executes `DROP TABLE` (⚠️ S3 data remains) | After each test |
| **Redshift** | Temporary tables | Session-specific temp schema | Database automatic | Session end |
| **Trino** | Memory tables | `memory.default` schema | Library executes `DROP TABLE` | After each test |
| **Snowflake** | Temporary tables | Session-specific temp schema | Database automatic | Session end |

#### **Cleanup Behavior Explained**

**Library-Managed Cleanup (BigQuery, Athena, Trino):**
- The SQL Testing Library explicitly calls cleanup methods after each test
- **BigQuery**: Creates standard tables in your dataset, then deletes them via `client.delete_table()`
- **Athena**: Creates external tables backed by S3 data, then drops table metadata via `DROP TABLE IF EXISTS` (⚠️ **S3 data files remain and require separate cleanup**)
- **Trino**: Creates tables in memory catalog, then drops them via `DROP TABLE IF EXISTS`

**Database-Managed Cleanup (Redshift, Snowflake):**
- These databases have built-in temporary table mechanisms
- **Redshift**: Uses `CREATE TEMPORARY TABLE` - automatically dropped when session ends
- **Snowflake**: Uses `CREATE TEMPORARY TABLE` - automatically dropped when session ends
- The library's cleanup method is a no-op for these databases

**Why the Difference?**
- **Athena & Trino**: Don't have true temporary table features, so library manages cleanup
- **BigQuery**: Has temporary tables, but library uses standard tables for better control
- **Redshift & Snowflake**: Have robust temporary table features that handle cleanup automatically

#### **Frequently Asked Questions**

**Q: Why does Athena require "manual" cleanup while others are automatic?**
A: Athena creates external tables backed by S3 data. The library automatically calls `DROP TABLE` after each test, which removes the table metadata from AWS Glue catalog. **However, the actual S3 data files remain and must be cleaned up separately** - either manually or through S3 lifecycle policies. This two-step cleanup process is why it's considered "manual" compared to true temporary tables.

**Q: What does "explicit cleanup" mean for Trino?**
A: Trino's memory catalog doesn't automatically clean up tables when sessions end. The library explicitly calls `DROP TABLE IF EXISTS` after each test to remove the tables. Like Athena, if a test fails catastrophically, some tables might persist until the Trino server restarts.

**Q: What is the TTL (Time To Live) for BigQuery tables?**
A: BigQuery tables created by the library are **standard tables without TTL** - they persist until explicitly deleted. The library immediately calls `client.delete_table()` after each test. If you want to set TTL as a safety net, you can configure it at the dataset level (e.g., 24 hours) to auto-delete any orphaned tables.

**Q: Which databases leave artifacts if tests crash?**
- **BigQuery, Athena, Trino**: May leave tables if library crashes before cleanup
- **Redshift, Snowflake**: No artifacts - temporary tables auto-cleanup on session end

**Q: How to manually clean up orphaned tables?**
```sql
-- BigQuery: List and delete tables with temp prefix
SELECT table_name FROM `project.dataset.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'temp_%';

-- Athena: List and drop tables with temp prefix
SHOW TABLES LIKE 'temp_%';
DROP TABLE temp_table_name;

-- Trino: List and drop tables with temp prefix
SHOW TABLES FROM memory.default LIKE 'temp_%';
DROP TABLE memory.default.temp_table_name;
```

**Q: How to handle S3 cleanup for Athena tables?**
Athena external tables store data in S3. When `DROP TABLE` is called, only the table metadata is removed from AWS Glue catalog - **S3 data files remain**. Here are cleanup options:

**Option 1: S3 Lifecycle Policy (Recommended)**
```json
{
  "Rules": [
    {
      "ID": "DeleteSQLTestingTempFiles",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "temp_"
      },
      "Expiration": {
        "Days": 1
      }
    }
  ]
}
```

**Option 2: Manual S3 Cleanup**
```bash
# List temp files in your Athena results bucket
aws s3 ls s3://your-athena-results-bucket/ --recursive | grep temp_

# Delete temp files older than 1 day
aws s3 rm s3://your-athena-results-bucket/ --recursive --exclude "*" --include "temp_*"
```

**Option 3: Automated Cleanup Script**
```bash
#!/bin/bash
# Delete S3 objects older than 1 day with temp_ prefix
aws s3api list-objects-v2 --bucket your-athena-results-bucket --prefix "temp_" \
  --query 'Contents[?LastModified<=`2024-01-01`].Key' --output text | \
  xargs -I {} aws s3 rm s3://your-athena-results-bucket/{}
```

#### **Query Size Limits (When Physical Tables Auto-Activate)**

| Database | CTE Query Size Limit | Physical Tables Threshold |
|----------|---------------------|---------------------------|
| **BigQuery** | ~1MB (estimated) | Large dataset or complex CTEs |
| **Athena** | 256KB | Automatically switches at 256KB |
| **Redshift** | 16MB | Automatically switches at 16MB |
| **Trino** | 16MB (estimated) | Large dataset or complex CTEs |
| **Snowflake** | 1MB | Automatically switches at 1MB |

### How to Control Execution Mode

```python
# Default: CTE Mode (recommended for most use cases)
@sql_test(mock_tables=[...], result_class=ResultClass)
def test_default_mode():
    return TestCase(query="SELECT * FROM table")

# Explicit CTE Mode
@sql_test(mock_tables=[...], result_class=ResultClass)
def test_explicit_cte():
    return TestCase(
        query="SELECT * FROM table",
        use_physical_tables=False  # Explicit CTE mode
    )

# Explicit Physical Tables Mode
@sql_test(mock_tables=[...], result_class=ResultClass)
def test_physical_tables():
    return TestCase(
        query="SELECT * FROM table",
        use_physical_tables=True  # Force physical tables
    )
```

**Notes:**
- **CTE Mode**: Default mode, works with all database engines, suitable for most use cases
- **Physical Tables**: Used automatically when CTE queries exceed database size limits or when explicitly requested
- **Snowflake**: Full support for both CTE and physical table modes

## Installation

### For End Users (pip)
```bash
# Install with BigQuery support
pip install sql-testing-library[bigquery]

# Install with Athena support
pip install sql-testing-library[athena]

# Install with Redshift support
pip install sql-testing-library[redshift]

# Install with Trino support
pip install sql-testing-library[trino]

# Install with Snowflake support
pip install sql-testing-library[snowflake]

# Or install with all database adapters
pip install sql-testing-library[all]
```

### For Development (poetry)
```bash
# Install base dependencies
poetry install

# Install with specific database support
poetry install --with bigquery
poetry install --with athena
poetry install --with redshift
poetry install --with trino
poetry install --with snowflake

# Install with all database adapters and dev tools
poetry install --with bigquery,athena,redshift,trino,snowflake,dev
```

## Quick Start

1. **Configure your database** in `pytest.ini`:

```ini
[sql_testing]
adapter = bigquery  # Use 'bigquery', 'athena', 'redshift', 'trino', or 'snowflake'

# BigQuery configuration
[sql_testing.bigquery]
project_id = <my-test-project>
dataset_id = <test_dataset>
credentials_path = <path to credentials json>

# Athena configuration
# [sql_testing.athena]
# database = <test_database>
# s3_output_location = s3://my-athena-results/
# region = us-west-2
# aws_access_key_id = <optional>  # Optional: if not using default credentials
# aws_secret_access_key = <optional>  # Optional: if not using default credentials

# Redshift configuration
# [sql_testing.redshift]
# host = <redshift-host.example.com>
# database = <test_database>
# user = <redshift_user>
# password = <redshift_password>
# port = <5439>  # Optional: default port is 5439

# Trino configuration
# [sql_testing.trino]
# host = <trino-host.example.com>
# port = <8080>  # Optional: default port is 8080
# user = <trino_user>
# catalog = <memory>  # Optional: default catalog is 'memory'
# schema = <default>  # Optional: default schema is 'default'
# http_scheme = <http>  # Optional: default is 'http', use 'https' for secure connections
#
# # Authentication configuration (choose one method)
# # For Basic Authentication:
# auth_type = basic
# password = <trino_password>
#
# # For JWT Authentication:
# # auth_type = jwt
# # token = <jwt_token>

# Snowflake configuration
# [sql_testing.snowflake]
# account = <account-identifier>
# user = <snowflake_user>
# database = <test_database>
# schema = <PUBLIC>  # Optional: default schema is 'PUBLIC'
# warehouse = <compute_wh>  # Required: specify a warehouse
# role = <role_name>  # Optional: specify a role
#
# # Authentication (choose one):
# # Option 1: Key-pair authentication (recommended for MFA)
# private_key_path = </path/to/private_key.pem>
# # Or use environment variable SNOWFLAKE_PRIVATE_KEY
#
# # Option 2: Password authentication (for accounts without MFA)
# password = <snowflake_password>
```

### Database Context Understanding

Each database adapter uses a different concept for organizing tables and queries. Understanding the **database context** - the minimum qualification needed to uniquely identify a table - is crucial for writing mock tables and queries:

| Adapter | Database Context Format | Components | Mock Table Example | Query Example |
|---------|------------------------|------------|-------------------|---------------|
| **BigQuery** | `{project_id}.{dataset_id}` | project + dataset | `"test-project.test_dataset"` | `SELECT * FROM test-project.test_dataset.users` |
| **Athena** | `{database}` | database only | `"test_db"` | `SELECT * FROM test_db.customers` |
| **Redshift** | `{database}` | database only | `"test_db"` | `SELECT * FROM test_db.orders` |
| **Snowflake** | `{database}.{schema}` | database + schema | `"test_db.public"` | `SELECT * FROM test_db.public.products` |
| **Trino** | `{catalog}.{schema}` | catalog + schema | `"memory.default"` | `SELECT * FROM memory.default.inventory` |

#### Key Points:

1. **Mock Tables**: Use hardcoded database contexts in your test mock tables. Don't rely on environment variables - this makes tests predictable and consistent.

2. **default_namespace Parameter**: This parameter serves as a **namespace resolution context** that qualifies unqualified table names in your SQL queries. When creating TestCase instances, use the same database context format as your mock tables.

3. **Query References**: Your SQL queries can use either:
   - **Fully qualified names**: `SELECT * FROM test-project.test_dataset.users`
   - **Unqualified names**: `SELECT * FROM users` (qualified using `default_namespace`)

4. **Case Sensitivity**:
   - **All SQL Adapters**: Table name matching is **case-insensitive** - you can use lowercase contexts like `"test_db.public"` even if your SQL uses `FROM CUSTOMERS`
   - This follows standard SQL behavior where table names are case-insensitive

#### Understanding the `default_namespace` Parameter

The `default_namespace` parameter is **not where your SQL executes** - it's the **namespace prefix** used to resolve unqualified table names in your queries.

**How it works:**
- **Query**: `SELECT * FROM users JOIN orders ON users.id = orders.user_id`
- **default_namespace**: `"test-project.test_dataset"`
- **Resolution**: `users` → `test-project.test_dataset.users`, `orders` → `test-project.test_dataset.orders`
- **Requirement**: These resolved names must match your mock tables' `get_qualified_name()` values

**Alternative parameter names under consideration:**
- `default_namespace` ✅ (most clear about purpose)
- `table_context`
- `namespace_prefix`

**Example showing the difference:**

```python
# Option 1: Fully qualified table names (default_namespace not used for resolution)
TestCase(
    query="SELECT * FROM test-project.test_dataset.users",
    default_namespace="test-project.test_dataset",  # For consistency, but not used
)

# Option 2: Unqualified table names (default_namespace used for resolution)
TestCase(
    query="SELECT * FROM users",  # Unqualified
    default_namespace="test-project.test_dataset",  # Qualifies to: test-project.test_dataset.users
)
```

#### Example Mock Table Implementations:

```python
# BigQuery Mock Table
class UsersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test-project.test_dataset"  # project.dataset format

    def get_table_name(self) -> str:
        return "users"

# Athena Mock Table
class CustomerMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"  # database only

    def get_table_name(self) -> str:
        return "customers"

# Snowflake Mock Table
class ProductsMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db.public"  # database.schema format (lowercase)

    def get_table_name(self) -> str:
        return "products"
```

2. **Write a test** using one of the flexible patterns:

```python
from dataclasses import dataclass
from datetime import date
from pydantic import BaseModel
from sql_testing_library import sql_test, TestCase
from sql_testing_library.mock_table import BaseMockTable

@dataclass
class User:
    user_id: int
    name: str
    email: str

class UserResult(BaseModel):
    user_id: int
    name: str

class UsersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "sqltesting_db"

    def get_table_name(self) -> str:
        return "users"

# Pattern 1: Define all test data in the decorator
@sql_test(
    mock_tables=[
        UsersMockTable([
            User(1, "Alice", "alice@example.com"),
            User(2, "Bob", "bob@example.com")
        ])
    ],
    result_class=UserResult
)
def test_pattern_1():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="sqltesting_db"
    )

# Pattern 2: Define all test data in the TestCase
@sql_test()  # Empty decorator
def test_pattern_2():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="sqltesting_db",
        mock_tables=[
            UsersMockTable([
                User(1, "Alice", "alice@example.com"),
                User(2, "Bob", "bob@example.com")
            ])
        ],
        result_class=UserResult
    )

# Pattern 3: Mix and match between decorator and TestCase
@sql_test(
    mock_tables=[
        UsersMockTable([
            User(1, "Alice", "alice@example.com"),
            User(2, "Bob", "bob@example.com")
        ])
    ]
)
def test_pattern_3():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="sqltesting_db",
        result_class=UserResult
    )
```

### Working with Struct Types (Athena and Trino)

The library supports struct/record types using Python dataclasses or Pydantic models for Athena and Trino:

```python
from dataclasses import dataclass
from decimal import Decimal
from pydantic import BaseModel
from sql_testing_library import sql_test, TestCase
from sql_testing_library.mock_table import BaseMockTable

# Define nested structs using dataclasses
@dataclass
class Address:
    street: str
    city: str
    state: str
    zip_code: str

@dataclass
class Employee:
    id: int
    name: str
    salary: Decimal
    address: Address  # Nested struct
    is_active: bool = True

# Or use Pydantic models
class AddressPydantic(BaseModel):
    street: str
    city: str
    state: str
    zip_code: str

class EmployeeResultPydantic(BaseModel):
    id: int
    name: str
    city: str  # Extracted from nested struct

# Mock table with struct data
class EmployeesMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "employees"

# Test with struct types
@sql_test(
    adapter_type="athena",  # or "trino"
    mock_tables=[
        EmployeesMockTable([
            Employee(
                id=1,
                name="Alice Johnson",
                salary=Decimal("120000.00"),
                address=Address(
                    street="123 Tech Lane",
                    city="San Francisco",
                    state="CA",
                    zip_code="94105"
                ),
                is_active=True
            ),
            Employee(
                id=2,
                name="Bob Smith",
                salary=Decimal("95000.00"),
                address=Address(
                    street="456 Oak Ave",
                    city="New York",
                    state="NY",
                    zip_code="10001"
                ),
                is_active=False
            )
        ])
    ],
    result_class=EmployeeResultPydantic
)
def test_struct_with_dot_notation():
    return TestCase(
        query="""
            SELECT
                id,
                name,
                address.city as city  -- Access nested field with dot notation
            FROM employees
            WHERE address.state = 'CA'  -- Use struct fields in WHERE clause
        """,
        default_namespace="test_db"
    )

# You can also query entire structs
@sql_test(
    adapter_type="trino",
    mock_tables=[EmployeesMockTable([...])],
    result_class=dict  # Returns full struct as dict
)
def test_query_full_struct():
    return TestCase(
        query="SELECT id, name, address FROM employees",
        default_namespace="test_db"
    )
```

**Struct Type Features:**
- **Nested Structures**: Support for deeply nested structs using dataclasses or Pydantic models
- **Dot Notation**: Access struct fields using `struct.field` syntax in queries
- **Type Safety**: Full type conversion between Python objects and SQL ROW types
- **NULL Handling**: Proper handling of optional struct fields
- **WHERE Clause**: Use struct fields in filtering conditions
- **List of Structs**: Full support for `List[StructType]` with array operations

**SQL Type Mapping:**
- Python dataclass/Pydantic model → SQL `ROW(field1 type1, field2 type2, ...)`
- Nested structs are fully supported
- All struct values are properly cast to ensure type consistency

3. **Run with pytest**:

```bash
# Run all tests
pytest test_users.py

# Run only SQL tests (using the sql_test marker)
pytest -m sql_test

# Exclude SQL tests
pytest -m "not sql_test"

# Run a specific test
pytest test_users.py::test_user_query

# If using Poetry
poetry run pytest test_users.py::test_user_query
```

## Troubleshooting

### "No [sql_testing] section found" Error

If you encounter the error `No [sql_testing] section found in pytest.ini, setup.cfg, or tox.ini`, this typically happens when using IDEs like PyCharm, VS Code, or other development environments that run pytest from a different working directory.

**Problem**: The library looks for configuration files (`pytest.ini`, `setup.cfg`, `tox.ini`) in the current working directory, but IDEs may run tests from a different location.

#### Solution 1: Set Environment Variable (Recommended)

Set the `SQL_TESTING_PROJECT_ROOT` environment variable to point to your project root directory:

**In PyCharm:**
1. Go to **Run/Debug Configurations**
2. Select your test configuration
3. In **Environment variables**, add:
   - Name: `SQL_TESTING_PROJECT_ROOT`
   - Value: `/path/to/your/project/root` (where your `pytest.ini` is located)

**In VS Code:**
Add to your `.vscode/settings.json`:
```json
{
    "python.testing.pytestArgs": [
        "--rootdir=/path/to/your/project/root"
    ],
    "python.envFile": "${workspaceFolder}/.env"
}
```

Create a `.env` file in your project root:
```bash
SQL_TESTING_PROJECT_ROOT=/path/to/your/project/root
```

#### Solution 2: Use conftest.py (Automatic)

Create a `conftest.py` file in your project root directory:

```python
"""
PyTest configuration file to ensure SQL Testing Library can find config
"""
import os
import pytest

def pytest_configure(config):
    """Ensure SQL_TESTING_PROJECT_ROOT is set for IDE compatibility"""
    if not os.environ.get('SQL_TESTING_PROJECT_ROOT'):
        # Set to current working directory where conftest.py is located
        project_root = os.path.dirname(os.path.abspath(__file__))
        os.environ['SQL_TESTING_PROJECT_ROOT'] = project_root
        print(f"Setting SQL_TESTING_PROJECT_ROOT to: {project_root}")
```

This automatically sets the project root when pytest runs, regardless of the IDE or working directory.

#### Solution 3: Alternative Configuration File

Create a `setup.cfg` file alongside your `pytest.ini`:

```ini
[tool:pytest]
testpaths = tests

[sql_testing]
adapter = bigquery

[sql_testing.bigquery]
project_id = your-project-id
dataset_id = your_dataset
credentials_path = /path/to/credentials.json
```

#### Solution 4: Set Working Directory in IDE

**In PyCharm:**
1. Go to **Run/Debug Configurations**
2. Set **Working directory** to your project root (where `pytest.ini` is located)

**In VS Code:**
Ensure your workspace is opened at the project root level where `pytest.ini` exists.

#### Verification

To verify your configuration is working, run this Python snippet:

```python
from sql_testing_library._pytest_plugin import SQLTestDecorator

decorator = SQLTestDecorator()
try:
    project_root = decorator._get_project_root()
    print(f"Project root: {project_root}")

    config_parser = decorator._get_config_parser()
    print(f"Config sections: {config_parser.sections()}")

    if 'sql_testing' in config_parser:
        adapter = config_parser.get('sql_testing', 'adapter')
        print(f"✅ Configuration found! Adapter: {adapter}")
    else:
        print("❌ No sql_testing section found")
except Exception as e:
    print(f"❌ Error: {e}")
```

### Common IDE-Specific Issues

**PyCharm**: Often runs pytest from the project parent directory instead of the project root.
- **Solution**: Set working directory or use `conftest.py`

**VS Code**: May not respect the pytest.ini location when using the Python extension.
- **Solution**: Use `.env` file or set `python.testing.pytestArgs` in settings

**Jupyter Notebooks**: Running tests in notebooks may not find configuration files.
- **Solution**: Set `SQL_TESTING_PROJECT_ROOT` environment variable in the notebook

**Docker/Containers**: Configuration files may not be mounted or accessible.
- **Solution**: Ensure config files are included in your Docker build context and set the environment variable

## Usage Patterns

The library supports flexible ways to configure your tests:

1. **All Config in Decorator**: Define all mock tables and result class in the `@sql_test` decorator, with only query and default_namespace in TestCase.
2. **All Config in TestCase**: Use an empty `@sql_test()` decorator and define everything in the TestCase return value.
3. **Mix and Match**: Specify some parameters in the decorator and others in the TestCase.
4. **Per-Test Database Adapters**: Specify which adapter to use for specific tests.

**Important notes**:
- Parameters provided in the decorator take precedence over those in TestCase
- Either the decorator or TestCase must provide mock_tables and result_class

### Using Different Database Adapters in Tests

The adapter specified in `[sql_testing]` section acts as the default adapter for all tests. When you don't specify an `adapter_type` in your test, it uses this default.

```ini
[sql_testing]
adapter = snowflake  # This becomes the default for all tests
```

You can override the default adapter for individual tests:

```python
# Use BigQuery adapter for this test
@sql_test(
    adapter_type="bigquery",
    mock_tables=[...],
    result_class=UserResult
)
def test_bigquery_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="sqltesting_db"
    )

# Use Athena adapter for this test
@sql_test(
    adapter_type="athena",
    mock_tables=[...],
    result_class=UserResult
)
def test_athena_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="test_db"
    )

# Use Redshift adapter for this test
@sql_test(
    adapter_type="redshift",
    mock_tables=[...],
    result_class=UserResult
)
def test_redshift_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="test_db"
    )

# Use Trino adapter for this test
@sql_test(
    adapter_type="trino",
    mock_tables=[...],
    result_class=UserResult
)
def test_trino_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="test_db"
    )

# Use Snowflake adapter for this test
@sql_test(
    adapter_type="snowflake",
    mock_tables=[...],
    result_class=UserResult
)
def test_snowflake_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="test_db"
    )
```

The adapter_type parameter will use the configuration from the corresponding section in pytest.ini, such as `[sql_testing.bigquery]`, `[sql_testing.athena]`, `[sql_testing.redshift]`, `[sql_testing.trino]`, or `[sql_testing.snowflake]`.

**Default Adapter Behavior:**
- If `adapter_type` is not specified in the test, the library uses the adapter from `[sql_testing]` section's `adapter` setting
- If no adapter is specified in the `[sql_testing]` section, it defaults to "bigquery"
- Each adapter reads its configuration from `[sql_testing.<adapter_name>]` section

### Adapter-Specific Features

#### BigQuery Adapter
- Supports Google Cloud BigQuery service
- Uses UNION ALL pattern for CTE creation with complex data types
- Handles authentication via service account or application default credentials

#### Athena Adapter
- Supports Amazon Athena service for querying data in S3
- Uses CTAS (CREATE TABLE AS SELECT) for efficient temporary table creation
- Handles large queries by automatically falling back to physical tables
- Supports authentication via AWS credentials or instance profiles

#### Redshift Adapter
- Supports Amazon Redshift data warehouse service
- Uses CTAS (CREATE TABLE AS SELECT) for efficient temporary table creation
- Takes advantage of Redshift's automatic session-based temporary table cleanup
- Handles large datasets and complex queries with SQL-compliant syntax
- Supports authentication via username and password

#### Trino Adapter
- Supports Trino (formerly PrestoSQL) distributed SQL query engine
- Uses CTAS (CREATE TABLE AS SELECT) for efficient temporary table creation
- Provides explicit table cleanup management
- Works with a variety of catalogs and data sources
- Handles large datasets and complex queries with full SQL support
- Supports multiple authentication methods including Basic and JWT

#### Snowflake Adapter
- Supports Snowflake cloud data platform
- Uses CTAS (CREATE TABLE AS SELECT) for efficient temporary table creation
- Creates temporary tables that automatically expire at the end of the session
- Handles large datasets and complex queries with Snowflake's SQL dialect
- Supports authentication via username and password
- Optional support for warehouse, role, and schema specification

**Default Behavior:**
- If adapter_type is not specified in the TestCase or decorator, the library will use the adapter specified in the `[sql_testing]` section's `adapter` setting.
- If no adapter is specified in the `[sql_testing]` section, it defaults to "bigquery".
- The library will then look for adapter-specific configuration in the `[sql_testing.<adapter>]` section.
- If the adapter-specific section doesn't exist, it falls back to using the `[sql_testing]` section for backward compatibility.

## Development Setup

### Quick Start with Make

The project includes a Makefile for common development tasks:

```bash
# Install all dependencies
make install

# Run unit tests
make test

# Run linting and type checking
make lint

# Format code
make format

# Run all checks (lint + format check + tests)
make check

# See all available commands
make help
```

### Available Make Commands

| Command | Description |
|---------|-------------|
| `make install` | Install all dependencies with poetry |
| `make test` | Run unit tests with coverage |
| `make test-unit` | Run unit tests (excludes integration tests) |
| `make test-integration` | Run integration tests (requires DB credentials) |
| `make test-all` | Run all tests (unit + integration) |
| `make test-tox` | Run tests across all Python versions (3.9-3.12) |
| `make lint` | Run ruff and mypy checks |
| `make format` | Format code with black and ruff |
| `make check` | Run all checks (lint + format + tests) |
| `make clean` | Remove build artifacts and cache files |
| `make build` | Build distribution packages |
| `make docs` | Build documentation |

### Testing Across Python Versions

The project supports Python 3.9-3.12. You can test across all versions using:

```bash
# Using tox (automatically tests all Python versions)
make test-tox

# Or directly with tox
tox

# Test specific Python version
tox -e py39  # Python 3.9
tox -e py310 # Python 3.10
tox -e py311 # Python 3.11
tox -e py312 # Python 3.12
```

### Code Quality

The project uses comprehensive tools to ensure code quality:

1. **Ruff** for linting and formatting
2. **Black** for code formatting
3. **Mypy** for static type checking
4. **Pre-commit hooks** for automated checks

To set up the development environment:

1. Install development dependencies:
   ```bash
   # Using make
   make install

   # Or directly with poetry
   poetry install --all-extras
   ```

2. Set up pre-commit hooks:
   ```bash
   ./scripts/setup-hooks.sh
   ```

This ensures code is automatically formatted, linted, and type-checked on commit.

For more information on code quality standards, see [docs/linting.md](docs/linting.md).

## CI/CD Integration

The library includes comprehensive GitHub Actions workflows for automated testing across multiple database platforms:

### Integration Tests
Automatically runs on every PR and merge to master:
- **Unit Tests**: Mock-based tests in `tests/` (free)
- **Integration Tests**: Real database tests in `tests/integration/` (minimal cost)
- **Cleanup**: Automatic resource cleanup

### Athena Integration Tests
- **Real AWS Athena tests** with automatic S3 setup and cleanup
- **Cost**: ~$0.05 per test run
- **Setup Guide**: [Athena CI/CD Setup](.github/ATHENA_CICD_SETUP.md)

**Required Setup**:
- **Secrets**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ATHENA_OUTPUT_LOCATION`
- **Variables**: `AWS_ATHENA_DATABASE`, `AWS_REGION` (optional)

**Validation**:
```bash
python scripts/validate-athena-setup.py
```

### BigQuery Integration Tests
- **Real GCP BigQuery tests** with dataset creation and cleanup
- **Cost**: Minimal (within free tier for most use cases)
- **Setup Guide**: [BigQuery CI/CD Setup](.github/BIGQUERY_CICD_SETUP.md)

**Required Setup**:
- **Secrets**: `GCP_SA_KEY`, `GCP_PROJECT_ID`

**Validation**:
```bash
python scripts/validate-bigquery-setup.py
```

### Redshift Integration Tests
- **Real AWS Redshift Serverless tests** with namespace/workgroup creation
- **Cost**: ~$0.50-$1.00 per test run (free tier: $300 credit for new accounts)
- **Setup Guide**: [Redshift CI/CD Setup](.github/REDSHIFT_CICD_SETUP.md)

**Required Setup**:
- **Secrets**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `REDSHIFT_ADMIN_PASSWORD`
- **Variables**: `AWS_REGION`, `REDSHIFT_NAMESPACE`, `REDSHIFT_WORKGROUP` (optional)
- **IAM Permissions**: Includes EC2 permissions for automatic security group configuration

**Validation**:
```bash
python scripts/validate-redshift-setup.py
```

**Manual Testing**:
```bash
# Create Redshift cluster (automatically configures security groups for connectivity)
python scripts/manage-redshift-cluster.py create

# Get connection details and psql command
python scripts/manage-redshift-cluster.py endpoint

# Run integration tests
poetry run pytest tests/integration/test_redshift_integration.py -v

# Clean up resources (automatically waits for proper deletion order)
python scripts/manage-redshift-cluster.py destroy
```

### Trino Integration Tests
- **Real Trino tests** using Docker with Memory connector
- **Cost**: Free (runs locally with Docker)
- **Setup Guide**: [Trino CI/CD Setup](.github/TRINO_CICD_SETUP.md)

**Required Setup**:
- Docker for containerized Trino server
- No additional secrets or variables required

**Manual Testing**:
```bash
# Run integration tests (automatically manages Docker containers)
poetry run pytest tests/integration/test_trino_integration.py -v
```

### Snowflake Integration Tests
- **Real Snowflake tests** using cloud data platform
- **Cost**: Compute time charges based on warehouse size
- **Setup Guide**: [Snowflake CI/CD Setup](.github/SNOWFLAKE_CICD_SETUP.md)

**Required Setup**:
- **Secrets**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- **Variables**: `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE` (optional)

**Validation**:
```bash
python scripts/validate-snowflake-setup.py
```

**Manual Testing**:
```bash
# Run integration tests
poetry run pytest tests/integration/test_snowflake_integration.py -v
```

## Documentation

The library automatically:
- Parses SQL to find table references
- Resolves unqualified table names with database context
- Injects mock data via CTEs or temp tables
- Deserializes results to typed Python objects

For detailed usage and configuration options, see the example files included.

## Known Limitations and TODOs

The library has a few known limitations that are planned to be addressed in future updates:

### General Improvements
- Add support for more SQL dialects
- Improve error handling for malformed SQL
- Enhance documentation with more examples

## Requirements

- Python >= 3.9
- sqlglot >= 18.0.0
- pydantic >= 2.0.0
- Database-specific clients:
  - google-cloud-bigquery for BigQuery
  - boto3 for Athena
  - psycopg2-binary for Redshift
  - trino for Trino
  - snowflake-connector-python for Snowflake
