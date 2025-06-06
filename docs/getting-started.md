---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Installation

The SQL Testing Library supports multiple database backends. Install only what you need:

### Install with specific database support

```bash
# For BigQuery
pip install sql-testing-library[bigquery]

# For Athena
pip install sql-testing-library[athena]

# For Redshift
pip install sql-testing-library[redshift]

# For Trino
pip install sql-testing-library[trino]

# For Snowflake
pip install sql-testing-library[snowflake]
```

### Install with all database adapters

```bash
pip install sql-testing-library[all]
```

### Development installation

If you're contributing to the library:

```bash
# Clone the repository
git clone https://github.com/gurmeetsaran/sqltesting.git
cd sqltesting

# Install with poetry
poetry install --with bigquery,athena,redshift,trino,snowflake,dev
```

## Configuration

The library uses pytest configuration files to manage database connections. Create a `pytest.ini` file in your project root:

### Basic configuration

```ini
[sql_testing]
adapter = bigquery  # Choose: bigquery, athena, redshift, trino, or snowflake
```

### Database-specific configuration

#### BigQuery

```ini
[sql_testing.bigquery]
project_id = my-test-project
dataset_id = test_dataset
credentials_path = path/to/service-account-key.json
```

#### Athena

```ini
[sql_testing.athena]
database = test_database
s3_output_location = s3://my-athena-results/
region = us-west-2
# Optional: if not using default AWS credentials
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

#### Redshift

```ini
[sql_testing.redshift]
host = redshift-cluster.region.redshift.amazonaws.com
database = test_database
user = redshift_user
password = redshift_password
port = 5439  # Optional, defaults to 5439
```

#### Trino

```ini
[sql_testing.trino]
host = trino-server.example.com
port = 8080  # Optional, defaults to 8080
user = trino_user
catalog = memory  # Optional, defaults to 'memory'
schema = default  # Optional, defaults to 'default'
http_scheme = http  # Optional, use 'https' for secure connections

# For Basic Authentication:
auth_type = basic
password = trino_password

# For JWT Authentication:
# auth_type = jwt
# token = your_jwt_token
```

#### Snowflake

```ini
[sql_testing.snowflake]
account = account-identifier
user = snowflake_user
password = snowflake_password
database = test_database
schema = PUBLIC  # Optional, defaults to 'PUBLIC'
warehouse = compute_wh  # Required
role = my_role  # Optional
```

## Writing Your First Test

### 1. Create a mock table

```python
from dataclasses import dataclass
from sql_testing_library.mock_table import BaseMockTable

@dataclass
class User:
    user_id: int
    name: str
    email: str
    active: bool

class UsersMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"  # Your test database/dataset

    def get_table_name(self) -> str:
        return "users"
```

### 2. Write a test with the @sql_test decorator

```python
from sql_testing_library import sql_test, TestCase
from pydantic import BaseModel

# Define result model
class ActiveUserResult(BaseModel):
    user_id: int
    name: str

# Create test
@sql_test(
    mock_tables=[
        UsersMockTable([
            User(1, "Alice", "alice@example.com", True),
            User(2, "Bob", "bob@example.com", False),
            User(3, "Charlie", "charlie@example.com", True)
        ])
    ],
    result_class=ActiveUserResult
)
def test_active_users():
    return TestCase(
        query="""
            SELECT user_id, name
            FROM users
            WHERE active = true
            ORDER BY user_id
        """,
        default_namespace="test_db"
    )
```

### 3. Run your test

```bash
# Run all tests
pytest test_users.py

# Run only SQL tests (using the sql_test marker)
pytest -m sql_test

# Exclude SQL tests from your test run
pytest -m "not sql_test"

# Run a specific test
pytest test_users.py::test_active_users -v

# With poetry
poetry run pytest test_users.py

# Combine markers with other pytest options
pytest -m sql_test -v --tb=short
```

**Note**: The `@sql_test` decorator automatically adds a pytest marker to your tests, making it easy to run or exclude SQL tests from your test suite.

## Understanding the Basics

### Mock Tables

Mock tables represent your database tables with test data:

- Inherit from `BaseMockTable`
- Implement `get_database_name()` and `get_table_name()`
- Pass data as a list of dataclasses, dictionaries, or objects

### Test Cases

The `TestCase` class defines your SQL test:

- `query`: The SQL query to test
- `default_namespace`: Database context for unqualified table names
- `mock_tables`: List of mock tables (can be in decorator or TestCase)
- `result_class`: Class for deserializing results (dataclass or Pydantic)

### Result Classes

Define expected results using:

- Python dataclasses
- Pydantic models
- Dict (for simple key-value results)

## Common Patterns

### Pattern 1: All configuration in decorator

```python
@sql_test(
    mock_tables=[users_mock, orders_mock],
    result_class=OrderSummary
)
def test_order_summary():
    return TestCase(
        query="SELECT * FROM orders JOIN users ON orders.user_id = users.id",
        default_namespace="test_db"
    )
```

### Pattern 2: All configuration in TestCase

```python
@sql_test()  # Empty decorator
def test_order_summary():
    return TestCase(
        query="SELECT * FROM orders JOIN users ON orders.user_id = users.id",
        default_namespace="test_db",
        mock_tables=[users_mock, orders_mock],
        result_class=OrderSummary
    )
```

### Pattern 3: Using physical tables for large datasets

```python
@sql_test(
    mock_tables=[large_dataset_mock],
    result_class=ResultClass,
    use_physical_tables=True  # Force physical tables
)
def test_large_dataset():
    return TestCase(
        query="SELECT * FROM large_table",
        default_namespace="test_db"
    )
```

## Next Steps

- [Learn about database adapters](adapters)
- [Explore advanced examples](examples)
- [Read the API reference](api-reference)
- [Debug with SQL logging](sql-logging)
- [Troubleshooting guide](troubleshooting)

## Quick Tips

1. **Start Simple**: Begin with basic queries and gradually add complexity
2. **Use Type Hints**: Leverage dataclasses and Pydantic for type safety
3. **Test Incrementally**: Test individual CTEs and subqueries separately
4. **Mock Realistically**: Use representative test data that matches production schemas
5. **Check Query Plans**: Use `EXPLAIN` to understand how your queries execute
