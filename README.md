# SQL Testing Library

A Python library for testing SQL queries with mock data injection across Athena, BigQuery, and Redshift.

[![Tests](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml)
[![GitHub license](https://img.shields.io/github/license/gurmeetsaran/sqltesting.svg)](https://github.com/gurmeetsaran/sqltesting/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/gurmeetsaran/sqltesting/branch/master/graph/badge.svg?token=CN3G5X5ZA5)](https://codecov.io/gh/gurmeetsaran/sqltesting)
![python version](https://img.shields.io/badge/python-3.9%2B-yellowgreen)
## Features

- **Multi-Database Support**: Test SQL across BigQuery, Athena, and Redshift
- **Mock Data Injection**: Use Python dataclasses for type-safe test data
- **CTE or Physical Tables**: Automatic fallback for query size limits
- **Type-Safe Results**: Deserialize results to Pydantic models
- **Pytest Integration**: Seamless testing with `@sql_test` decorator

## Installation

```bash
# Install with BigQuery support
pip install sql-testing-library[bigquery]

# Install with Athena support
pip install sql-testing-library[athena]

# Install with Redshift support
pip install sql-testing-library[redshift]

# Or install with all database adapters
pip install sql-testing-library[all]
```

## Quick Start

1. **Configure your database** in `pytest.ini`:

```ini
[sql_testing]
adapter = bigquery  # Use 'bigquery', 'athena', or 'redshift'

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
        return "analytics_db"

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
        execution_database="analytics_db"
    )

# Pattern 2: Define all test data in the TestCase
@sql_test()  # Empty decorator
def test_pattern_2():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        execution_database="analytics_db",
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
        execution_database="analytics_db",
        result_class=UserResult
    )
```

3. **Run with pytest**:

```bash
# Run all tests
pytest test_users.py

# Run a specific test
pytest test_users.py::test_user_query

# If using Poetry
poetry run pytest test_users.py::test_user_query
```

## Usage Patterns

The library supports flexible ways to configure your tests:

1. **All Config in Decorator**: Define all mock tables and result class in the `@sql_test` decorator, with only query and execution_database in TestCase.
2. **All Config in TestCase**: Use an empty `@sql_test()` decorator and define everything in the TestCase return value.
3. **Mix and Match**: Specify some parameters in the decorator and others in the TestCase.
4. **Per-Test Database Adapters**: Specify which adapter to use for specific tests.

**Important notes**:
- Parameters provided in the decorator take precedence over those in TestCase
- Either the decorator or TestCase must provide mock_tables and result_class

### Using Different Database Adapters in Tests

You can specify which database adapter to use for individual tests:

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
        execution_database="analytics_db"
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
        execution_database="test_db"
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
        execution_database="test_db"
    )
```

The adapter_type parameter will use the configuration from the corresponding section in pytest.ini, such as `[sql_testing.bigquery]`, `[sql_testing.athena]`, or `[sql_testing.redshift]`.

### Adapter-Specific Features

#### BigQuery Adapter
- Supports Google Cloud BigQuery service
- Uses STRUCT and UNNEST for efficient CTE creation
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

**Default Behavior:**
- If adapter_type is not specified in the TestCase or decorator, the library will use the adapter specified in the `[sql_testing]` section's `adapter` setting.
- If no adapter is specified in the `[sql_testing]` section, it defaults to "bigquery".
- The library will then look for adapter-specific configuration in the `[sql_testing.<adapter>]` section.
- If the adapter-specific section doesn't exist, it falls back to using the `[sql_testing]` section for backward compatibility.

## Development Setup

### Code Quality

The project uses comprehensive tools to ensure code quality:

1. **Ruff** for linting and formatting
2. **Mypy** for static type checking
3. **Pre-commit hooks** for automated checks

To set up the development environment:

1. Install development dependencies:
   ```bash
   poetry install --with dev
   ```

2. Set up pre-commit hooks:
   ```bash
   ./scripts/setup-hooks.sh
   ```

This ensures code is automatically formatted, linted, and type-checked on commit.

For more information on code quality standards, see [docs/linting.md](docs/linting.md).

## Documentation

The library automatically:
- Parses SQL to find table references
- Resolves unqualified table names with database context
- Injects mock data via CTEs or temp tables
- Deserializes results to typed Python objects

For detailed usage and configuration options, see the example files included.

## Requirements

- Python >= 3.9
- sqlglot >= 18.0.0
- pydantic >= 2.0.0
- Database-specific clients:
  - google-cloud-bigquery for BigQuery
  - boto3 for Athena
  - psycopg2-binary for Redshift
