# SQL Testing Library

A Python library for testing SQL queries with mock data injection across Athena, BigQuery, and Redshift.

[![Tests](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml)
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

# Or install with all database adapters
pip install sql-testing-library[all]
```

## Quick Start

1. **Configure your database** in `pytest.ini`:

```ini
[sql_testing]
adapter = bigquery
project_id = <my-test-project>
dataset_id = <test_dataset>
credentials_path = <path to credentials json>
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

**Important notes**:
- Parameters provided in the decorator take precedence over those in TestCase
- Either the decorator or TestCase must provide mock_tables and result_class

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
- Database-specific clients (google-cloud-bigquery, boto3, psycopg2-binary)
