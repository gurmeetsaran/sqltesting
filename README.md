# SQL Testing Library

A Python library for testing SQL queries with mock data injection across Athena, BigQuery, Redshift, Trino, and Snowflake.

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

[![Unit Tests](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/tests.yaml)
[![Athena Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/athena-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/athena-integration.yml)
[![BigQuery Integration](https://github.com/gurmeetsaran/sqltesting/actions/workflows/bigquery-integration.yml/badge.svg)](https://github.com/gurmeetsaran/sqltesting/actions/workflows/bigquery-integration.yml)
[![GitHub license](https://img.shields.io/github/license/gurmeetsaran/sqltesting.svg)](https://github.com/gurmeetsaran/sqltesting/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/gurmeetsaran/sqltesting/branch/master/graph/badge.svg?token=CN3G5X5ZA5)](https://codecov.io/gh/gurmeetsaran/sqltesting)
![python version](https://img.shields.io/badge/python-3.9%2B-yellowgreen)
## Features

- **Multi-Database Support**: Test SQL across BigQuery, Athena, Redshift, Trino, and Snowflake
- **Mock Data Injection**: Use Python dataclasses for type-safe test data
- **CTE or Physical Tables**: Automatic fallback for query size limits
- **Type-Safe Results**: Deserialize results to Pydantic models
- **Pytest Integration**: Seamless testing with `@sql_test` decorator

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

# Install with all database adapters and dev tools
poetry install --with bigquery,athena,redshift,trino,dev
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
# password = <snowflake_password>
# database = <test_database>
# schema = <PUBLIC>  # Optional: default schema is 'PUBLIC'
# warehouse = <compute_wh>  # Optional: specify a warehouse
# role = <role_name>  # Optional: specify a role
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

# Use Trino adapter for this test
@sql_test(
    adapter_type="trino",
    mock_tables=[...],
    result_class=UserResult
)
def test_trino_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        execution_database="test_db"
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
        execution_database="test_db"
    )
```

The adapter_type parameter will use the configuration from the corresponding section in pytest.ini, such as `[sql_testing.bigquery]`, `[sql_testing.athena]`, `[sql_testing.redshift]`, `[sql_testing.trino]`, or `[sql_testing.snowflake]`.

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

### Code Quality

The project uses comprehensive tools to ensure code quality:

1. **Ruff** for linting and formatting
2. **Mypy** for static type checking
3. **Pre-commit hooks** for automated checks

To set up the development environment:

1. Install development dependencies:
   ```bash
   # Install all dependencies including database adapters and dev tools
   poetry install --with bigquery,athena,redshift,trino,dev
   ```

2. Set up pre-commit hooks:
   ```bash
   ./scripts/setup-hooks.sh
   ```

This ensures code is automatically formatted, linted, and type-checked on commit.

For more information on code quality standards, see [docs/linting.md](docs/linting.md).

## CI/CD Integration

The library includes GitHub Actions workflows for automated testing:

### Athena Integration Tests
Automatically runs on every PR and merge to master:
- **Unit Tests**: Mock-based tests in `tests/` (free)
- **Integration Tests**: Real AWS Athena tests in `tests/integration/` (minimal cost)
- **Cleanup**: Automatic resource cleanup

#### Setup Guide
1. **Configure GitHub Secrets & Variables** (see [Setup Guide](.github/ATHENA_CICD_SETUP.md)):
   - **Secrets**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ATHENA_OUTPUT_LOCATION`
   - **Variables**: `AWS_ATHENA_DATABASE`, `AWS_REGION` (optional)

2. **Validate Setup**:
   ```bash
   python scripts/validate-athena-setup.py
   ```

3. **Monitor Results**: Check GitHub Actions tab for test results

**Cost**: ~$0.05 per test run. For cost optimization after release, see the [setup guide](.github/ATHENA_CICD_SETUP.md).

## Documentation

The library automatically:
- Parses SQL to find table references
- Resolves unqualified table names with database context
- Injects mock data via CTEs or temp tables
- Deserializes results to typed Python objects

For detailed usage and configuration options, see the example files included.

## Known Limitations and TODOs

The library has a few known limitations that are planned to be addressed in future updates:

### Core Functionality
- The `mock_table` function is not exposed in the public API but is used in tests
- TestCase interface could be streamlined for better usability

### Snowflake Support
- Physical table tests for Snowflake are currently skipped due to complex mocking requirements
- Need better support for Snowflake-specific data types (VARIANT, OBJECT, ARRAY)
- Add integration tests with actual Snowflake connection

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
