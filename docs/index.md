---
layout: default
title: Python SQL Unit Testing with Pytest
nav_order: 1
description: "Python SQL testing library for unit testing database queries with mock data. Test BigQuery, Snowflake, Redshift, Athena, Trino, DuckDB & ClickHouse with pytest. Perfect for data engineering and ETL pipeline testing."
permalink: /
---

# SQL Testing Library for Python
{: .fs-9 }

Unit test SQL queries with mock data injection for BigQuery, Snowflake, Redshift, Athena, Trino, DuckDB, and ClickHouse. Pytest integration for data engineering and ETL testing.
{: .fs-6 .fw-300 }

[Get started now](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 } [View on GitHub](https://github.com/gurmeetsaran/sqltesting){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## 🎯 Why Use SQL Testing Library for Python?

**SQL unit testing** in data engineering can be challenging, especially when working with cloud databases like **BigQuery, Snowflake, Redshift, Athena, and ClickHouse**. This **Python SQL testing framework** addresses critical pain points:

- **Fragile Integration Tests**: Traditional SQL tests that depend on live data break when data changes, causing flaky CI/CD pipelines
- **Slow Feedback Loops**: Running database tests against full datasets takes too long for continuous integration
- **Database Engine Upgrades**: UDF semantics and SQL behavior change between versions (e.g., Athena v2 to v3), causing silent production failures
- **Database Lock-in**: SQL tests written for BigQuery don't work on Snowflake or Redshift without rewrites
- **Complex Setup**: Each cloud database requires different mocking strategies, credentials, and testing tools

### Perfect for Data Engineering Teams

Whether you're building **ETL pipelines**, validating **data transformations**, or testing **analytics queries**, this library provides a unified pytest-based framework for SQL testing across all major cloud databases.

## ✨ Key Features

### 🚀 Multi-Database Support
Test your SQL queries across BigQuery, Athena, Redshift, Trino, Snowflake, DuckDB, and ClickHouse with a unified API.

### 🎯 Type-Safe Testing
Use Python dataclasses and Pydantic models for type-safe test data and results.

### ⚡ Flexible Execution
Automatically switches between CTE injection and physical tables based on query size.

### 🧪 Pytest Integration
Seamlessly integrates with pytest using the `@sql_test` decorator.

### 📊 Comprehensive Type Support
Supports primitive types, arrays, decimals, dates, optional values, and struct types (Athena/Trino/BigQuery) across databases.

### 🔍 SQL Logging & Debugging
Automatic SQL logging with formatted output, temp table queries, and full error tracebacks for easy debugging.

## 📋 Quick Example

```python
from dataclasses import dataclass
from sql_testing_library import sql_test, TestCase
from sql_testing_library.mock_table import BaseMockTable
from pydantic import BaseModel

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
        return "test_db"

    def get_table_name(self) -> str:
        return "users"

@sql_test(
    mock_tables=[
        UsersMockTable([
            User(1, "Alice", "alice@example.com"),
            User(2, "Bob", "bob@example.com")
        ])
    ],
    result_class=UserResult
)
def test_user_query():
    return TestCase(
        query="SELECT user_id, name FROM users WHERE user_id = 1",
        default_namespace="test_db"
    )
```

## 🏗️ Supported Databases

| Database | CTE Mode | Physical Tables | Query Size Limit |
|----------|----------|-----------------|------------------|
| **BigQuery** | ✅ | ✅ | ~1MB |
| **Athena** | ✅ | ✅ | 256KB |
| **Redshift** | ✅ | ✅ | 16MB |
| **Trino** | ✅ | ✅ | ~16MB |
| **Snowflake** | ✅ | ✅ | 1MB |
| **DuckDB** | ✅ | ✅ | No limit |
| **ClickHouse** | ✅ | ✅ | No hard limit |

### Data Types Support

✅ **Supported Types**:
- String
- Integer
- Float
- Boolean
- Date
- Datetime
- Decimal
- Arrays
- Map/Dict types (Dict[K, V])
- Optional/Nullable types
- Struct/Record types (Athena/Trino/BigQuery/DuckDB/ClickHouse - using dataclasses or Pydantic models; ClickHouse uses native named `Tuple`)

❌ **Not Yet Supported**:
- Struct/Record types for Redshift and Snowflake
- Nested Arrays (arrays of arrays) on BigQuery

## 📚 Documentation

<div class="grid">
  <div class="col-4 col-md-4 col-lg-4">
    <a href="getting-started" class="card">
      <div class="card-body">
        <h3>Getting Started</h3>
        <p>Installation, configuration, and your first test</p>
      </div>
    </a>
  </div>
  <div class="col-4 col-md-4 col-lg-4">
    <a href="adapters" class="card">
      <div class="card-body">
        <h3>Database Adapters</h3>
        <p>Configure and use different database engines</p>
      </div>
    </a>
  </div>
  <div class="col-4 col-md-4 col-lg-4">
    <a href="api-reference" class="card">
      <div class="card-body">
        <h3>API Reference</h3>
        <p>Complete reference for all classes and methods</p>
      </div>
    </a>
  </div>
  <div class="col-4 col-md-4 col-lg-4">
    <a href="mocksmith_integration" class="card">
      <div class="card-body">
        <h3>Mocksmith Integration</h3>
        <p>Generate realistic test data automatically</p>
      </div>
    </a>
  </div>
</div>

## 🚀 Getting Started

### Installation

```bash
# Install with specific database support
pip install sql-testing-library[bigquery]
pip install sql-testing-library[athena]
pip install sql-testing-library[redshift]
pip install sql-testing-library[trino]
pip install sql-testing-library[snowflake]
pip install sql-testing-library[duckdb]
pip install sql-testing-library[clickhouse]

# Or install with all database adapters
pip install sql-testing-library[all]
```

### Configuration

Create a `pytest.ini` file in your project root:

```ini
[sql_testing]
adapter = bigquery  # Choose your database

[sql_testing.bigquery]
project_id = my-test-project
dataset_id = test_dataset
credentials_path = path/to/credentials.json
```

### Write Your First Test

```python
@sql_test
def test_simple_query():
    return TestCase(
        query="SELECT 1 as value",
        result_class=dict
    )
```

## 🤝 Contributing

Contributions are welcome! Please check out our [Contributing Guide](https://github.com/gurmeetsaran/sqltesting/blob/master/CONTRIBUTING.md) for details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/gurmeetsaran/sqltesting/blob/master/LICENSE) file for details.

## 🙏 Acknowledgments

Built with ❤️ by the data engineering community. Special thanks to all [contributors](https://github.com/gurmeetsaran/sqltesting/graphs/contributors).

---

## 🤔 Frequently Asked Questions

### How do I unit test SQL queries in Python?

Use SQL Testing Library with pytest to write unit tests for SQL queries. The library injects mock data via CTEs or temporary tables, allowing you to test query logic without accessing real databases. Perfect for testing BigQuery, Snowflake, Redshift, Athena, Trino, DuckDB, and ClickHouse queries.

### Can I test BigQuery SQL queries without a BigQuery account?

Yes! SQL Testing Library creates temporary mock data within your BigQuery project, so you only need access to a test project. No production data needed. The library works with BigQuery's free tier.

### How do I test Snowflake SQL queries locally?

Configure the Snowflake adapter in `pytest.ini` with your test warehouse credentials. The library creates temporary tables that auto-cleanup after each test. Alternatively, use DuckDB adapter for fully local testing with similar SQL syntax.

### What's the best way to test ETL pipelines?

Use SQL Testing Library to test individual SQL transformations with controlled mock data. This approach is faster and more reliable than end-to-end integration tests. Write pytest tests for each transformation step in your data pipeline.

### Can I use this for data validation testing?

Absolutely! SQL Testing Library is perfect for testing data validation rules, business logic, and quality checks written in SQL. Test assertions, constraints, and transformations with type-safe mock data using Python dataclasses or Pydantic models.

### Does this work with dbt?

Yes! You can test dbt SQL models by extracting the compiled SQL and testing it with SQL Testing Library. This provides unit-level testing for your dbt transformations before running full dbt tests.

### How do I test SQL queries across multiple databases?

Write your test once and use different adapters (BigQuery, Snowflake, Redshift, etc.) by specifying `adapter_type` in the `@sql_test` decorator. Perfect for testing query compatibility when migrating between cloud databases.

---

<div class="text-center">
  <a href="https://github.com/gurmeetsaran/sqltesting" class="btn btn-outline">View on GitHub</a>
  <a href="https://pypi.org/project/sql-testing-library/" class="btn btn-outline">View on PyPI</a>
</div>
