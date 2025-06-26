---
layout: default
title: SQL Testing Library
nav_order: 1
description: "A Python library for testing SQL queries with mock data injection across multiple database platforms"
permalink: /
---

# SQL Testing Library
{: .fs-9 }

A Python library for testing SQL queries with mock data injection across Athena, BigQuery, Redshift, Trino, and Snowflake.
{: .fs-6 .fw-300 }

[Get started now](getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 } [View on GitHub](https://github.com/gurmeetsaran/sqltesting){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## 🎯 Why SQL Testing Library?

SQL testing in data engineering can be challenging, especially when working with large datasets and complex queries across multiple database platforms. This library addresses the pain points of:

- **Fragile Integration Tests**: Traditional tests that depend on live data break when data changes
- **Slow Feedback Loops**: Running tests against full datasets takes too long for CI/CD
- **Database Engine Upgrades**: UDF semantics and SQL behavior change between database versions, causing silent production failures
- **Database Lock-in**: Tests written for one database don't work on another
- **Complex Setup**: Each database requires different mocking strategies and tooling

## ✨ Key Features

### 🚀 Multi-Database Support
Test your SQL queries across BigQuery, Athena, Redshift, Trino, and Snowflake with a unified API.

### 🎯 Type-Safe Testing
Use Python dataclasses and Pydantic models for type-safe test data and results.

### ⚡ Flexible Execution
Automatically switches between CTE injection and physical tables based on query size.

### 🧪 Pytest Integration
Seamlessly integrates with pytest using the `@sql_test` decorator.

### 📊 Comprehensive Type Support
Supports primitive types, arrays, decimals, dates, optional values, and struct types (Athena/Trino) across databases.

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
- Struct/Record types (Athena/Trino only - using dataclasses or Pydantic models)

❌ **Not Yet Supported**:
- Struct/Record types for BigQuery, Redshift, and Snowflake
- Nested Arrays (arrays of arrays)

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

<div class="text-center">
  <a href="https://github.com/gurmeetsaran/sqltesting" class="btn btn-outline">View on GitHub</a>
  <a href="https://pypi.org/project/sql-testing-library/" class="btn btn-outline">View on PyPI</a>
</div>
