---
layout: default
title: SQL Logging - Debug & Inspect Queries
nav_order: 7
description: "Enable SQL query logging for debugging. View generated queries, temporary tables, and execution details."
---

# SQL Logging Feature
{: .no_toc }

The SQL Testing Library includes a comprehensive SQL logging feature that helps with debugging failed tests and understanding the generated SQL queries.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

When enabled, the SQL logging feature:
- Creates formatted SQL files with the complete transformed query
- Organizes logs by test run with timestamp directories (e.g., `runid_20250603T144523`)
- Includes test metadata (name, class, file, execution time, run ID, etc.)
- Shows mock table information
- Logs both the original and transformed queries
- Captures temp table creation SQL for physical table mode
- Includes full error details and stack traces for failed tests
- **Automatically logs SQL when queries fail due to SQL-level errors** (syntax errors, table not found, etc.)
- **Does NOT automatically log SQL for assertion failures** (use `log_sql=True` or `SQL_TEST_LOG_ALL` to capture these)
- Supports environment variables for logging all tests
- Intelligently finds project root to ensure consistent log location

## Usage

### Quick Reference: When is SQL Logged?

| Scenario | Automatic Logging | How to Enable |
|----------|------------------|---------------|
| **SQL execution error** (syntax error, table not found, etc.) | ✅ Yes | Automatic (unless `log_sql=False`) |
| **Assertion failure** (test expectations not met) | ❌ No | Use `log_sql=True` or `SQL_TEST_LOG_ALL=true` |
| **Successful test** (for debugging) | ❌ No | Use `log_sql=True` or `SQL_TEST_LOG_ALL=true` |
| **All tests in a run** | ❌ No | Use `SQL_TEST_LOG_ALL=true` environment variable |

### 1. Log SQL for Specific Tests

You can enable SQL logging for individual tests using either the decorator or test case parameter:

```python
# Using decorator parameter
@sql_test(log_sql=True)
def test_with_logging() -> SQLTestCase[MyModel]:
    return SQLTestCase(
        query="SELECT * FROM users",
        default_namespace="test_db",
        mock_tables=[...],
        result_class=MyModel,
    )

# Using SQLTestCase parameter
@sql_test()
def test_with_logging() -> SQLTestCase[MyModel]:
    return SQLTestCase(
        query="SELECT * FROM users",
        default_namespace="test_db",
        mock_tables=[...],
        result_class=MyModel,
        log_sql=True,  # Enable logging here
    )
```

### 2. Automatic Logging on SQL-Level Failures

**Important**: SQL is automatically logged **only when queries fail due to SQL-level errors** such as:
- SQL syntax errors
- Table or column not found errors
- Type conversion errors
- Database connection issues
- Other SQL execution failures

**Assertion failures are NOT automatically logged**. If your test fails during assertion (e.g., `assert result.count == 5`), the SQL will not be logged automatically. For these cases, you need to explicitly enable logging:

```python
# Option 1: Enable logging for specific test
@sql_test(log_sql=True)
def test_with_explicit_logging() -> SQLTestCase[MyModel]:
    # SQL will be logged even if only assertions fail
    ...

# Option 2: Use environment variable for all tests
# SQL_TEST_LOG_ALL=true pytest tests/

# To completely disable automatic logging (even for SQL errors):
@sql_test(log_sql=False)
def test_no_logging_ever() -> SQLTestCase[MyModel]:
    # SQL won't be logged even if SQL execution fails
    ...
```

### 3. Log All Tests with Environment Variable

To log SQL for all tests in a test run, set the `SQL_TEST_LOG_ALL` environment variable:

```bash
# Log SQL for all tests
SQL_TEST_LOG_ALL=true pytest tests/

# Or use 1 or yes
SQL_TEST_LOG_ALL=1 pytest tests/
SQL_TEST_LOG_ALL=yes pytest tests/
```

### 4. Understanding SQL-Level vs Assertion Failures

Here's a practical example to illustrate the difference:

```python
from sql_testing_library import sql_test, TestCase

# Example 1: SQL-level failure - SQL IS LOGGED AUTOMATICALLY
@sql_test()
def test_sql_error():
    return TestCase(
        query="SELECT * FROM non_existent_table",  # Table doesn't exist
        default_namespace="test_db",
        mock_tables=[...],
        result_class=MyModel,
    )
# Result: SQL is automatically logged because the query execution fails
# Error: "relation 'non_existent_table' does not exist"

# Example 2: Assertion failure - SQL IS NOT LOGGED AUTOMATICALLY
@sql_test()
def test_assertion_failure():
    return TestCase(
        query="SELECT COUNT(*) as count FROM users",
        default_namespace="test_db",
        mock_tables=[users_mock],
        result_class=MyModel,
        assertions=[
            lambda result: result[0].count == 5  # Assertion fails if count != 5
        ]
    )
# Result: SQL is NOT logged automatically if assertion fails
# You need log_sql=True or SQL_TEST_LOG_ALL=true to see the SQL

# Example 3: Enable logging for assertion failures
@sql_test(log_sql=True)  # Now SQL will be logged
def test_assertion_with_logging():
    return TestCase(
        query="SELECT COUNT(*) as count FROM users",
        default_namespace="test_db",
        mock_tables=[users_mock],
        result_class=MyModel,
        assertions=[
            lambda result: result[0].count == 5
        ]
    )
# Result: SQL is logged regardless of success or failure
```

**Key Takeaway**: If your test query executes successfully but your assertions fail, you need to explicitly enable logging to debug the SQL. Use `log_sql=True` on the test or set `SQL_TEST_LOG_ALL=true` for all tests.

### 5. SQL Log Files

SQL files are created in the `.sql_logs` directory at the project root, organized by test run:

```
<project_root>/.sql_logs/
└── runid_20250603T144523/
├── test_module__TestClass__test_method__20240115_143022_123.sql
├── test_module__test_function__FAILED__20240115_143025_456.sql
└── ...
```

The filename includes:
- Test module name
- Test class name (if applicable)
- Test method/function name
- FAILED indicator (for failed tests)
- Timestamp with milliseconds

**Note**: The SQL logger automatically detects your project root by looking for files like `pyproject.toml`, `setup.py`, or `.git` directory. This ensures logs are always written to the same location regardless of where you run tests from (e.g., PyCharm, command line, subdirectories).

### 6. SQL File Contents

Each SQL file contains:

```sql
-- SQL Test Case Log
-- ==============================================================================
-- Generated: 2024-01-15T14:30:22.123456
-- Run ID: runid_20240115T143022
-- Test Name: test_user_aggregation
-- Test Class: TestUserQueries
-- Test File: tests/test_users.py
-- Adapter: bigquery
-- Default Namespace: analytics_db
-- Use Physical Tables: False
-- Execution Time: 0.123 seconds
-- Result Rows: 5
-- Status: SUCCESS

-- Mock Tables:
-- ------------------------------------------------------------------------------
-- Table: users
--   Rows: 10
--   Columns: user_id, name, created_date

-- Original Query:
-- ------------------------------------------------------------------------------
-- SELECT user_id, COUNT(*) as count
-- FROM users
-- GROUP BY user_id

-- Transformed Query:
-- ==============================================================================

WITH users AS (
  SELECT * FROM (
    SELECT 1 as user_id, 'Alice' as name, '2024-01-01' as created_date
    UNION ALL
    SELECT 2 as user_id, 'Bob' as name, '2024-01-02' as created_date
    -- ... more mock data
  )
)
SELECT user_id, COUNT(*) as count
FROM users
GROUP BY user_id
```

#### For Physical Table Mode

When using `use_physical_tables=True`, the log includes temp table creation queries:

```sql
-- Temporary Table Creation Queries:
-- ------------------------------------------------------------------------------

-- Query 1:

CREATE TEMPORARY TABLE "temp_users_1748847357874" AS
SELECT 1 AS "user_id", 'Alice' AS "name", '2024-01-01' AS "created_date"
UNION ALL SELECT 2, 'Bob', '2024-01-02'

-- Transformed Query:
-- ==============================================================================
SELECT user_id, COUNT(*) as count
FROM temp_users_1748847357874
GROUP BY user_id
```

#### For Failed Tests

Failed tests include complete error details and stack traces:

```sql
-- Status: FAILED
-- Error: relation "users" does not exist

-- Full Error Details:
-- ------------------------------------------------------------------------------
-- Traceback (most recent call last):
--   File "/path/to/_core.py", line 131, in run_test
--     result_df = self.adapter.execute_query(final_query)
--   File "/path/to/postgres.py", line 89, in execute_query
--     cursor.execute(query)
-- psycopg2.errors.UndefinedTable: relation "users" does not exist
```

## Configuration

### Log Directory

By default, SQL files are saved to the `.sql_logs` directory at your project root. The logger automatically detects the project root by looking for:
- `pyproject.toml`
- `setup.py`
- `setup.cfg`
- `tox.ini`
- `.git` directory

This ensures logs are always written to the same location regardless of where tests are run from.

### Run Directory Organization

Each test run creates a new timestamped directory:
- Directory name format: `runid_YYYYMMDDTHHMMSS` (e.g., `runid_20250603T144523`)
- All SQL logs from a single test session are grouped together
- The run directory is created when the first SQL log is written
- The run ID is displayed at the start of logging

Example directory structure after multiple test runs:
```
.sql_logs/
├── runid_20250603T144523/
│   ├── test_user_query__20250603_144524_123.sql
│   └── test_order_query__FAILED__20250603_144525_456.sql
├── runid_20250603T152031/
│   ├── test_auth__TestLogin__test_valid_login__20250603_152032_789.sql
│   └── test_auth__TestLogin__test_invalid_login__20250603_152033_012.sql
└── runid_20250603T160145/
    └── test_integration__20250603_160146_345.sql
```

You can customize the log directory in several ways:

#### 1. Environment Variable

```bash
# Set custom log directory
export SQL_TEST_LOG_DIR=/path/to/my/logs
pytest tests/
```

#### 2. Custom SQLLogger Instance

```python
from sql_testing_library._sql_logger import SQLLogger

# Custom log directory
sql_logger = SQLLogger(log_dir="custom/sql/logs")

# Pass to framework
framework = SQLTestFramework(adapter, sql_logger=sql_logger)
```

**Note**: The `.sql_logs` directory is automatically added to `.gitignore` to prevent test logs from being committed to version control.

## Best Practices

1. **Development**: Enable `SQL_TEST_LOG_ALL` during development to see all generated queries
2. **CI/CD**: Rely on automatic SQL-level failure logging in CI/CD pipelines. For debugging assertion failures, use `log_sql=True` on specific tests
3. **Debugging Assertion Failures**: Always use `log_sql=True` when debugging tests that fail on assertions, as these are not automatically logged
4. **Debugging SQL Errors**: SQL-level errors are automatically logged, no need to explicitly enable logging
5. **Performance**: Disable logging in production test runs with `log_sql=False` if needed

## Troubleshooting

### SQL files not being created

1. **Check the failure type**: SQL is only automatically logged for SQL-level errors (syntax errors, table not found). If your test fails during assertions, you need to explicitly enable logging with `log_sql=True` or `SQL_TEST_LOG_ALL=true`
2. Check that the test is actually running (not skipped)
3. Verify write permissions for the `.sql_logs` directory
4. For specific tests, ensure `log_sql=True` is set
5. For all tests, ensure `SQL_TEST_LOG_ALL` environment variable is set correctly
6. The directory is hidden (starts with a dot), so use `ls -la` to see it

### Console output not showing

The console will show the SQL file location:
- For SQL-level failures (syntax errors, table not found, etc.) - automatic (unless `log_sql=False`)
- For assertion failures - only when `log_sql=True` or `SQL_TEST_LOG_ALL` is set
- For successful tests - only when `log_sql=True` or `SQL_TEST_LOG_ALL` is set

### Large SQL files

For tests with large mock datasets, the SQL files can become quite large. Consider:
- Using smaller mock datasets for tests
- Enabling physical tables mode for very large datasets
- Compressing old SQL log files
