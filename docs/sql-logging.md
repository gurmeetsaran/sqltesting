---
layout: default
title: SQL Logging
nav_order: 5
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
- Automatically logs SQL for failed tests (unless explicitly disabled)
- Supports environment variables for logging all tests
- Intelligently finds project root to ensure consistent log location

## Usage

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

### 2. Automatic Logging on Failure

By default, SQL is automatically logged when a test fails. This helps with debugging without requiring you to explicitly enable logging. To disable this behavior:

```python
@sql_test(log_sql=False)
def test_no_logging_on_failure() -> SQLTestCase[MyModel]:
    # Even if this test fails, SQL won't be logged
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

### 4. SQL Log Files

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

### 5. SQL File Contents

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
2. **CI/CD**: Rely on automatic failure logging in CI/CD pipelines
3. **Debugging**: Use `log_sql=True` on specific tests when debugging
4. **Performance**: Disable logging in production test runs with `log_sql=False` if needed

## Troubleshooting

### SQL files not being created

1. Check that the test is actually running (not skipped)
2. Verify write permissions for the `.sql_logs` directory
3. For specific tests, ensure `log_sql=True` is set
4. For all tests, ensure `SQL_TEST_LOG_ALL` environment variable is set correctly
5. The directory is hidden (starts with a dot), so use `ls -la` to see it

### Console output not showing

The console will show the SQL file location:
- Always for failed tests (unless `log_sql=False`)
- For successful tests only when `SQL_TEST_LOG_ALL` is set

### Large SQL files

For tests with large mock datasets, the SQL files can become quite large. Consider:
- Using smaller mock datasets for tests
- Enabling physical tables mode for very large datasets
- Compressing old SQL log files
