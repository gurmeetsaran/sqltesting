---
layout: default
title: Troubleshooting
nav_order: 5
description: "Common issues and solutions for SQL Testing Library. Troubleshoot configuration, connection, and test execution problems."
---

# Troubleshooting
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Common Issues

### Configuration Not Found

**Error**: `No [sql_testing] section found in pytest.ini, setup.cfg, or tox.ini`

**Causes**:
- IDE running tests from wrong directory
- Configuration file in wrong location
- Missing configuration file

**Solutions**:

1. **Set environment variable**:
   ```bash
   export SQL_TESTING_PROJECT_ROOT=/path/to/project
   ```

2. **Create conftest.py** in project root:
   ```python
   import os
   import pytest

   def pytest_configure(config):
       if not os.environ.get('SQL_TESTING_PROJECT_ROOT'):
           project_root = os.path.dirname(os.path.abspath(__file__))
           os.environ['SQL_TESTING_PROJECT_ROOT'] = project_root
   ```

3. **Check working directory** in IDE settings

### Mock Table Not Found

**Error**: `MockTableNotFoundError: Mock table 'users' not found`

**Causes**:
- Table name mismatch
- Database/schema name mismatch
- Case sensitivity issues

**Solutions**:

1. **Verify table names match**:
   ```python
   # In mock table
   def get_table_name(self) -> str:
       return "users"  # Must match SQL query

   # In SQL query
   SELECT * FROM users  # Table name must match
   ```

2. **Check database context**:
   ```python
   # Mock table database
   def get_database_name(self) -> str:
       return "test_db"

   # TestCase namespace
   default_namespace="test_db"  # Must match
   ```

### Query Size Limit Exceeded

**Error**: `QuerySizeLimitExceeded: Query exceeds 256KB limit`

**Automatic handling**: Library switches to physical tables

**Manual solutions**:

1. **Force physical tables**:
   ```python
   @sql_test(use_physical_tables=True)
   def test_large_dataset():
       return TestCase(...)
   ```

2. **Reduce test data size**:
   ```python
   # Instead of 10,000 rows, use representative sample
   mock_data = generate_sample_data(100)
   ```

### Type Conversion Errors

**Error**: `TypeConversionError: Cannot convert 'invalid_date' to date`

**Causes**:
- Data type mismatch
- Invalid date/time formats
- Null handling issues

**Solutions**:

1. **Match Python and SQL types**:
   ```python
   from datetime import date, datetime
   from decimal import Decimal

   @dataclass
   class Transaction:
       amount: Decimal  # Not float
       transaction_date: date  # Not string
       created_at: datetime  # With timezone
   ```

2. **Handle nulls properly**:
   ```python
   from typing import Optional

   @dataclass
   class User:
       email: Optional[str]  # Can be None/NULL
   ```

### Database Connection Issues

#### BigQuery

**Error**: `google.auth.exceptions.DefaultCredentialsError`

**Solutions**:
1. Set credentials path:
   ```ini
   [sql_testing.bigquery]
   credentials_path = /path/to/service-account.json
   ```

2. Use application default credentials:
   ```bash
   gcloud auth application-default login
   ```

#### Athena

**Error**: `botocore.exceptions.NoCredentialsError`

**Solutions**:
1. Configure AWS credentials:
   ```ini
   [sql_testing.athena]
   aws_access_key_id = YOUR_KEY
   aws_secret_access_key = YOUR_SECRET
   ```

2. Use AWS CLI:
   ```bash
   aws configure
   ```

#### Redshift

**Error**: `psycopg2.OperationalError: FATAL: password authentication failed`

**Solutions**:
1. Verify credentials
2. Check network/firewall access
3. Ensure security group allows connections

#### Snowflake

**Error**: `snowflake.connector.errors.DatabaseError: Invalid account identifier`

**Solutions**:
1. Use correct account format:
   ```ini
   [sql_testing.snowflake]
   account = xy12345.us-west-2  # Include region
   ```

### Array Type Issues

**Error**: Arrays not working as expected

**Database-specific solutions**:

1. **BigQuery**:
   ```python
   # NULL arrays become empty arrays
   tags: List[str] = field(default_factory=list)
   ```

2. **Redshift**:
   ```python
   # Arrays via JSON
   tags: List[str]  # Stored as JSON string
   ```

3. **Athena/Trino**:
   ```sql
   -- Use UNNEST for array operations
   SELECT * FROM UNNEST(array_column) AS t(value)
   ```

## Performance Issues

### Slow Test Execution

**Causes**:
- Large datasets in CTE mode
- Network latency
- Inefficient queries

**Solutions**:

1. **Use physical tables for large data**:
   ```python
   @sql_test(use_physical_tables=True)
   ```

2. **Optimize test data**:
   ```python
   # Generate only necessary data
   def create_minimal_test_data():
       return [row for row in data if row.is_relevant]
   ```

3. **Run tests in parallel**:
   ```bash
   pytest -n auto  # Requires pytest-xdist
   ```

### Memory Issues

**Error**: `MemoryError` or slow performance

**Solutions**:

1. **Stream large results**:
   ```python
   # Process results in chunks
   for chunk in pd.read_sql(query, con, chunksize=1000):
       process_chunk(chunk)
   ```

2. **Limit result size**:
   ```sql
   SELECT * FROM large_table LIMIT 1000
   ```

## Debugging Tips

### Use SQL Logging

The SQL Testing Library provides comprehensive SQL logging to help debug test failures:

```bash
# Enable logging for all tests
SQL_TEST_LOG_ALL=true pytest tests/

# Or enable for specific tests
@sql_test(log_sql=True)
def test_with_logging():
    ...
```

SQL logs are saved to `<project_root>/.sql_logs/` and include:
- Complete transformed queries with CTEs or temp table SQL
- Full error messages and stack traces
- Test metadata and execution details

See the [SQL Logging documentation](sql-logging) for more details.

### Enable Verbose Output

```bash
# See generated SQL
pytest -v -s test_file.py

# With captured output
pytest --capture=no
```

### Inspect Generated CTE

```python
@sql_test(mock_tables=[...])
def test_debug():
    test_case = TestCase(
        query="SELECT * FROM users",
        default_namespace="test_db"
    )

    # Print generated query
    print(test_case.query)
    return test_case
```

### Check Adapter Configuration

```python
from sql_testing_library._pytest_plugin import SQLTestDecorator

decorator = SQLTestDecorator()
config = decorator._get_adapter_config("bigquery")
print(config)
```

## Platform-Specific Issues

### GitHub Actions

**Issue**: Tests pass locally but fail in CI

**Solutions**:
1. Use same Python version
2. Set timezone: `TZ=UTC`
3. Check secret/environment variables

### Docker

**Issue**: Configuration not found in container

**Solutions**:
1. Mount config file:
   ```dockerfile
   COPY pytest.ini /app/pytest.ini
   ```

2. Set environment:
   ```dockerfile
   ENV SQL_TESTING_PROJECT_ROOT=/app
   ```

## Getting Help

### Resources

1. [GitHub Issues](https://github.com/gurmeetsaran/sqltesting/issues)
2. [Discussions](https://github.com/gurmeetsaran/sqltesting/discussions)
3. [Stack Overflow](https://stackoverflow.com/questions/tagged/sql-testing-library)

### Debug Information

When reporting issues, include:

```python
import sys
import sql_testing_library

print(f"Python: {sys.version}")
print(f"Library: {sql_testing_library.__version__}")
print(f"Platform: {sys.platform}")

# Adapter versions
try:
    import google.cloud.bigquery
    print(f"BigQuery: {google.cloud.bigquery.__version__}")
except ImportError:
    pass
```

### Minimal Reproducible Example

```python
# test_minimal.py
from sql_testing_library import sql_test, TestCase
from sql_testing_library.mock_table import BaseMockTable

class MinimalMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "test_table"

@sql_test(
    mock_tables=[MinimalMockTable([{"id": 1}])],
    result_class=dict
)
def test_minimal():
    return TestCase(
        query="SELECT * FROM test_table",
        default_namespace="test_db"
    )
```
