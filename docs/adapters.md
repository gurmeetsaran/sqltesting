---
layout: default
title: Database Adapters - Configuration Guide
nav_order: 3
description: "Configure SQL Testing Library for BigQuery, Snowflake, Redshift, Athena, Trino, and DuckDB. Database adapter setup and connection configuration guide."
---

# Database Adapters Configuration
{: .no_toc }

Configure database adapters for SQL unit testing with BigQuery, Snowflake, Redshift, Athena, Trino, and DuckDB.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

The SQL Testing Library supports multiple database engines through adapters. Each adapter handles the specific SQL dialect, connection management, and data type conversions for its database.

## Supported Databases

| Database | Adapter Class | Required Package | SQL Dialect |
|----------|---------------|------------------|-------------|
| BigQuery | `BigQueryAdapter` | `google-cloud-bigquery` | BigQuery Standard SQL |
| Athena | `AthenaAdapter` | `boto3` | Presto/Trino SQL |
| Redshift | `RedshiftAdapter` | `psycopg2-binary` | PostgreSQL-based |
| Trino | `TrinoAdapter` | `trino` | Trino SQL |
| Snowflake | `SnowflakeAdapter` | `snowflake-connector-python` | Snowflake SQL |
| DuckDB | `DuckDBAdapter` | `duckdb` | DuckDB SQL |

## BigQuery Adapter

### Installation

```bash
pip install sql-testing-library[bigquery]
```

### Configuration

```ini
[sql_testing.bigquery]
project_id = my-gcp-project
dataset_id = test_dataset
credentials_path = /path/to/service-account.json
# Optional: use application default credentials
# Leave credentials_path empty to use ADC
```

### Features

- **CTE Creation**: Uses UNION ALL pattern for compatibility with complex data types
- **Array Support**: Full support for ARRAY types using `[element1, element2]` syntax
- **Dict/Map Support**: Full support for Dict types stored as JSON strings
- **Decimal Handling**: Automatic conversion for NUMERIC/DECIMAL types
- **Query Limits**: ~1MB for CTE mode before switching to physical tables

### Database Context

BigQuery uses project and dataset: `project_id.dataset_id`

```python
class MyMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "my-project.my_dataset"
```

### Example

```python
from decimal import Decimal
from datetime import date

@dataclass
class Transaction:
    id: int
    amount: Decimal
    date: date
    tags: List[str]
    metadata: Dict[str, str]  # Dict support added

# BigQuery handles arrays, decimals, and dicts (as JSON)
transactions = TransactionsMockTable([
    Transaction(1, Decimal("99.99"), date(2024, 1, 1),
                ["online", "credit"], {"status": "completed", "region": "US"}),
    Transaction(2, Decimal("149.50"), date(2024, 1, 2),
                ["store", "debit"], {"status": "pending", "region": "EU"})
])
```

## Athena Adapter

### Installation

```bash
pip install sql-testing-library[athena]
```

### Configuration

```ini
[sql_testing.athena]
database = test_database
s3_output_location = s3://my-athena-results/
region = us-west-2
# Optional AWS credentials (uses boto3 defaults if not specified)
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

### Features

- **S3 Integration**: Results stored in S3
- **Presto SQL**: Uses Presto/Trino SQL dialect
- **Query Limits**: 256KB limit for CTE mode
- **External Tables**: Physical tables backed by S3 data
- **Struct/ROW Types**: Full support for nested structures using dataclasses or Pydantic models
- **Map Types**: Native MAP support for Dict[K, V] types

### Database Context

Athena uses single database name: `database`

```python
class MyMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "my_database"
```

### Important Notes

- **S3 Cleanup**: When using physical tables, table metadata is cleaned but S3 data files remain
- **IAM Permissions**: Requires S3 read/write and Athena query permissions
- **Cost**: Queries are billed by data scanned

## Redshift Adapter

### Installation

```bash
pip install sql-testing-library[redshift]
```

### Configuration

```ini
[sql_testing.redshift]
host = my-cluster.region.redshift.amazonaws.com
database = test_db
user = redshift_user
password = redshift_password
port = 5439  # Optional, defaults to 5439
```

### Features

- **PostgreSQL Compatible**: Based on PostgreSQL 8.0.2
- **Temporary Tables**: Automatic cleanup at session end
- **Array Support**: Via SUPER type (JSON parsing)
- **Map Support**: Via SUPER type for Dict[K, V] types
- **Query Limits**: 16MB limit for CTE mode
- **Column Store**: Optimized for analytical queries

### Database Context

Redshift uses single database name: `database`

```python
class MyMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "my_database"
```

### Best Practices

- Use distribution keys for large test tables
- Consider sort keys for time-series data
- Temporary tables are session-scoped

## Trino Adapter

### Installation

```bash
pip install sql-testing-library[trino]
```

### Configuration

```ini
[sql_testing.trino]
host = trino.example.com
port = 8080
user = trino_user
catalog = memory  # Default catalog
schema = default  # Default schema
http_scheme = http  # or https

# Authentication options:
# Basic auth
auth_type = basic
password = my_password

# JWT auth
# auth_type = jwt
# token = eyJhbGc...
```

### Features

- **Memory Catalog**: Default testing catalog
- **Multi-Catalog**: Can query across catalogs
- **Distributed**: Scales across cluster
- **Query Limits**: ~16MB for CTE mode
- **Struct/ROW Types**: Full support for nested structures using dataclasses or Pydantic models
- **Map Types**: Native MAP support for Dict[K, V] types

### Database Context

Trino uses catalog and schema: `catalog.schema`

```python
class MyMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "memory.default"
```

### Testing Tips

- Memory catalog is ideal for testing (no persistence)
- Can test cross-catalog joins
- Supports complex analytical functions

## Snowflake Adapter

### Installation

```bash
pip install sql-testing-library[snowflake]
```

### Configuration

```ini
[sql_testing.snowflake]
account = my-account.us-west-2
user = snowflake_user
database = TEST_DB
schema = PUBLIC
warehouse = COMPUTE_WH
role = DEVELOPER  # Optional

# Authentication (choose one):
# Option 1: Key-pair authentication (recommended for MFA)
private_key_path = /path/to/private_key.pem
# Or use environment variable SNOWFLAKE_PRIVATE_KEY

# Option 2: Password authentication (for accounts without MFA)
password = snowflake_password
```

### Features

- **Case Sensitivity**: Column names normalized to lowercase
- **Temporary Tables**: Session-scoped cleanup
- **Semi-Structured**: Full JSON/VARIANT support including Dict/Map types
- **Map Support**: Dict[K, V] types stored as VARIANT using PARSE_JSON
- **Query Limits**: 1MB for CTE mode
- **Time Travel**: Can query historical data

### Database Context

Snowflake uses database and schema: `database.schema`

```python
class MyMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return "test_db.public"  # lowercase recommended
```

### Known Limitations

- Physical table mode has visibility issues in tests
- Case sensitivity requires careful handling

## DuckDB Adapter

### Installation

```bash
pip install sql-testing-library[duckdb]
```

### Configuration

```ini
[sql_testing.duckdb]
database = :memory:  # Use in-memory database (default)
# Or use a file-based database:
# database = /path/to/database.db
```

### Features

- **In-Memory Database**: Default configuration uses in-memory database (`:memory:`)
- **File-Based Database**: Can use persistent file-based databases
- **Native Complex Types**: Full support for:
  - Arrays: `LIST` type for `List[T]`
  - Maps: `MAP` type for `Dict[K, V]`
  - Structs: `STRUCT` type for dataclasses and Pydantic models
- **JSON Support**: Automatic handling of nested JSON data
- **High Performance**: Optimized for analytical queries
- **No Query Limits**: No inherent query size restrictions
- **SQL Compatibility**: PostgreSQL-compatible SQL with extensions

### Database Context

DuckDB uses simple table names (no schema required by default):

```python
class MyMockTable(BaseMockTable):
    def get_database_name(self) -> str:
        return ""  # No database/schema prefix needed
```

### Example

```python
from decimal import Decimal
from datetime import date
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class Address:
    street: str
    city: str
    zip_code: str

@dataclass
class User:
    id: int
    name: str
    address: Address
    scores: List[int]
    metadata: Dict[str, str]

# DuckDB handles structs, arrays, and maps natively
users = UsersMockTable([
    User(1, "Alice", Address("123 Main St", "NYC", "10001"),
         [85, 92, 78], {"role": "admin", "status": "active"}),
    User(2, "Bob", Address("456 Oak Ave", "LA", "90210"),
         [75, 88, 91], {"role": "user", "status": "pending"})
])
```

### SQL Examples

```sql
-- Struct field access
SELECT
    u.name,
    u.address.city,
    u.address.zip_code
FROM users u
WHERE u.address.city = 'NYC'

-- Array operations
SELECT
    u.name,
    list_avg(u.scores) as avg_score,
    len(u.scores) as num_scores
FROM users u
WHERE list_contains(u.scores, 85)

-- Map operations
SELECT
    u.name,
    u.metadata['role'] as user_role,
    u.metadata['status'] as user_status
FROM users u
WHERE u.metadata['role'] = 'admin'

-- Complex aggregations
SELECT
    u.address.city,
    count(*) as user_count,
    list_avg(list_avg(u.scores)) as city_avg_score
FROM users u
GROUP BY u.address.city
```

### Performance Tips

- Use in-memory database (`:memory:`) for testing (default)
- DuckDB is optimized for analytical workloads
- No need for indexing in test scenarios
- Automatic query optimization and vectorized execution

## Choosing an Adapter

### Default Adapter Configuration

The adapter specified in the `[sql_testing]` section of your configuration file acts as the default for all tests:

```ini
[sql_testing]
adapter = snowflake  # All tests will use Snowflake by default

[sql_testing.snowflake]
account = my-account
user = my_user
# ... other Snowflake settings
```

When you don't specify an `adapter_type` in your `@sql_test` decorator or `TestCase`, the library uses this default adapter.

### For your tests

You can override the default adapter for specific tests:

```python
@sql_test(
    adapter_type="bigquery",  # Override default adapter
    mock_tables=[...],
    result_class=ResultClass
)
def test_bigquery_specific():
    return TestCase(...)
```

### Default adapter

Set in `pytest.ini`:

```ini
[sql_testing]
adapter = redshift  # Default for all tests
```

## Data Type Support by Adapter

### Primitive Types

| Type | BigQuery | Athena | Redshift | Trino | Snowflake | DuckDB |
|------|----------|---------|----------|-------|-----------|---------|
| String | ✅ STRING | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR |
| Integer | ✅ INT64 | ✅ BIGINT | ✅ INTEGER | ✅ BIGINT | ✅ NUMBER | ✅ BIGINT |
| Float | ✅ FLOAT64 | ✅ DOUBLE | ✅ REAL | ✅ DOUBLE | ✅ FLOAT | ✅ DOUBLE |
| Boolean | ✅ BOOL | ✅ BOOLEAN | ✅ BOOLEAN | ✅ BOOLEAN | ✅ BOOLEAN | ✅ BOOLEAN |
| Date | ✅ DATE | ✅ DATE | ✅ DATE | ✅ DATE | ✅ DATE | ✅ DATE |
| Datetime | ✅ DATETIME | ✅ TIMESTAMP | ✅ TIMESTAMP | ✅ TIMESTAMP | ✅ TIMESTAMP | ✅ TIMESTAMP |
| Decimal | ✅ NUMERIC | ✅ DECIMAL | ✅ DECIMAL | ✅ DECIMAL | ✅ NUMBER | ✅ DECIMAL |

### Complex Types

| Type | Python Type | BigQuery | Athena | Redshift | Trino | Snowflake | DuckDB |
|------|-------------|----------|---------|----------|-------|-----------|---------|
| String Array | `List[str]` | ✅ ARRAY | ✅ ARRAY | ✅ JSON | ✅ ARRAY | ✅ ARRAY | ✅ LIST |
| Int Array | `List[int]` | ✅ ARRAY | ✅ ARRAY | ✅ JSON | ✅ ARRAY | ✅ ARRAY | ✅ LIST |
| Decimal Array | `List[Decimal]` | ✅ ARRAY | ✅ ARRAY | ✅ JSON | ✅ ARRAY | ✅ ARRAY | ✅ LIST |
| String Map | `Dict[str, str]` | ✅ JSON | ✅ MAP | ✅ SUPER | ✅ MAP | ✅ VARIANT | ✅ MAP |
| Int Map | `Dict[str, int]` | ✅ JSON | ✅ MAP | ✅ SUPER | ✅ MAP | ✅ VARIANT | ✅ MAP |
| Mixed Map | `Dict[K, V]` | ✅ JSON | ✅ MAP | ✅ SUPER | ✅ MAP | ✅ VARIANT | ✅ MAP |
| Struct | `dataclass` | ✅ STRUCT | ✅ ROW | ❌ | ✅ ROW | ❌ | ✅ STRUCT |
| Struct | `Pydantic model` | ✅ STRUCT | ✅ ROW | ❌ | ✅ ROW | ❌ | ✅ STRUCT |

## Adapter-Specific SQL

### BigQuery

```sql
-- Arrays
SELECT ARRAY[1, 2, 3] as numbers

-- Structs
SELECT STRUCT(1 as id, 'Alice' as name) as user

-- JSON/Dict handling (stored as STRING columns)
-- Python Dict[str, str] is stored as JSON string
SELECT
    JSON_EXTRACT_SCALAR(metadata, '$.status') as status,
    JSON_EXTRACT_SCALAR(metadata, '$.region') as region
FROM transactions

-- Querying JSON data
SELECT *
FROM transactions
WHERE JSON_EXTRACT_SCALAR(metadata, '$.status') = 'completed'

-- Window functions
SELECT *, ROW_NUMBER() OVER (PARTITION BY category) as rn
FROM products
```

### Athena/Trino Complex Type Operations

```sql
-- Arrays with UNNEST
SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(number)

-- Creating Maps
SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]) as my_map

-- Map operations
SELECT
    settings['theme'] as theme_preference,
    MAP_KEYS(user_data) as all_keys,
    MAP_VALUES(user_data) as all_values,
    CARDINALITY(user_data) as map_size
FROM user_preferences
WHERE settings['notifications'] = 'enabled'

-- Lambdas with arrays
SELECT FILTER(ARRAY[1, 2, 3, 4], x -> x > 2) as filtered

-- Complex map types supported by SQL Testing Library
-- Python: Dict[str, str] → SQL: MAP(VARCHAR, VARCHAR)
-- Python: Dict[str, int] → SQL: MAP(VARCHAR, INTEGER/BIGINT)
-- Python: Dict[int, str] → SQL: MAP(INTEGER/BIGINT, VARCHAR)

-- Struct/ROW types (Athena/Trino only)
-- Using named fields with dot notation
SELECT
    employee.name,
    employee.address.city,
    employee.address.zip_code
FROM employees
WHERE employee.salary > 100000
    AND employee.address.state = 'CA'

-- Creating ROW values
SELECT CAST(ROW('John', 30, ROW('123 Main St', 'NYC', '10001'))
    AS ROW(name VARCHAR, age INTEGER, address ROW(street VARCHAR, city VARCHAR, zip VARCHAR)))
    AS person_info
```

### Redshift

```sql
-- JSON arrays via SUPER type
SELECT JSON_PARSE('[1, 2, 3]') as numbers

-- JSON maps via SUPER type
SELECT JSON_PARSE('{"key1": "value1", "key2": "value2"}') as my_map

-- Accessing SUPER elements
SELECT
    my_super_column[0] as first_element,
    my_super_column.field_name as field_value
FROM table_with_super

-- Window functions
SELECT *, RANK() OVER (ORDER BY sales DESC) as rank
FROM sales_data

-- COPY command (not supported in tests)
```

### Snowflake

```sql
-- Semi-structured data
SELECT PARSE_JSON('{"name": "Alice"}') as user_data

-- Dict/Map support via VARIANT type
-- Python Dict[str, str] is stored as VARIANT using PARSE_JSON
SELECT
    metadata:status::STRING as status,
    metadata:region::STRING as region
FROM transactions
WHERE metadata:status = 'completed'

-- Flatten arrays
SELECT VALUE FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3)))

-- Working with VARIANT columns containing maps
SELECT
    user_preferences,
    user_preferences:theme::STRING as theme,
    user_preferences:notifications::BOOLEAN as notifications_enabled
FROM user_settings

-- Time travel
SELECT * FROM users AT(TIMESTAMP => '2024-01-01'::TIMESTAMP)
```

### DuckDB

```sql
-- Arrays/Lists
SELECT [1, 2, 3] as numbers, ['a', 'b', 'c'] as strings

-- List operations
SELECT
    list_contains(my_list, 'target') as contains_target,
    list_avg(scores) as avg_score,
    len(my_list) as list_length
FROM my_table

-- Maps
SELECT MAP {'key1': 'value1', 'key2': 'value2'} as my_map

-- Map operations
SELECT
    my_map['key1'] as value1,
    map_keys(my_map) as all_keys,
    map_values(my_map) as all_values
FROM my_table

-- Structs
SELECT {'name': 'Alice', 'age': 30, 'city': 'NYC'} as person

-- Struct field access
SELECT
    person.name,
    person.age,
    person.address.city
FROM people
WHERE person.age > 25

-- Complex nested operations
SELECT
    u.name,
    u.address.city,
    list_avg(u.scores) as avg_score,
    u.metadata['role'] as user_role
FROM users u
WHERE u.address.city = 'NYC'
    AND list_contains(u.scores, 85)
    AND u.metadata['status'] = 'active'

-- Array aggregations
SELECT
    city,
    count(*) as user_count,
    list_avg(list_avg(scores)) as city_avg_score
FROM (
    SELECT
        u.address.city as city,
        u.scores as scores
    FROM users u
)
GROUP BY city

-- Window functions with complex types
SELECT
    u.name,
    u.address.city,
    list_avg(u.scores) as avg_score,
    row_number() OVER (PARTITION BY u.address.city ORDER BY list_avg(u.scores) DESC) as city_rank
FROM users u
```

## Troubleshooting

### Connection Issues

1. **BigQuery**: Check service account permissions and project access
2. **Athena**: Verify S3 permissions and AWS credentials
3. **Redshift**: Check security groups and network access
4. **Trino**: Ensure correct authentication method
5. **Snowflake**: Verify account identifier format
6. **DuckDB**: Check file permissions for file-based databases

### Query Failures

- Check SQL dialect differences
- Verify data types match database expectations
- Look for case sensitivity issues (especially Snowflake)
- Check query size limits for CTE mode

### Performance

- Use physical tables for large datasets
- Consider partitioning strategies
- Monitor query costs (BigQuery, Athena)
- Use appropriate warehouse size (Snowflake)
