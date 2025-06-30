---
layout: default
title: Database Adapters
nav_order: 3
---

# Database Adapters
{: .no_toc }

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

| Type | BigQuery | Athena | Redshift | Trino | Snowflake |
|------|----------|---------|----------|-------|-----------|
| String | ✅ STRING | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR |
| Integer | ✅ INT64 | ✅ BIGINT | ✅ INTEGER | ✅ BIGINT | ✅ NUMBER |
| Float | ✅ FLOAT64 | ✅ DOUBLE | ✅ REAL | ✅ DOUBLE | ✅ FLOAT |
| Boolean | ✅ BOOL | ✅ BOOLEAN | ✅ BOOLEAN | ✅ BOOLEAN | ✅ BOOLEAN |
| Date | ✅ DATE | ✅ DATE | ✅ DATE | ✅ DATE | ✅ DATE |
| Datetime | ✅ DATETIME | ✅ TIMESTAMP | ✅ TIMESTAMP | ✅ TIMESTAMP | ✅ TIMESTAMP |
| Decimal | ✅ NUMERIC | ✅ DECIMAL | ✅ DECIMAL | ✅ DECIMAL | ✅ NUMBER |

### Complex Types

| Type | Python Type | BigQuery | Athena | Redshift | Trino | Snowflake |
|------|-------------|----------|---------|----------|-------|-----------|
| String Array | `List[str]` | ✅ ARRAY | ✅ ARRAY | ✅ JSON | ✅ ARRAY | ✅ ARRAY |
| Int Array | `List[int]` | ✅ ARRAY | ✅ ARRAY | ✅ JSON | ✅ ARRAY | ✅ ARRAY |
| Decimal Array | `List[Decimal]` | ✅ ARRAY | ✅ ARRAY | ✅ JSON | ✅ ARRAY | ✅ ARRAY |
| String Map | `Dict[str, str]` | ✅ JSON | ✅ MAP | ✅ SUPER | ✅ MAP | ✅ VARIANT |
| Int Map | `Dict[str, int]` | ✅ JSON | ✅ MAP | ✅ SUPER | ✅ MAP | ✅ VARIANT |
| Mixed Map | `Dict[K, V]` | ✅ JSON | ✅ MAP | ✅ SUPER | ✅ MAP | ✅ VARIANT |
| Struct | `dataclass` | ✅ STRUCT | ✅ ROW | ❌ | ✅ ROW | ❌ |
| Struct | `Pydantic model` | ✅ STRUCT | ✅ ROW | ❌ | ✅ ROW | ❌ |

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

## Troubleshooting

### Connection Issues

1. **BigQuery**: Check service account permissions and project access
2. **Athena**: Verify S3 permissions and AWS credentials
3. **Redshift**: Check security groups and network access
4. **Trino**: Ensure correct authentication method
5. **Snowflake**: Verify account identifier format

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
