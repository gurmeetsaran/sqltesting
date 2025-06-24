# Integration Tests

This directory contains integration tests that require real database connections and may incur costs when run against cloud providers.

## Test Organization

### Current Tests
- **`test_athena_integration.py`**: AWS Athena integration tests
- **`test_redshift_integration.py`**: AWS Redshift integration tests
- **`test_bigquery_integration.py`**: Google BigQuery integration tests
- **`test_trino_integration.py`**: Trino integration tests (Docker-based)
- **`test_snowflake_integration.py`**: Snowflake integration tests
- **`test_primitive_types_integration.py`**: Tests for basic data types across all adapters
- **`test_complex_types_integration.py`**: Tests for array types across all adapters
- **`test_map_types_integration.py`**: Tests for MAP types (Athena and Trino only)

## Running Integration Tests

### All Integration Tests
```bash
# Run all integration tests (may incur costs)
pytest tests/integration/ -v -m integration

# Run with specific adapter
pytest tests/integration/ -v -m "integration and athena"
pytest tests/integration/ -v -m "integration and redshift"
pytest tests/integration/ -v -m "integration and trino"
pytest tests/integration/ -v -m "integration and snowflake"
```

### Individual Test Files
```bash
# Athena integration tests only
pytest tests/integration/test_athena_integration.py -v

# Redshift integration tests only
pytest tests/integration/test_redshift_integration.py -v

# Trino integration tests only
pytest tests/integration/test_trino_integration.py -v

# Snowflake integration tests only
pytest tests/integration/test_snowflake_integration.py -v

# Skip slow tests
pytest tests/integration/ -v -m "integration and not slow"
```

## Test Requirements

### Environment Variables
Each adapter requires specific environment variables. See the main documentation for setup instructions:

- **Athena**: See [Athena CI/CD Setup](../../.github/ATHENA_CICD_SETUP.md)
- **BigQuery**: `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT_ID`
- **Redshift**: `REDSHIFT_HOST`, `REDSHIFT_USER`, etc.
- **Trino**: Docker-based (no external credentials needed)
- **Snowflake**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY` (for MFA), `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE` (optional), `SNOWFLAKE_ROLE` (optional)

### Markers
All integration tests must use appropriate pytest markers:

```python
@pytest.mark.integration
@pytest.mark.athena  # or bigquery, redshift, trino, snowflake
def test_my_integration():
    # Test implementation
```

## Cost Considerations

⚠️ **Integration tests may incur costs** when run against real cloud providers:

- **Athena**: ~$0.01-0.05 per test run
- **BigQuery**: Varies by query complexity
- **Redshift**: Cluster time charges
- **Trino**: Free (Docker-based, no cloud costs)
- **Snowflake**: Compute time charges based on warehouse size

### Cost Control
- Use markers to run specific tests: `-m "athena and not slow"`, `-m "trino"`, or `-m "snowflake"`
- Set timeouts: `--timeout=300`
- Limit failures: `--maxfail=3`
- Run locally only when needed
- Retry failed tests automatically: `--reruns 2 --reruns-delay 5`

## Trino Testing

Trino integration tests use Docker for a completely free testing environment:

**Local Development**:
```bash
# Start Trino locally
docker run -d -p 8080:8080 --name trino trinodb/trino:457

# Run tests
pytest tests/integration/test_trino_integration.py -v

# Stop Trino
docker stop trino && docker rm trino
```

**Features**:
- Uses Memory connector (no external storage needed)
- Full SQL feature support (joins, aggregations, window functions)
- Same test cases as Athena (since both are based on Trino/Presto)
- Supports complex types: Arrays and Maps (Dict[K, V])
- Automatic setup and teardown in CI/CD

## CI/CD Integration

Integration tests are automatically run by GitHub Actions:

- **Unit Tests Workflow**: Skips integration tests (`-m "not integration"`)
- **Adapter-Specific Workflows**: Run only relevant integration tests
- **Manual Triggers**: Available for cost control

See the [main CI/CD documentation](../../.github/ATHENA_CICD_SETUP.md) for more details.
