# Integration Tests

This directory contains integration tests that require real database connections and may incur costs when run against cloud providers.

## Test Organization

### Current Tests
- **`test_athena_integration.py`**: AWS Athena integration tests

### Future Tests
- **`test_bigquery_integration.py`**: Google BigQuery integration tests
- **`test_redshift_integration.py`**: AWS Redshift integration tests
- **`test_trino_integration.py`**: Trino integration tests

## Running Integration Tests

### All Integration Tests
```bash
# Run all integration tests (may incur costs)
pytest tests/integration/ -v -m integration

# Run with specific adapter
pytest tests/integration/ -v -m "integration and athena"
```

### Individual Test Files
```bash
# Athena integration tests only
pytest tests/integration/test_athena_integration.py -v

# Skip slow tests
pytest tests/integration/ -v -m "integration and not slow"
```

## Test Requirements

### Environment Variables
Each adapter requires specific environment variables. See the main documentation for setup instructions:

- **Athena**: See [Athena CI/CD Setup](../../.github/ATHENA_CICD_SETUP.md)
- **BigQuery**: `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT_ID`
- **Redshift**: `REDSHIFT_HOST`, `REDSHIFT_USER`, etc.
- **Trino**: `TRINO_HOST`, `TRINO_USER`, etc.

### Markers
All integration tests must use appropriate pytest markers:

```python
@pytest.mark.integration
@pytest.mark.athena  # or bigquery, redshift, trino
def test_my_integration():
    # Test implementation
```

## Cost Considerations

⚠️ **Integration tests may incur costs** when run against real cloud providers:

- **Athena**: ~$0.01-0.05 per test run
- **BigQuery**: Varies by query complexity
- **Redshift**: Cluster time charges
- **Trino**: Depends on underlying storage

### Cost Control
- Use markers to run specific tests: `-m "athena and not slow"`
- Set timeouts: `--timeout=300`
- Limit failures: `--maxfail=3`
- Run locally only when needed

## CI/CD Integration

Integration tests are automatically run by GitHub Actions:

- **Unit Tests Workflow**: Skips integration tests (`-m "not integration"`)
- **Adapter-Specific Workflows**: Run only relevant integration tests
- **Manual Triggers**: Available for cost control

See the [main CI/CD documentation](../../.github/ATHENA_CICD_SETUP.md) for more details.
