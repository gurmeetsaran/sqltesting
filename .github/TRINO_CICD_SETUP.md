# Trino CI/CD Integration Setup

This document provides setup instructions for running Trino integration tests in GitHub Actions.

## Overview

Trino integration tests use Docker to run a local Trino server with the Memory connector, providing a completely free testing environment that requires no external cloud credentials or services.

## Architecture

### Docker-Based Testing
- **Trino Server**: Runs in Docker container (`trinodb/trino:457`)
- **Memory Connector**: Built-in connector for in-memory tables
- **No External Dependencies**: No cloud services or credentials needed
- **Fast Execution**: ~2-3 minutes per test run

### Workflow File
- `.github/workflows/trino-integration.yml` - Trino integration tests

## Setup Requirements

### GitHub Repository Configuration

**Secrets Required:** None ✅
**Variables Required:** None ✅
**External Services:** None ✅

This is the simplest integration to set up - no configuration needed!

## Workflow Features

### Automatic Docker Management
```yaml
- name: Start Trino server
  run: |
    docker run -d \
      --name trino \
      -p 8080:8080 \
      trinodb/trino:${{ env.TRINO_VERSION }}

    # Wait for Trino to be ready with health checks
    timeout 120 bash -c '
      until curl -f -s http://localhost:8080/v1/info > /dev/null; do
        echo "Waiting for Trino to be ready..."
        sleep 3
      done
    '
```

### Automatic Configuration
```yaml
- name: Configure Trino tests
  run: |
    # Generate pytest.ini for Trino integration tests
    echo "[sql_testing]" > pytest.ini
    echo "adapter = trino" >> pytest.ini
    echo "host = localhost" >> pytest.ini
    echo "port = 8080" >> pytest.ini
    echo "catalog = memory" >> pytest.ini
    echo "schema = default" >> pytest.ini
    echo "user = test" >> pytest.ini
```

### Built-in Connectivity Testing
```yaml
- name: Test Trino connectivity
  run: |
    # Test basic HTTP connectivity
    response = requests.get('http://localhost:8080/v1/info')

    # Test SQL execution with Memory connector
    from trino.dbapi import connect
    conn = connect(
        host='localhost',
        port=8080,
        user='test',
        catalog='memory',
        schema='default'
    )
    cur = conn.cursor()
    cur.execute('SELECT 1 as test_value')
    result = cur.fetchone()
```

### Retry Mechanism
```yaml
- name: Run Trino integration tests
  run: |
    poetry run pytest tests/integration/test_trino_integration.py \
      -v --tb=short \
      -m "integration and trino" \
      --reruns 2 --reruns-delay 5
```

## Test Coverage

### SQL Features Tested
- **Basic Queries**: SELECT, WHERE, ORDER BY
- **Joins**: INNER, LEFT JOIN operations
- **Aggregations**: COUNT, SUM, AVG, GROUP BY
- **Date Functions**: DATE, TIMESTAMP operations
- **String Functions**: UPPER, LOWER, LENGTH
- **Window Functions**: ROW_NUMBER, RANK
- **Null Handling**: COALESCE, IS NULL
- **Case Statements**: Conditional logic
- **Subqueries**: EXISTS, IN clauses
- **CTEs**: Common Table Expressions

### Data Types Supported
- **Numeric**: BIGINT, DOUBLE, DECIMAL(38,9)
- **String**: VARCHAR
- **Boolean**: BOOLEAN
- **Date/Time**: DATE, TIMESTAMP
- **Nullable**: All types support NULL values

## Workflow Triggers

### Automatic Triggers
The workflow runs automatically when changes are made to:
```yaml
paths:
  - 'src/sql_testing_library/adapters/trino.py'
  - 'src/sql_testing_library/core.py'
  - 'src/sql_testing_library/mock_table.py'
  - 'tests/test_trino*.py'
  - 'tests/integration/test_trino_integration.py'
  - '.github/workflows/trino-integration.yml'
```

### Events
- **Push to master/main**: Always runs
- **Pull Request**: Runs if relevant files changed
- **Manual Trigger**: Available via `workflow_dispatch`
- **Workflow Call**: Can be called by release workflow

## Local Development

### Running Trino Locally
```bash
# Start Trino server
docker run -d -p 8080:8080 --name trino trinodb/trino:457

# Wait for startup (check logs)
docker logs -f trino

# Test connection
curl http://localhost:8080/v1/info

# Run integration tests
pytest tests/integration/test_trino_integration.py -v

# Cleanup
docker stop trino && docker rm trino
```

### Development Workflow
```bash
# 1. Start Trino
docker run -d -p 8080:8080 --name trino trinodb/trino:457

# 2. Install dependencies
poetry install --with dev,trino

# 3. Run specific test
poetry run pytest tests/integration/test_trino_integration.py::TestTrinoIntegration::test_simple_customer_query -v

# 4. Run all Trino tests
poetry run pytest -m "integration and trino" -v

# 5. Cleanup
docker stop trino && docker rm trino
```

## Memory Connector Features

### Table Creation
Tables are created dynamically using `CREATE TABLE AS SELECT`:
```sql
CREATE TABLE memory.default.customers AS
SELECT
  CAST(1 AS BIGINT) AS customer_id,
  'Alice Johnson' AS name,
  'alice@example.com' AS email,
  DATE '2023-01-15' AS signup_date,
  true AS is_premium,
  CAST(1500.00 AS DECIMAL(38,9)) AS lifetime_value
UNION ALL
SELECT 2, 'Bob Smith', 'bob@example.com', DATE '2023-02-20', false, CAST(NULL AS DECIMAL(38,9))
```

### Automatic Cleanup
- Tables are automatically dropped after each test
- Memory connector tables exist only in RAM
- No persistent storage or cleanup needed

### Performance
- **In-Memory**: Extremely fast query execution
- **No I/O**: No disk or network bottlenecks
- **Scalable**: Handles test datasets efficiently

## Troubleshooting

### Common Issues

**Docker Connection Failed:**
```bash
❌ Failed to connect to Trino info endpoint: Connection refused
```
**Solution:** Docker service issue, usually resolves with retry mechanism

**Memory Connector Error:**
```bash
❌ Catalog 'memory' not found
```
**Solution:** Trino not fully started, wait longer or check Docker logs

**Type Conversion Error:**
```bash
❌ Cannot convert type "unknown" to numeric
```
**Solution:** NULL value type casting issue, fixed in adapter

**Permission Denied:**
```bash
❌ Cannot bind to port 8080: Permission denied
```
**Solution:** Port already in use, change port or stop existing process

### Debug Commands

**Check Trino Status:**
```bash
curl -s http://localhost:8080/v1/info | jq '.'
```

**Test Memory Connector:**
```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: test" \
  -H "X-Trino-Catalog: memory" \
  -H "X-Trino-Schema: default" \
  -d "SELECT 1"
```

**View Docker Logs:**
```bash
docker logs trino
```

**Check Running Containers:**
```bash
docker ps | grep trino
```

## Performance Metrics

### Typical Run Times
- **Docker Startup**: ~30 seconds
- **Test Execution**: ~1-2 minutes
- **Total Runtime**: ~2-3 minutes

### Resource Usage
- **Memory**: ~2GB RAM for Trino container
- **CPU**: Minimal CPU usage
- **Network**: Local only, no external traffic
- **Storage**: No persistent storage used

## Comparison with Other Adapters

| Feature | Trino | Athena | BigQuery | Redshift |
|---------|-------|--------|----------|----------|
| **Cost** | FREE | $0.01-0.05 | Variable | $2-5 |
| **Setup** | None | AWS Creds | GCP Creds | AWS Creds |
| **Runtime** | 2-3 min | 3-5 min | 2-4 min | 8-12 min |
| **Dependencies** | Docker only | AWS S3 | GCP Project | AWS VPC |
| **Reliability** | High | Medium | High | Medium |

## SQL Compatibility

### Trino vs Athena
Since Athena is based on Trino (formerly Presto), SQL compatibility is very high:
- Same SQL syntax and functions
- Same data type support
- Same query execution semantics
- Same optimization patterns

### Test Reuse
Trino integration tests are based on Athena tests with minimal changes:
- Same test data and scenarios
- Same SQL queries (with minor syntax adjustments)
- Same expected results
- Same edge cases and error handling

## Advantages of Trino Testing

### Development Benefits
1. **Fast Feedback**: Quick test execution
2. **No Costs**: Completely free testing
3. **No Dependencies**: Works offline
4. **Consistent**: Same environment every time
5. **Debuggable**: Easy to inspect and troubleshoot

### CI/CD Benefits
1. **Reliable**: No external service dependencies
2. **Fast**: Shortest integration test runtime
3. **Scalable**: Can run many instances in parallel
4. **Simple**: No credential management needed
5. **Secure**: No secrets or external access required

## Maintenance

### Regular Updates
```yaml
env:
  TRINO_VERSION: '457'  # Update periodically
```

### Version Management
- Monitor Trino releases for new features
- Test compatibility before updating version
- Update documentation for any breaking changes

### Dependencies
- Docker (managed by GitHub Actions runner)
- Python trino client (managed by Poetry)
- No external service dependencies

## Best Practices

### Test Design
1. **Use Memory Connector**: Fastest and most reliable
2. **Small Datasets**: Keep test data minimal
3. **Deterministic Results**: Ensure consistent test outcomes
4. **Proper Cleanup**: Tests should be independent
5. **Type Safety**: Use explicit type casting

### CI/CD Integration
1. **File-based Triggers**: Only run when relevant files change
2. **Parallel Execution**: Can run alongside other tests
3. **Retry Logic**: Handle transient Docker issues
4. **Timeout Protection**: Prevent hanging builds
5. **Resource Cleanup**: Always stop Docker containers

## Future Enhancements

### Planned Improvements
1. **Multi-Catalog Testing**: Test with multiple connectors
2. **Performance Benchmarks**: Add timing metrics
3. **Advanced SQL**: More complex query patterns
4. **Error Scenarios**: Negative test cases
5. **Load Testing**: Higher volume datasets

### Community Contributions
Trino integration testing can serve as a template for:
- Other Trino deployments
- Different connector configurations
- Custom SQL testing scenarios
- Performance benchmarking suites
