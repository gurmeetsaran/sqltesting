# DuckDB CI/CD Setup

This document describes the CI/CD setup for DuckDB integration tests in the SQL Testing Library.

## Overview

DuckDB is an in-process SQL OLAP database management system. Unlike other adapters that require external database servers, DuckDB runs entirely in-process, making it ideal for testing and CI/CD environments.

## CI/CD Configuration

### GitHub Actions Workflow

The DuckDB integration tests are defined in `.github/workflows/duckdb-integration.yml`.

#### Key Features:
- **No external dependencies**: DuckDB runs in-memory, no server setup required
- **Fast execution**: In-memory database provides excellent performance
- **File-based testing**: Also supports file-based databases for persistence testing
- **Cross-platform support**: Works on Linux, macOS, and Windows

#### Workflow Triggers:
- Pull requests to `master`/`main` branches
- Direct pushes to `master`/`main` branches
- Manual workflow dispatch
- Workflow reuse calls

#### Path-based Triggering:
The workflow only runs when relevant files are modified:
- `src/sql_testing_library/_adapters/duckdb.py`
- `tests/integration/test_duckdb_integration.py`
- `tests/test_duckdb.py`
- Core library files
- Cross-adapter integration tests
- The workflow file itself

### Dependencies

DuckDB integration tests require:
- Python 3.10+
- DuckDB Python package (installed via poetry with `--with dev,duckdb`)
- No external database server

## Test Configuration

### pytest.ini Configuration

The CI/CD automatically generates a `pytest.ini` file with DuckDB-specific settings:

```ini
[sql_testing]
adapter = duckdb
database = :memory:
```

### Test Categories

1. **Basic Operations**: Simple queries, data types, basic SQL functions
2. **Join Operations**: Complex joins between multiple tables
3. **Analytical Functions**: Window functions, aggregations, CTEs
4. **Data Types**: Date/time handling, decimals, arrays, structs
5. **DuckDB-Specific Features**: Array operations, struct handling, analytical functions

## Local Development

### Running Tests Locally

```bash
# Install dependencies
poetry install --with dev,duckdb

# Run all DuckDB integration tests
poetry run pytest tests/integration/ -m "integration and duckdb" -v

# Run specific test modes
poetry run pytest tests/integration/test_duckdb_integration.py::TestDuckDBIntegration::test_simple_customer_query -v

# Run with coverage
poetry run pytest tests/integration/ -m "integration and duckdb" --cov=src/sql_testing_library
```

### Test Data

The integration tests use realistic business data:
- **Customers**: User profiles with demographics and lifetime value
- **Orders**: Transaction records with dates, amounts, and status
- **Products**: Catalog items with categories and pricing

### Execution Modes

Tests run in both execution modes:
- **CTE Mode**: Uses Common Table Expressions (default)
- **Physical Tables Mode**: Creates temporary tables in DuckDB

## Performance Characteristics

- **Startup time**: ~1-2 seconds (no server startup required)
- **Test execution**: Very fast due to in-memory processing
- **Memory usage**: Minimal, scales with data size
- **Parallelization**: Supports parallel test execution

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure DuckDB is installed with `poetry install --with dev,duckdb`
2. **Memory constraints**: DuckDB uses in-memory storage by default
3. **File permissions**: When using file-based databases, ensure write permissions

### Debug Commands

```bash
# Test DuckDB connectivity
poetry run python -c "import duckdb; print(duckdb.connect(':memory:').execute('SELECT 1').fetchone())"

# Check installed version
poetry run python -c "import duckdb; print(duckdb.__version__)"

# Test file database
poetry run python -c "import duckdb; conn = duckdb.connect('test.db'); print('File DB OK'); conn.close()"
```

## Comparison with Other Adapters

| Feature | DuckDB | Trino | BigQuery | Snowflake |
|---------|--------|-------|----------|-----------|
| Server Required | No | Yes | No (Cloud) | No (Cloud) |
| Startup Time | <1s | ~30s | ~5s | ~10s |
| Local Development | Excellent | Good | Limited | Limited |
| CI/CD Complexity | Low | Medium | High | High |
| Data Volume | Memory Limited | Large Scale | Large Scale | Large Scale |

## Future Enhancements

Potential improvements for DuckDB testing:

1. **Extensions Testing**: Test DuckDB extensions (parquet, httpfs, etc.)
2. **File Format Tests**: CSV, JSON, Parquet integration tests
3. **Performance Benchmarks**: Query performance testing
4. **Memory Usage Tests**: Large dataset handling
5. **Concurrent Access**: Multi-connection testing

## Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)
- [SQL Testing Library Documentation](../README.md)
