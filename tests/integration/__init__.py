"""Integration tests for SQL Testing Library.

This module contains integration tests that require real database connections
and may incur costs when run against cloud providers.

Test Organization:
- test_athena_integration.py: AWS Athena integration tests
- (Future) test_bigquery_integration.py: Google BigQuery integration tests
- (Future) test_redshift_integration.py: AWS Redshift integration tests
- (Future) test_trino_integration.py: Trino integration tests

All tests in this module should be marked with @pytest.mark.integration
and the appropriate adapter marker (e.g., @pytest.mark.athena).
"""
