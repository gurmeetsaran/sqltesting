"""Database adapters for SQL testing library."""

from typing import List


# Lazy import adapters - only import when explicitly requested
# This prevents loading all heavy database SDKs when just importing the base adapter
__all__: List[str] = []

# Individual adapters can be imported directly:
# from sql_testing_library._adapters.bigquery import BigQueryAdapter
# from sql_testing_library._adapters.athena import AthenaAdapter
# from sql_testing_library._adapters.redshift import RedshiftAdapter
# from sql_testing_library._adapters.trino import TrinoAdapter
# from sql_testing_library._adapters.snowflake import SnowflakeAdapter
