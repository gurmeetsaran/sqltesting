"""SQL Testing Library - Test SQL queries with mock data injection."""

from .core import SQLTestFramework, TestCase
from .mock_table import BaseMockTable
from .adapters.base import DatabaseAdapter
from .pytest_plugin import sql_test
from .exceptions import (
    SQLTestingError,
    MockTableNotFoundError,
    SQLParseError,
    QuerySizeLimitExceeded,
    TypeConversionError
)

# Import adapters if their dependencies are available
try:
    from .adapters.bigquery import BigQueryAdapter
    __all__ = ["BigQueryAdapter"]
except ImportError:
    __all__ = []

__version__ = "0.1.0"
__all__.extend([
    "SQLTestFramework",
    "TestCase",
    "BaseMockTable",
    "DatabaseAdapter",
    "sql_test",
    "SQLTestingError",
    "MockTableNotFoundError",
    "SQLParseError",
    "QuerySizeLimitExceeded",
    "TypeConversionError"
])
