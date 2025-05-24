"""SQL Testing Library - Test SQL queries with mock data injection."""

from .adapters.base import DatabaseAdapter  # noqa: F401
from .core import SQLTestCase, SQLTestFramework  # noqa: F401
from .exceptions import (
    MockTableNotFoundError,  # noqa: F401
    QuerySizeLimitExceeded,  # noqa: F401
    SQLParseError,  # noqa: F401
    SQLTestingError,  # noqa: F401
    TypeConversionError,  # noqa: F401
)
from .mock_table import BaseMockTable  # noqa: F401
from .pytest_plugin import sql_test  # noqa: F401


# Backward compatibility alias
TestCase = SQLTestCase
# Import adapters if their dependencies are available
try:
    from .adapters.bigquery import BigQueryAdapter

    __all__ = ["BigQueryAdapter"]
except ImportError:
    __all__ = []

__version__ = "0.1.0"
__all__.extend(
    [
        "SQLTestFramework",
        "TestCase",
        "BaseMockTable",
        "DatabaseAdapter",
        "sql_test",
        "SQLTestingError",
        "MockTableNotFoundError",
        "SQLParseError",
        "QuerySizeLimitExceeded",
        "TypeConversionError",
    ]
)
