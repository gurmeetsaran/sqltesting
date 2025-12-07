"""SQL Testing Library - Test SQL queries with mock data injection."""

# Import from private modules (leading underscore indicates internal use)
from ._adapters.base import DatabaseAdapter  # noqa: F401
from ._core import SQLTestCase, SQLTestFramework  # noqa: F401
from ._exceptions import (
    MockTableNotFoundError,  # noqa: F401
    QuerySizeLimitExceeded,  # noqa: F401
    SQLParseError,  # noqa: F401
    SQLTestingError,  # noqa: F401
    TypeConversionError,  # noqa: F401
)
from ._mock_table import BaseMockTable, BigQueryMockTable  # noqa: F401
from ._pytest_plugin import sql_test  # noqa: F401


# Backward compatibility alias
TestCase = SQLTestCase

# Import adapters if their dependencies are available
try:
    from ._adapters.bigquery import (
        BigQueryAdapter,  # noqa: F401  # pyright: ignore[reportUnusedImport]
    )

    __all__ = ["BigQueryAdapter"]
except ImportError:
    __all__ = []

__version__ = "0.20.0"
__all__.extend(
    [
        "SQLTestFramework",
        "TestCase",
        "BaseMockTable",
        "BigQueryMockTable",
        "DatabaseAdapter",
        "sql_test",
        "SQLTestingError",
        "MockTableNotFoundError",
        "SQLParseError",
        "QuerySizeLimitExceeded",
        "TypeConversionError",
    ]
)
