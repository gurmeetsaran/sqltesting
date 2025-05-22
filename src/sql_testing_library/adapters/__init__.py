"""Database adapters for SQL testing library."""

from typing import List


# Import adapters if their dependencies are available
__all__: List[str] = []

try:
    from .bigquery import BigQueryAdapter  # noqa: F401

    __all__.append("BigQueryAdapter")
except ImportError:
    pass

# Optional adapters - these may not be implemented yet,
# but prepare the imports for when they are
try:
    from .athena import AthenaAdapter  # noqa: F401

    __all__.append("AthenaAdapter")
except ImportError:
    pass

try:
    # This import will fail as module does not exist yet
    from . import redshift  # type: ignore # noqa: F401

    __all__.append("RedshiftAdapter")
except ImportError:
    pass
