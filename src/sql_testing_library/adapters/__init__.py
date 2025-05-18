"""Database adapters for SQL testing library."""

# Import adapters if their dependencies are available
__all__ = []

try:
    from .bigquery import BigQueryAdapter  # noqa: F401

    __all__.append("BigQueryAdapter")
except ImportError:
    pass

try:
    from .athena import AthenaAdapter  # noqa: F401

    __all__.append("AthenaAdapter")
except ImportError:
    pass

try:
    from .redshift import RedshiftAdapter  # noqa: F401

    __all__.append("RedshiftAdapter")
except ImportError:
    pass
