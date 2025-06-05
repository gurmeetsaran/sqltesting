"""Base database adapter interface."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, List, Optional, Tuple


if TYPE_CHECKING:
    import pandas as pd

# Heavy import moved to function level for better performance
from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""

    @abstractmethod
    def get_sqlglot_dialect(self) -> str:
        """Return the sqlglot dialect string for this database."""
        pass

    @abstractmethod
    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        pass

    @abstractmethod
    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table with mock data. Returns temp table name."""
        pass

    @abstractmethod
    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create a temporary table and return both table name and SQL.

        Returns:
            Tuple of (temp_table_name, create_table_sql)
        """
        pass

    @abstractmethod
    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Clean up temporary tables."""
        pass

    @abstractmethod
    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format a value for inclusion in a CTE VALUES clause."""
        pass

    def get_type_converter(self) -> BaseTypeConverter:
        """Get the type converter for this adapter. Override for custom conversion."""
        return BaseTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes, or None if no limit."""
        return None
