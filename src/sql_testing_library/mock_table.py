"""Mock table base class and utilities."""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, get_type_hints
from dataclasses import is_dataclass
import pandas as pd


class BaseMockTable(ABC):
    """Base class for mock table implementations."""

    def __init__(self, data: List[Any]):
        """
        Initialize mock table with data.

        Args:
            data: List of dataclass instances or dictionaries
        """
        self.data = self._normalize_data(data)

    def _normalize_data(self, data: List[Any]) -> List[Dict[str, Any]]:
        """Convert dataclass instances to dictionaries."""
        if not data:
            return []

        if is_dataclass(data[0]):
            return [self._dataclass_to_dict(item) for item in data]
        return data

    def _dataclass_to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert dataclass instance to dictionary."""
        if is_dataclass(obj):
            return {
                field.name: getattr(obj, field.name)
                for field in obj.__dataclass_fields__.values()
            }
        return obj

    @abstractmethod
    def get_database_name(self) -> str:
        """Return the database name for this table."""
        pass

    @abstractmethod
    def get_table_name(self) -> str:
        """Return the table name."""
        pass

    def get_qualified_name(self) -> str:
        """Return the fully qualified table name."""
        return f"{self.get_database_name()}.{self.get_table_name()}"

    def get_column_types(self) -> Dict[str, type]:
        """
        Extract column types from the dataclass type hints.
        Returns a mapping of column name to Python type.
        """
        if not self.data:
            return {}

        # Get the original dataclass if available
        sample_item = self.data[0]
        if hasattr(self, '_original_dataclass'):
            return get_type_hints(self._original_dataclass)

        # Fallback: infer from data values
        return {
            key: type(value) for key, value in sample_item.items()
            if value is not None
        }

    def to_dataframe(self) -> pd.DataFrame:
        """Convert mock data to pandas DataFrame."""
        return pd.DataFrame(self.data)

    def get_cte_alias(self) -> str:
        """Get the CTE alias name (database__tablename)."""
        return f"{self.get_database_name().replace('-', '_').replace('.', '_')}__{self.get_table_name()}"
