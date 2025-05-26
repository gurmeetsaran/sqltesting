"""Mock table base class and utilities."""

from abc import ABC, abstractmethod
from dataclasses import is_dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    get_type_hints,
)

import pandas as pd

from .types import unwrap_optional_type


class BaseMockTable(ABC):
    """Base class for mock table implementations."""

    def __init__(self, data: List[Any]) -> None:
        """
        Initialize mock table with data.

        Args:
            data: List of dataclass instances or dictionaries
        """
        # Store the original dataclass type if available for type hints
        self._original_dataclass: Optional[Type[Any]]
        if data and is_dataclass(data[0]):
            self._original_dataclass = type(data[0])
        else:
            self._original_dataclass = None

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
            result: Dict[str, Any] = {
                field.name: getattr(obj, field.name) for field in obj.__dataclass_fields__.values()
            }
            return result
        return obj  # type: ignore  # This should be a dict already

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

    def get_column_types(self) -> Dict[str, Type[Any]]:
        """
        Extract column types from the dataclass type hints or infer from pandas dtypes.
        Returns a mapping of column name to Python type.
        """
        if not self.data:
            return {}

        # Try to get types from dataclass type hints first
        if hasattr(self, "_original_dataclass") and self._original_dataclass:
            type_hints = get_type_hints(self._original_dataclass)
            # Unwrap Optional types (Union[T, None] -> T)
            unwrapped_types = {}
            for col_name, col_type in type_hints.items():
                unwrapped_types[col_name] = unwrap_optional_type(col_type)
            return unwrapped_types

        # Fallback: infer from pandas dtypes (handles nulls better)
        df = self.to_dataframe()
        type_mapping: Dict[str, Type[Any]] = {
            "object": str,
            "int64": int,
            "float64": float,
            "bool": bool,
        }

        column_types: Dict[str, Type[Any]] = {}
        for col_name, dtype in df.dtypes.items():
            dtype_str = str(dtype)

            # Handle special cases
            if dtype_str.startswith("datetime64"):
                from datetime import datetime

                column_types[col_name] = datetime
            elif dtype_str.startswith("timedelta"):
                from datetime import timedelta

                column_types[col_name] = timedelta
            elif dtype_str in type_mapping:
                column_types[col_name] = type_mapping[dtype_str]
            else:
                # For object dtype, try to infer from non-null values
                non_null_values = df[col_name].dropna()
                if not non_null_values.empty:
                    sample_value = non_null_values.iloc[0]
                    column_types[col_name] = type(sample_value)
                else:
                    column_types[col_name] = str  # Default to string

        return column_types

    def to_dataframe(self) -> pd.DataFrame:
        """Convert mock data to pandas DataFrame."""
        return pd.DataFrame(self.data)

    def get_cte_alias(self) -> str:
        """Get the CTE alias name (database__tablename)."""
        return f"{self.get_database_name().replace('-', '_').replace('.', '_')}__{self.get_table_name()}"  # noqa: E501
