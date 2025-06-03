"""Mock table base class and utilities."""

from abc import ABC, abstractmethod
from dataclasses import is_dataclass
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
    get_type_hints,
)


if TYPE_CHECKING:
    import pandas as pd

# Heavy import moved to function level for better performance
from ._types import unwrap_optional_type


try:
    from pydantic import BaseModel

    pydantic_available = True
except ImportError:
    BaseModel = None  # type: ignore
    pydantic_available = False


def _is_pydantic_model(obj: Any) -> bool:
    """Check if an object is a Pydantic model instance."""
    if not pydantic_available or BaseModel is None:
        return False
    return isinstance(obj, BaseModel)


class BaseMockTable(ABC):
    """Base class for mock table implementations."""

    def __init__(self, data: List[Any]) -> None:
        """
        Initialize mock table with data.

        Args:
            data: List of dataclass instances, Pydantic models, or dictionaries
        """
        # Store the original model type if available for type hints
        self._original_model_class: Optional[Type[Any]]
        if data and (is_dataclass(data[0]) or _is_pydantic_model(data[0])):
            self._original_model_class = type(data[0])
        else:
            self._original_model_class = None

        self.data = self._normalize_data(data)

    def _normalize_data(self, data: List[Any]) -> List[Dict[str, Any]]:
        """Convert dataclass instances or Pydantic models to dictionaries."""
        if not data:
            return []

        first_item = data[0]
        if is_dataclass(first_item):
            return [self._dataclass_to_dict(item) for item in data]
        elif _is_pydantic_model(first_item):
            return [self._pydantic_to_dict(item) for item in data]
        return data

    def _dataclass_to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert dataclass instance to dictionary."""
        if is_dataclass(obj):
            result: Dict[str, Any] = {
                field.name: getattr(obj, field.name) for field in obj.__dataclass_fields__.values()
            }
            return result
        return obj  # type: ignore  # This should be a dict already

    def _pydantic_to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert Pydantic model instance to dictionary."""
        if _is_pydantic_model(obj):
            result = obj.model_dump()
            return result  # type: ignore[no-any-return]
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
        Extract column types from dataclass/Pydantic model type hints or infer from pandas dtypes.
        Returns a mapping of column name to Python type.
        """
        if not self.data:
            return {}

        # Try to get types from model class type hints first
        if hasattr(self, "_original_model_class") and self._original_model_class:
            type_hints = get_type_hints(self._original_model_class)
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

    def to_dataframe(self) -> "pd.DataFrame":
        """Convert mock data to pandas DataFrame."""
        import pandas as pd

        if not self.data:
            return pd.DataFrame()

        df = pd.DataFrame(self.data)

        # STEP 2: Convert pandas-generated NaN values to Python None
        #
        # WHY THIS IS NEEDED:
        # - Original data may contain explicit None values for optional fields
        # - pandas DataFrame creation may convert None → NaN during dtype inference
        # - Subsequent nullable dtype conversion (Int64, boolean) may introduce more NaN
        # - We need to preserve None for SQL NULL generation in CTE queries
        #
        # DIFFERENCE FROM core.py NaN HANDLING:
        # - mock_table.py: Converts NaN → None on mock data going INTO SQL queries
        # - core.py: Converts NaN → None on real data coming OUT OF SQL queries
        # - Uses df.where() for efficiency during bulk DataFrame dtype operations
        df = df.where(pd.notnull(df), None)

        # Apply proper nullable types based on model class type hints
        if hasattr(self, "_original_model_class") and self._original_model_class:
            type_hints = get_type_hints(self._original_model_class)

            # Type mapping for nullable pandas dtypes
            type_mapping = {
                int: "Int64",  # Nullable integer
                bool: "boolean",  # Nullable boolean
                str: "object",  # Object type for strings
                Decimal: "object",  # Object type for decimals
            }

            for col_name, col_type in type_hints.items():
                if col_name in df.columns:
                    unwrapped_type = unwrap_optional_type(col_type)
                    target_dtype = type_mapping.get(unwrapped_type)

                    if target_dtype:
                        df[col_name] = df[col_name].astype(target_dtype)

        return df

    def get_cte_alias(self) -> str:
        """Get the CTE alias name (database__tablename)."""
        return f"{self.get_database_name().replace('-', '_').replace('.', '_')}__{self.get_table_name()}"  # noqa: E501
