"""Type conversion utilities."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Type, TypeVar, Union, cast, get_args, get_origin


T = TypeVar("T")


class BaseTypeConverter:
    """Base type converter with common conversion logic."""

    @staticmethod
    def is_optional_type(type_hint: Type) -> bool:
        """Check if a type is Optional[T] (Union[T, None])."""
        origin = get_origin(type_hint)
        if origin is not None:
            args = get_args(type_hint)
            return len(args) == 2 and type(None) in args
        return False

    @staticmethod
    def get_optional_inner_type(type_hint: Type) -> Type:
        """Extract T from Optional[T]."""
        args = get_args(type_hint)
        inner_type: Type = next(arg for arg in args if arg is not type(None))
        return inner_type

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert value to target type."""
        # Handle None/NULL values
        if value is None:
            return None

        # Handle Optional types
        if self.is_optional_type(target_type):
            if value is None:
                return None
            target_type = self.get_optional_inner_type(target_type)

        # Handle array/list types
        if hasattr(target_type, "__origin__") and target_type.__origin__ is list:
            # If value is already a list, return it
            if isinstance(value, list):
                return value

            # Handle numpy arrays (BigQuery returns arrays as numpy arrays)
            import numpy as np

            if isinstance(value, np.ndarray):
                # Convert numpy array to list, then process each element
                elements = value.tolist()
                # Get the element type from List[T]
                element_type = get_args(target_type)[0] if get_args(target_type) else str
                # Convert each element to the proper type
                converted_elements = []
                for element in elements:
                    converted_element = self.convert(element, element_type)
                    converted_elements.append(converted_element)
                return converted_elements

            # If value is a string representation of an array, parse it
            if isinstance(value, str):
                # Parse string array format like '[hello, world, athena]' or '[1, 2, 3]'
                # Remove outer brackets and split by comma
                if value.startswith("[") and value.endswith("]"):
                    inner_value = value[1:-1].strip()
                    if not inner_value:  # Empty array
                        return []

                    # Split by comma and clean up each element
                    elements = [elem.strip() for elem in inner_value.split(",")]

                    # Get the element type from List[T]
                    element_type = get_args(target_type)[0] if get_args(target_type) else str

                    # Convert each element to the proper type
                    converted_elements = []
                    for element in elements:
                        # Remove quotes if present (for string elements)
                        if element.startswith(("'", '"')) and element.endswith(("'", '"')):
                            element = element[1:-1]

                        # Recursively convert each element
                        converted_element = self.convert(element, element_type)
                        converted_elements.append(converted_element)

                    return converted_elements
                else:
                    # If it doesn't look like array format, try to convert as single element list
                    element_type = get_args(target_type)[0] if get_args(target_type) else str
                    converted_element = self.convert(value, element_type)
                    return [converted_element]

            # For other types, try to convert to list
            return [value] if value is not None else []

        # Handle basic types
        if target_type is str:
            return str(value)
        elif target_type is int:
            if isinstance(value, str):
                return int(float(value))  # Handle "123.0" -> 123
            return int(value)
        elif target_type is float:
            return float(value)
        elif target_type is bool:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "t")
            return bool(value)
        elif target_type == Decimal:
            return Decimal(str(value))
        elif target_type == date:
            if isinstance(value, str):
                return datetime.fromisoformat(value).date()
            elif isinstance(value, datetime):
                return value.date()
            return value
        elif target_type == datetime:
            if isinstance(value, str):
                return datetime.fromisoformat(value)
            return value
        else:
            # For unsupported types, convert to string
            return str(value)


def unwrap_optional_type(col_type: Type[Any]) -> Type[Any]:
    """Unwrap Optional[T] to T, leave other types unchanged.

    This is a utility function that can be used by adapters and mock tables
    to handle Optional types consistently.
    """
    # Check if this is a Union type (which Optional[T] is)
    if get_origin(col_type) is Union:
        args = get_args(col_type)
        # Optional[T] is Union[T, None], so filter out NoneType
        non_none_types = [arg for arg in args if arg is not type(None)]
        if non_none_types:
            return cast(Type[Any], non_none_types[0])  # Return the first non-None type
    return col_type
