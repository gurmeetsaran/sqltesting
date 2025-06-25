"""Type conversion utilities."""

from dataclasses import is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Type, TypeVar, Union, cast, get_args, get_origin, get_type_hints


T = TypeVar("T")


try:
    from pydantic import BaseModel

    pydantic_available = True
except ImportError:
    BaseModel = None  # type: ignore
    pydantic_available = False


def is_pydantic_model_class(cls: Type) -> bool:
    """Check if a class is a Pydantic model class."""
    if not pydantic_available or BaseModel is None:
        return False
    try:
        return issubclass(cls, BaseModel)
    except TypeError:
        # Handle cases where cls is not a class
        return False


def is_struct_type(type_hint: Type) -> bool:
    """Check if a type is a struct type (dataclass or Pydantic model)."""
    # Handle Optional types
    if hasattr(type_hint, "__origin__") and type_hint.__origin__ is Union:
        # Extract the non-None type from Optional[T]
        non_none_types = [arg for arg in get_args(type_hint) if arg is not type(None)]
        if non_none_types:
            type_hint = non_none_types[0]

    return is_dataclass(type_hint) or is_pydantic_model_class(type_hint)


def _get_struct_field_names(struct_type: Type) -> List[str]:
    """Get ordered field names for a struct type (dataclass or Pydantic model)."""
    if is_dataclass(struct_type):
        # For dataclasses, use __dataclass_fields__ to preserve order
        return list(struct_type.__dataclass_fields__.keys())
    elif is_pydantic_model_class(struct_type) and pydantic_available:
        # For Pydantic models, use model_fields
        return list(struct_type.model_fields.keys())
    else:
        # Fallback to type hints order (Python 3.7+ preserves dict order)
        return list(get_type_hints(struct_type).keys())


def _create_struct_instance(struct_type: Type, field_values: Dict[str, Any]) -> Any:
    """Create a struct instance from field values."""
    if is_dataclass(struct_type):
        return struct_type(**field_values)
    elif is_pydantic_model_class(struct_type) and pydantic_available:
        return struct_type(**field_values)
    else:
        # Fallback: try to construct with values or empty
        try:
            return struct_type(**field_values)
        except Exception:
            return struct_type()


def _parse_bracketed_string(
    value: str, open_bracket: str = "{", close_bracket: str = "}"
) -> List[str]:
    """Parse a bracket-delimited string considering nested brackets."""
    if not value.startswith(open_bracket) or not value.endswith(close_bracket):
        return []

    inner_value = value[1:-1].strip()
    if not inner_value:
        return []

    parts = []
    current_part = ""
    bracket_count = 0

    for char in inner_value:
        if char in "{[":
            bracket_count += 1
        elif char in "}]":
            bracket_count -= 1
        elif char == "," and bracket_count == 0:
            parts.append(current_part.strip())
            current_part = ""
            continue
        current_part += char

    if current_part.strip():
        parts.append(current_part.strip())

    return parts


def _parse_string_value(value_str: str) -> Any:
    """Parse a string value to appropriate Python type."""
    value_lower = value_str.lower()

    # Check for boolean values
    if value_lower == "true":
        return True
    elif value_lower == "false":
        return False
    elif value_lower == "null":
        return None

    # Try to parse as number
    try:
        if "." in value_str:
            return float(value_str)
        else:
            return int(value_str)
    except ValueError:
        # Not a number, return as string
        return value_str


def _parse_key_value_pairs(
    pairs: List[str], converter, key_type: Type = str, value_type: Type = str
) -> Dict[str, Any]:
    """Parse key=value pairs and convert them to proper types.

    Args:
        pairs: List of "key=value" strings
        converter: Type converter instance with convert method
        key_type: Expected type for keys
        value_type: Expected type for values (or callable that returns type based on key)

    Returns:
        Dictionary with converted keys and values
    """
    result = {}
    for pair in pairs:
        # Split by first = to handle values that contain =
        parts = pair.split("=", 1)
        if len(parts) == 2:
            key_str, value_str = parts
            key_str = key_str.strip()
            value_str = value_str.strip()

            # Convert key to proper type
            converted_key = converter.convert(key_str, key_type)

            # Get value type - if value_type is a custom callable (not a type), call it with the key
            # Built-in types like int, str, float are callable but shouldn't be called here
            if callable(value_type) and not isinstance(value_type, type):
                actual_value_type = value_type(key_str)
            else:
                actual_value_type = value_type

            # Convert value to proper type
            converted_value = converter.convert(value_str, actual_value_type)
            result[converted_key] = converted_value

    return result


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

        # Handle struct types (dataclass or Pydantic model)
        if is_struct_type(target_type):
            return self._convert_struct(value, target_type)

        # Handle dict/map types
        if hasattr(target_type, "__origin__") and target_type.__origin__ is dict:
            # If value is already a dict, return it
            if isinstance(value, dict):
                return value

            # If value is a string representation of a map, parse it
            if isinstance(value, str):
                # Parse string map format like '{key1=value1, key2=value2}' (Athena/Trino format)
                pairs = _parse_bracketed_string(value)
                if not pairs:
                    return {}

                # Get the key and value types from Dict[K, V]
                type_args = get_args(target_type)
                key_type = type_args[0] if type_args else str
                value_type = type_args[1] if len(type_args) > 1 else str

                return _parse_key_value_pairs(pairs, self, key_type, value_type)

            # For other types, try to convert to dict
            return {} if value is None else {}

        # Handle array/list types
        if hasattr(target_type, "__origin__") and target_type.__origin__ is list:
            # If value is already a list, convert each element
            if isinstance(value, list):
                # Get the element type from List[T]
                element_type = get_args(target_type)[0] if get_args(target_type) else str
                # Convert each element to the proper type
                converted_elements = []
                for element in value:
                    converted_element = self.convert(element, element_type)
                    converted_elements.append(converted_element)
                return converted_elements

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
                # Check if it's an array format
                if value.startswith("[") and value.endswith("]"):
                    # Parse string array format like '[hello, world, athena]' or '[1, 2, 3]'
                    elements = _parse_bracketed_string(value, "[", "]")
                    # Return empty list for empty arrays
                    if not elements:
                        return []
                else:
                    # If it doesn't look like array format, try to convert as single element list
                    element_type = get_args(target_type)[0] if get_args(target_type) else str
                    converted_element = self.convert(value, element_type)
                    return [converted_element]

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

    def _convert_struct(self, value: Any, target_type: Type) -> Any:
        """Convert value to struct type (dataclass or Pydantic model)."""
        # If value is already an instance of the target type, return it
        if isinstance(value, target_type):
            return value

        # Get type hints for the struct
        type_hints = get_type_hints(target_type)

        # If value is a dict, construct the struct from it
        if isinstance(value, dict):
            return _create_struct_instance(target_type, value)

        # If value is a tuple (Athena/Trino returns structs as tuples)
        if isinstance(value, tuple):
            field_names = _get_struct_field_names(target_type)

            # Convert tuple values to dict mapping field names to values
            result = {}
            for i, (field_name, field_value) in enumerate(zip(field_names, value)):
                if i < len(value):  # Ensure we don't go out of bounds
                    field_type = type_hints.get(field_name, str)
                    # Recursively convert the field value
                    converted_value = self.convert(field_value, field_type)
                    result[field_name] = converted_value

            return _create_struct_instance(target_type, result)

        # If value is a string representation of a struct (from SQL query result)
        if isinstance(value, str):
            # Parse Athena/Trino struct format
            if value.startswith("{") and value.endswith("}"):
                inner_value = value[1:-1].strip()
                if not inner_value:  # Empty struct
                    return _create_struct_instance(target_type, {})

                # Check if it's a simple comma-separated format (like Athena returns)
                # e.g., "{John Doe, 30, 75000.5, {123 Main St, New York, 10001}, true}"
                if "=" not in inner_value or inner_value.count(",") > inner_value.count("="):
                    # This looks like positional values, not key=value pairs
                    field_names = _get_struct_field_names(target_type)
                    values = _parse_bracketed_string(value)

                    # Convert values to dict
                    result = {}
                    for field_name, value_str in zip(field_names, values):
                        field_type = type_hints.get(field_name, str)
                        # Handle nested structs
                        if value_str.startswith("{") and value_str.endswith("}"):
                            converted_value = self.convert(value_str, field_type)
                        else:
                            # If the target type is string, use the value as-is
                            # Otherwise, parse the string value to appropriate type
                            if field_type is str:
                                converted_value = value_str
                            else:
                                parsed_value = _parse_string_value(value_str)
                                converted_value = self.convert(parsed_value, field_type)
                        result[field_name] = converted_value

                    return _create_struct_instance(target_type, result)

                # Parse key=value pairs
                pairs = _parse_bracketed_string(value)

                # Parse pairs and get field values
                result = {}
                for pair in pairs:
                    if "=" in pair:
                        key, value_str = pair.split("=", 1)
                        key = key.strip()
                        value_str = value_str.strip()
                        # Get the field type for this key
                        field_type = type_hints.get(key, str)
                        # Parse and convert the value
                        parsed_value = _parse_string_value(value_str)
                        result[key] = self.convert(parsed_value, field_type)

                return _create_struct_instance(target_type, result)

        # Fallback: try to construct with empty values
        return _create_struct_instance(target_type, {})


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
