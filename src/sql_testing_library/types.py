"""Type conversion utilities."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Type, get_args, get_origin


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
        return next(arg for arg in args if arg is not type(None))

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
