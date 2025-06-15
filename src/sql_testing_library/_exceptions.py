"""Custom exceptions for SQL testing library."""

from typing import Any, List


class SQLTestingError(Exception):
    """Base exception for SQL testing library."""

    pass


class MockTableNotFoundError(SQLTestingError):
    """Raised when a required mock table is not provided."""

    def __init__(self, qualified_table_name: str, available_mocks: List[str]):
        self.qualified_table_name = qualified_table_name
        self.available_mocks = available_mocks
        available_list = ", ".join(available_mocks) if available_mocks else "None"
        super().__init__(
            f"Mock table not found: '{qualified_table_name}'. Available: {available_list}"
        )


class SQLParseError(SQLTestingError):
    """Raised when SQL parsing fails."""

    def __init__(self, query: str, parse_error: str):
        self.query = query
        self.parse_error = parse_error
        super().__init__(f"Failed to parse SQL: {parse_error}")


class QuerySizeLimitExceeded(SQLTestingError):
    """Raised when query size exceeds database limits."""

    def __init__(self, actual_size: int, limit: int, adapter_name: str):
        self.actual_size = actual_size
        self.limit = limit
        self.adapter_name = adapter_name
        super().__init__(
            f"Query size ({actual_size} bytes) exceeds {adapter_name} limit "
            f"({limit} bytes). Consider using use_physical_tables=True"
        )


class TypeConversionError(SQLTestingError):
    """Raised when type conversion fails during result deserialization."""

    def __init__(self, value: Any, target_type: type, column_name: str):
        self.value = value
        self.target_type = target_type
        self.column_name = column_name

        # Handle type name extraction for various type forms
        try:
            type_name = target_type.__name__
        except AttributeError:
            # For types like Optional, Union, etc that don't have __name__
            type_name = str(target_type)

        if column_name:
            message = f"Cannot convert '{value}' to {type_name} for column '{column_name}'"
        else:
            message = f"Cannot convert '{value}' to {type_name}"

        super().__init__(message)
