"""SQL utility functions for escaping and formatting values."""

from typing import Any, Type


def escape_sql_string(value: str) -> str:
    """
    Escape a string value for SQL using standard SQL escaping rules.

    This handles:
    - Single quotes (escaped as '')
    - Backslashes (escaped as \\)
    - Control characters (newlines, tabs, etc.)
    - Null bytes (removed)

    Args:
        value: String value to escape

    Returns:
        Properly escaped SQL string literal
    """
    # Remove null bytes (not allowed in SQL strings)
    value = value.replace("\x00", "")

    # Escape control characters that break SQL syntax
    value = value.replace("\\", "\\\\")  # Must be first to avoid double-escaping
    value = value.replace("\n", "\\n")  # Newlines
    value = value.replace("\r", "\\r")  # Carriage returns
    value = value.replace("\t", "\\t")  # Tabs
    value = value.replace("\b", "\\b")  # Backspace
    value = value.replace("\f", "\\f")  # Form feed
    value = value.replace("\v", "\\v")  # Vertical tab

    # Escape single quotes (standard SQL)
    value = value.replace("'", "''")

    return f"'{value}'"


def escape_bigquery_string(value: str) -> str:
    """
    Escape a string value for BigQuery using triple-quoted strings when needed.

    BigQuery has issues with '' escaping in certain contexts, so we use
    triple-quoted raw strings for complex strings.

    Args:
        value: String value to escape

    Returns:
        Properly escaped BigQuery string literal
    """
    # Remove null bytes (not allowed in SQL strings)
    value = value.replace("\x00", "")

    # Check if string contains problematic characters that would cause
    # BigQuery concatenation issues with standard '' escaping
    has_quotes = "'" in value

    if has_quotes:
        # Use triple-quoted string to avoid concatenation issues with quotes
        # But we need to handle control characters properly (not as raw strings)
        # Escape any triple quotes in the content
        escaped_value = value.replace('"""', r"\"\"\"")
        return f'"""{escaped_value}"""'
    else:
        # Use standard SQL string escaping for simple cases
        return escape_sql_string(value)


def format_sql_value(value: Any, column_type: Type, dialect: str = "standard") -> str:
    """
    Format a Python value as a SQL literal based on column type and SQL dialect.

    Args:
        value: Python value to format
        column_type: Python type of the column
        dialect: SQL dialect ("standard", "bigquery", "mysql", etc.)

    Returns:
        Formatted SQL literal string
    """
    from datetime import date, datetime
    from decimal import Decimal

    import pandas as pd

    # Handle NULL values
    if value is None or pd.isna(value):
        if dialect == "redshift":
            # Redshift needs type-specific NULL casting
            if column_type == Decimal:
                return "NULL::DECIMAL(38,9)"
            elif column_type is int:
                return "NULL::BIGINT"
            elif column_type is float:
                return "NULL::DOUBLE PRECISION"
            elif column_type is bool:
                return "NULL::BOOLEAN"
            elif column_type is date:
                return "NULL::DATE"
            elif column_type == datetime:
                return "NULL::TIMESTAMP"
            else:
                return "NULL::VARCHAR(1024)"
        else:
            return "NULL"

    # Handle string types
    if column_type is str:
        if dialect == "bigquery":
            return escape_bigquery_string(str(value))
        else:
            return escape_sql_string(str(value))

    # Handle numeric types
    elif column_type in (int, float):
        return str(value)

    # Handle boolean types
    elif column_type is bool:
        return "TRUE" if value else "FALSE"

    # Handle date types
    elif column_type is date:
        if dialect == "bigquery":
            return f"DATE('{value}')"
        else:
            return f"DATE '{value}'"

    # Handle datetime/timestamp types
    elif column_type == datetime:
        if dialect == "bigquery":
            return f"DATETIME('{value.isoformat()}')"
        elif dialect in ("athena", "trino"):
            # Athena and Trino don't like 'T' separator in timestamps
            return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
        else:
            return f"TIMESTAMP '{value.isoformat()}'"

    # Handle decimal types
    elif column_type == Decimal:
        return str(value)

    # Default: convert to string
    else:
        return escape_sql_string(str(value))
