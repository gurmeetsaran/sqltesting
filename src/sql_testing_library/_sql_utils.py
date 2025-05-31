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
    # Note: pd.isna() doesn't work on lists/arrays, so check for None first
    # and only use pd.isna() on scalar values
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)):
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

    # Handle array/list types
    if hasattr(column_type, "__origin__") and column_type.__origin__ is list:
        from typing import get_args

        # Get the element type from List[T]
        element_type = get_args(column_type)[0] if get_args(column_type) else str

        # Return database-specific array syntax
        if dialect == "bigquery":
            # Format each element in the array for BigQuery
            formatted_elements = []
            for element in value:
                formatted_element = format_sql_value(element, element_type, dialect)
                formatted_elements.append(formatted_element)
            return f"[{', '.join(formatted_elements)}]"
        elif dialect in ("athena", "trino"):
            # Format each element in the array for Athena/Trino
            formatted_elements = []
            for element in value:
                formatted_element = format_sql_value(element, element_type, dialect)
                formatted_elements.append(formatted_element)
            return f"ARRAY[{', '.join(formatted_elements)}]"
        elif dialect == "redshift":
            # Redshift uses JSON-like syntax for SUPER arrays
            # Format elements as JSON (double quotes for strings)
            import json
            from decimal import Decimal

            # Convert elements to JSON-serializable types
            json_elements = []
            for element in value:
                if isinstance(element, Decimal):
                    json_elements.append(float(element))
                else:
                    json_elements.append(element)

            json_array = json.dumps(json_elements)
            return f"JSON_PARSE('{json_array}')"
        elif dialect == "snowflake":
            # Format each element in the array for Snowflake
            formatted_elements = []
            for element in value:
                formatted_element = format_sql_value(element, element_type, dialect)
                formatted_elements.append(formatted_element)
            return f"ARRAY_CONSTRUCT({', '.join(formatted_elements)})"
        else:
            # Default to generic array syntax
            formatted_elements = []
            for element in value:
                formatted_element = format_sql_value(element, element_type, dialect)
                formatted_elements.append(formatted_element)
            return f"ARRAY[{', '.join(formatted_elements)}]"

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
