"""SQL utility functions for escaping and formatting values."""

import json
from dataclasses import is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Type, get_type_hints


# SQL type mappings for different dialects
SQL_TYPE_MAPPINGS: Dict[str, Dict[Type, str]] = {
    "athena": {
        str: "VARCHAR",
        int: "BIGINT",
        float: "DOUBLE",
        bool: "BOOLEAN",
        date: "DATE",
        datetime: "TIMESTAMP",
        Decimal: "DECIMAL(38,9)",
    },
    "trino": {
        str: "VARCHAR",
        int: "BIGINT",
        float: "DOUBLE",
        bool: "BOOLEAN",
        date: "DATE",
        datetime: "TIMESTAMP",
        Decimal: "DECIMAL(38,9)",
    },
    # Can be extended for other dialects
    "bigquery": {
        str: "STRING",
        int: "INT64",
        float: "FLOAT64",
        bool: "BOOL",
        date: "DATE",
        datetime: "DATETIME",
        Decimal: "NUMERIC",
    },
    "redshift": {
        str: "VARCHAR",
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
        date: "DATE",
        datetime: "TIMESTAMP",
        Decimal: "DECIMAL(38,9)",
    },
    "snowflake": {
        str: "VARCHAR",
        int: "NUMBER",
        float: "FLOAT",
        bool: "BOOLEAN",
        date: "DATE",
        datetime: "TIMESTAMP",
        Decimal: "NUMBER(38,9)",
    },
}


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


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


def get_sql_type_string(py_type: Type, dialect: str) -> str:
    """
    Get SQL type string for a Python type based on dialect.

    Args:
        py_type: Python type to convert
        dialect: SQL dialect name

    Returns:
        SQL type string
    """
    # Import here to avoid circular imports
    from ._types import is_struct_type

    # Get type mapping for the dialect, fallback to athena if not found
    type_mapping = SQL_TYPE_MAPPINGS.get(dialect, SQL_TYPE_MAPPINGS.get("athena", {}))

    # Check if it's a basic type
    if py_type in type_mapping:
        return type_mapping[py_type]

    # Check if it's a struct type
    if is_struct_type(py_type):
        # Build nested ROW type recursively
        nested_hints = get_type_hints(py_type)
        nested_fields = []
        for nested_name, nested_type in nested_hints.items():
            nested_sql = get_sql_type_string(nested_type, dialect)
            nested_fields.append(f"{nested_name} {nested_sql}")
        return f"ROW({', '.join(nested_fields)})"

    # For other complex types, default to VARCHAR
    return type_mapping.get(str, "VARCHAR")


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
    from typing import get_args

    import pandas as pd

    # Import struct checking utilities
    from ._types import is_struct_type

    # Handle struct types before NULL checking
    if is_struct_type(column_type):
        return _format_struct_value(value, column_type, dialect)

    # Handle NULL values
    # Note: pd.isna() doesn't work on lists/arrays/dicts, so check for None first
    # and only use pd.isna() on scalar values
    if value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        # Check if column_type is a List type
        if hasattr(column_type, "__origin__") and column_type.__origin__ is list:
            # Get the element type from List[T]
            element_type = get_args(column_type)[0] if get_args(column_type) else str

            if dialect in ("athena", "trino"):
                # Check if element type is a struct
                if is_struct_type(element_type):
                    # Get the SQL type for the struct
                    sql_element_type = get_sql_type_string(element_type, dialect)
                    return f"CAST(NULL AS ARRAY({sql_element_type}))"
                # Map Python types to SQL types for array elements
                elif element_type == Decimal:
                    sql_element_type = "DECIMAL(38,9)"
                elif element_type is int:
                    sql_element_type = "INTEGER" if dialect == "athena" else "BIGINT"
                elif element_type is float:
                    sql_element_type = "DOUBLE"
                elif element_type is bool:
                    sql_element_type = "BOOLEAN"
                elif element_type is date:
                    sql_element_type = "DATE"
                elif element_type == datetime:
                    sql_element_type = "TIMESTAMP"
                else:
                    sql_element_type = "VARCHAR"

                return f"CAST(NULL AS ARRAY({sql_element_type}))"
            elif dialect == "bigquery":
                # BigQuery doesn't need explicit NULL array casting
                return "NULL"
            elif dialect == "redshift":
                # Redshift SUPER type handles NULL arrays
                return "NULL::SUPER"
            elif dialect == "snowflake":
                # Snowflake VARIANT type handles NULL arrays
                return "NULL::VARIANT"
            else:
                return "NULL"

        # Check if column_type is a Dict/Map type
        elif hasattr(column_type, "__origin__") and column_type.__origin__ is dict:
            # Get the key and value types from Dict[K, V]
            type_args = get_args(column_type)
            key_type = type_args[0] if type_args else str
            value_type = type_args[1] if len(type_args) > 1 else str

            if dialect in ("athena", "trino"):
                # Map Python types to SQL types for map key and value
                def get_sql_type(py_type):
                    if py_type == Decimal:
                        return "DECIMAL(38,9)"
                    elif py_type is int:
                        return "INTEGER" if dialect == "athena" else "BIGINT"
                    elif py_type is float:
                        return "DOUBLE"
                    elif py_type is bool:
                        return "BOOLEAN"
                    elif py_type is date:
                        return "DATE"
                    elif py_type == datetime:
                        return "TIMESTAMP"
                    else:
                        return "VARCHAR"

                sql_key_type = get_sql_type(key_type)
                sql_value_type = get_sql_type(value_type)
                return f"CAST(NULL AS MAP({sql_key_type}, {sql_value_type}))"
            elif dialect == "redshift":
                # Redshift SUPER type handles NULL maps
                return "NULL::SUPER"
            elif dialect == "bigquery":
                # BigQuery JSON type handles NULL maps
                return "NULL"
            elif dialect == "snowflake":
                # Snowflake VARIANT type handles NULL maps
                return "NULL::VARIANT"
            else:
                return "NULL"

        # Handle non-array NULL values
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
                return "NULL::VARCHAR"
        elif dialect in ("athena", "trino"):
            # Athena and Trino need type-specific NULL casting for table creation
            if column_type == Decimal:
                return "CAST(NULL AS DECIMAL(38,9))"
            elif column_type is int:
                # Athena uses INTEGER, Trino uses BIGINT
                int_type = "INTEGER" if dialect == "athena" else "BIGINT"
                return f"CAST(NULL AS {int_type})"
            elif column_type is float:
                return "CAST(NULL AS DOUBLE)"
            elif column_type is bool:
                return "CAST(NULL AS BOOLEAN)"
            elif column_type is date:
                return "CAST(NULL AS DATE)"
            elif column_type == datetime:
                return "CAST(NULL AS TIMESTAMP)"
            else:
                # Both Athena and Trino support VARCHAR without size specification
                return "CAST(NULL AS VARCHAR)"
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

    # Handle map/dict types
    if hasattr(column_type, "__origin__") and column_type.__origin__ is dict:
        from typing import get_args

        # Ensure value is a dictionary
        if not isinstance(value, dict):
            # If it's not a dict, return an empty map
            if dialect in ("athena", "trino"):
                return "MAP(ARRAY[], ARRAY[])"
            else:
                raise NotImplementedError(f"Map type not yet supported for dialect: {dialect}")

        # Get the key and value types from Dict[K, V]
        type_args = get_args(column_type)
        key_type = type_args[0] if type_args else str
        value_type = type_args[1] if len(type_args) > 1 else str

        # Return database-specific map syntax
        if dialect in ("athena", "trino"):
            # Both Athena and Trino use MAP(ARRAY[keys], ARRAY[values]) syntax
            keys = []
            values = []
            for k, v in value.items():
                keys.append(format_sql_value(k, key_type, dialect))
                values.append(format_sql_value(v, value_type, dialect))
            return f"MAP(ARRAY[{', '.join(keys)}], ARRAY[{', '.join(values)}])"
        elif dialect == "redshift":
            # Redshift uses SUPER type with JSON-like syntax for maps
            json_str = json.dumps(value, cls=DecimalEncoder)
            return f"JSON_PARSE('{json_str}')"
        elif dialect == "bigquery":
            # BigQuery stores JSON as strings
            json_str = json.dumps(value, cls=DecimalEncoder)
            # Escape single quotes in JSON string for SQL
            json_str = json_str.replace("'", "''")
            return f"'{json_str}'"
        elif dialect == "snowflake":
            # Snowflake uses VARIANT type with PARSE_JSON function
            json_str = json.dumps(value, cls=DecimalEncoder)
            # Escape single quotes in JSON string for SQL
            json_str = json_str.replace("'", "''")
            return f"PARSE_JSON('{json_str}')"
        else:
            # Other databases don't have native map support yet
            raise NotImplementedError(f"Map type not yet supported for dialect: {dialect}")

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
            if isinstance(value, datetime):
                return f"DATETIME('{value.isoformat()}')"
            else:
                return f"DATETIME('{value}')"
        elif dialect in ("athena", "trino"):
            # Athena and Trino don't like 'T' separator in timestamps
            # Athena expects millisecond precision, so truncate microseconds
            if isinstance(value, datetime):
                timestamp_str = value.strftime("%Y-%m-%d %H:%M:%S.%f")[
                    :-3
                ]  # Remove last 3 digits for millisecond precision
            else:
                timestamp_str = str(value)
            return f"TIMESTAMP '{timestamp_str}'"
        else:
            if isinstance(value, datetime):
                return f"TIMESTAMP '{value.isoformat()}'"
            else:
                return f"TIMESTAMP '{value}'"

    # Handle decimal types
    elif column_type == Decimal:
        return str(value)

    # Default: convert to string
    else:
        return escape_sql_string(str(value))


def _format_struct_value(value: Any, struct_type: Type, dialect: str) -> str:
    """
    Format a struct value (dataclass or Pydantic model instance) as SQL literal.

    Args:
        value: Struct instance (dataclass or Pydantic model)
        struct_type: The struct type
        dialect: SQL dialect

    Returns:
        Formatted SQL struct/ROW literal
    """
    # Import here to avoid circular imports
    from ._types import is_pydantic_model_class

    # For Athena/Trino
    if dialect in ("athena", "trino"):
        # Get type hints for building ROW type definition
        type_hints = get_type_hints(struct_type)

        # Build the ROW type definition using the global function
        row_type = get_sql_type_string(struct_type, dialect)

        # Handle NULL struct values
        if value is None:
            return f"CAST(NULL AS {row_type})"

        # Format non-NULL struct values
        field_values = []
        for field_name, field_type in type_hints.items():
            # Get the field value
            if is_dataclass(value):
                field_value = getattr(value, field_name, None)
            elif is_pydantic_model_class(type(value)):
                field_value = getattr(value, field_name, None)
            else:
                # If it's a dict
                field_value = value.get(field_name) if isinstance(value, dict) else None

            # Format the field value recursively using format_sql_value
            formatted_value = format_sql_value(field_value, field_type, dialect)
            field_values.append(formatted_value)

        return f"CAST(ROW({', '.join(field_values)}) AS {row_type})"

    # For other databases, struct support would need to be implemented
    else:
        raise NotImplementedError(f"Struct type not yet supported for dialect: {dialect}")
