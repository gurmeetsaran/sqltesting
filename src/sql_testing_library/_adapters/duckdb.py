"""DuckDB adapter implementation."""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Tuple,
    Type,
    get_args,
    get_type_hints,
)


if TYPE_CHECKING:
    import duckdb
    import pandas as pd

# Heavy imports moved to function level for better performance
from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter, is_union_type
from .base import DatabaseAdapter


try:
    import duckdb

    has_duckdb = True
except ImportError:
    has_duckdb = False
    duckdb = None  # type: ignore


class DuckDBTypeConverter(BaseTypeConverter):
    """DuckDB-specific type converter."""

    def _create_struct_instance(self, struct_type: Type, field_values: dict) -> Any:
        """Create a struct instance from field values."""
        from dataclasses import is_dataclass

        from .._types import is_pydantic_model_class

        if is_dataclass(struct_type):
            return struct_type(**field_values)
        elif is_pydantic_model_class(struct_type):
            return struct_type(**field_values)
        else:
            # Fallback: try to construct with values or empty
            try:
                return struct_type(**field_values)
            except Exception:
                return struct_type()

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert DuckDB result value to target type."""
        from .._types import is_struct_type

        # Handle None/NULL values first
        if value is None:
            return None

        # Handle Optional types
        if self.is_optional_type(target_type):
            if value is None:
                return None
            target_type = self.get_optional_inner_type(target_type)

        # Handle struct types
        if is_struct_type(target_type):
            if isinstance(value, dict):
                # DuckDB returns structs as dict-like objects
                type_hints = get_type_hints(target_type)
                field_values = {}
                for field_name, field_type in type_hints.items():
                    if field_name in value:
                        # Recursively convert nested values
                        field_values[field_name] = self.convert(value[field_name], field_type)
                    else:
                        field_values[field_name] = None
                # Create struct instance
                return self._create_struct_instance(target_type, field_values)
            else:
                return value

        # Handle dict/map types
        if hasattr(target_type, "__origin__") and target_type.__origin__ is dict:
            # DuckDB can return MAP types as dict directly
            if isinstance(value, dict):
                return value
            elif isinstance(value, str):
                # If stored as JSON string, parse it
                import json

                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return {}
            else:
                return {}

        # DuckDB typically returns proper Python types, so use base converter
        return super().convert(value, target_type)


class DuckDBAdapter(DatabaseAdapter):
    """DuckDB adapter for SQL testing."""

    def __init__(self, database: str = ":memory:") -> None:
        if not has_duckdb:
            raise ImportError(
                "DuckDB adapter requires duckdb. "
                "Install with: pip install sql-testing-library[duckdb]"
            )

        assert duckdb is not None  # For type checker

        self.database = database
        self.connection = duckdb.connect(database)

    def get_sqlglot_dialect(self) -> str:
        """Return DuckDB dialect for sqlglot."""
        return "duckdb"

    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        return self.connection.execute(query).df()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create temporary table in DuckDB."""
        temp_table_name = self.get_temp_table_name(mock_table)

        # Create table schema from mock table
        create_sql = self._generate_create_table_sql(mock_table, temp_table_name)

        # Create table
        self.connection.execute(create_sql)

        # Insert data
        df = mock_table.to_dataframe()
        if not df.empty:
            # Convert complex types for DuckDB
            df = self._prepare_dataframe_for_duckdb(df, mock_table)

            # Use DuckDB's efficient DataFrame insertion
            self.connection.register("temp_df", df)
            columns = ", ".join(df.columns)
            insert_sql = f"INSERT INTO {temp_table_name} SELECT {columns} FROM temp_df"
            self.connection.execute(insert_sql)
            self.connection.unregister("temp_df")

        return temp_table_name

    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create temporary table and return both table name and SQL."""
        temp_table_name = self.get_temp_table_name(mock_table)

        # Generate CREATE TABLE SQL
        create_sql = self._generate_create_table_sql(mock_table, temp_table_name)

        # Get insert SQL for the data
        df = mock_table.to_dataframe()
        if not df.empty:
            # Generate INSERT statement
            values_rows = []
            for _, row in df.iterrows():
                values = []
                for col in df.columns:
                    value = row[col]
                    col_type = mock_table.get_column_types().get(col, str)
                    formatted_value = self.format_value_for_cte(value, col_type)
                    values.append(formatted_value)
                values_rows.append(f"({', '.join(values)})")

            values_sql = ",\n".join(values_rows)
            insert_sql = f"INSERT INTO {temp_table_name} VALUES\n{values_sql}"
            full_sql = f"{create_sql};\n\n{insert_sql};"
        else:
            full_sql = create_sql + ";"

        # Actually create the table
        self.connection.execute(create_sql)

        # Insert data if any
        if not df.empty:
            df = self._prepare_dataframe_for_duckdb(df, mock_table)
            self.connection.register("temp_df", df)
            columns = ", ".join(df.columns)
            insert_sql = f"INSERT INTO {temp_table_name} SELECT {columns} FROM temp_df"
            self.connection.execute(insert_sql)
            self.connection.unregister("temp_df")

        return temp_table_name, full_sql

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Delete temporary tables."""
        for table_name in table_names:
            try:
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                logging.warning(f"Warning: Failed to delete table {table_name}: {e}")

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for DuckDB CTE VALUES clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect="duckdb")

    def get_type_converter(self) -> BaseTypeConverter:
        """Get DuckDB-specific type converter."""
        return DuckDBTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes, or None if no limit."""
        # DuckDB doesn't have strict query size limits like cloud databases
        return None

    def _generate_create_table_sql(self, mock_table: BaseMockTable, table_name: str) -> str:
        """Generate CREATE TABLE SQL for DuckDB."""
        from .._types import is_struct_type

        column_types = mock_table.get_column_types()

        # Type mapping from Python types to DuckDB types
        type_mapping = {
            str: "VARCHAR",
            int: "BIGINT",
            float: "DOUBLE",
            bool: "BOOLEAN",
            date: "DATE",
            datetime: "TIMESTAMP",
            Decimal: "DECIMAL",
        }

        column_defs = []
        for col_name, col_type in column_types.items():
            # Handle Optional types (both Optional[X] and X | None)
            if is_union_type(col_type):
                # Extract the non-None type from Optional[T] or T | None
                non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                if non_none_types:
                    col_type = non_none_types[0]

            # Handle List/Array types
            if hasattr(col_type, "__origin__") and col_type.__origin__ is list:
                # Get the element type from List[T]
                element_type = get_args(col_type)[0] if get_args(col_type) else str

                # Check if it's a list of structs
                if is_struct_type(element_type):
                    # Create struct array type
                    struct_def = self._get_struct_definition(element_type)
                    column_defs.append(f"{col_name} STRUCT{struct_def}[]")
                else:
                    # Map element type to DuckDB type
                    element_db_type = type_mapping.get(element_type, "VARCHAR")
                    column_defs.append(f"{col_name} {element_db_type}[]")

            # Handle Dict/Map types
            elif hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
                # DuckDB has native MAP support
                key_type = get_args(col_type)[0] if get_args(col_type) else str
                value_type = get_args(col_type)[1] if len(get_args(col_type)) > 1 else str

                key_db_type = type_mapping.get(key_type, "VARCHAR")
                value_db_type = type_mapping.get(value_type, "VARCHAR")
                column_defs.append(f"{col_name} MAP({key_db_type}, {value_db_type})")

            # Handle Struct types
            elif is_struct_type(col_type):
                struct_def = self._get_struct_definition(col_type)
                column_defs.append(f"{col_name} STRUCT{struct_def}")

            else:
                # Handle scalar types
                db_type = type_mapping.get(col_type, "VARCHAR")
                column_defs.append(f"{col_name} {db_type}")

        columns_sql = ",\n  ".join(column_defs)
        return f"CREATE TABLE {table_name} (\n  {columns_sql}\n)"

    def _get_struct_definition(self, struct_type: Type) -> str:
        """Convert struct type to DuckDB STRUCT definition."""
        from .._types import is_struct_type

        # Type mapping from Python types to DuckDB types
        type_mapping = {
            str: "VARCHAR",
            int: "BIGINT",
            float: "DOUBLE",
            bool: "BOOLEAN",
            date: "DATE",
            datetime: "TIMESTAMP",
            Decimal: "DECIMAL",
        }

        type_hints = get_type_hints(struct_type)
        field_defs = []

        for field_name, field_type in type_hints.items():
            # Handle Optional types (both Optional[X] and X | None)
            if is_union_type(field_type):
                # Extract the non-None type from Optional[T] or T | None
                non_none_types = [arg for arg in get_args(field_type) if arg is not type(None)]
                if non_none_types:
                    field_type = non_none_types[0]

            # Handle nested structs
            if is_struct_type(field_type):
                nested_struct_def = self._get_struct_definition(field_type)
                field_defs.append(f"{field_name} STRUCT{nested_struct_def}")

            # Handle List types in structs
            elif hasattr(field_type, "__origin__") and field_type.__origin__ is list:
                element_type = get_args(field_type)[0] if get_args(field_type) else str
                if is_struct_type(element_type):
                    # List of structs
                    nested_struct_def = self._get_struct_definition(element_type)
                    field_defs.append(f"{field_name} STRUCT{nested_struct_def}[]")
                else:
                    # List of scalars
                    element_db_type = type_mapping.get(element_type, "VARCHAR")
                    field_defs.append(f"{field_name} {element_db_type}[]")

            # Handle Dict types in structs
            elif hasattr(field_type, "__origin__") and field_type.__origin__ is dict:
                key_type = get_args(field_type)[0] if get_args(field_type) else str
                value_type = get_args(field_type)[1] if len(get_args(field_type)) > 1 else str

                key_db_type = type_mapping.get(key_type, "VARCHAR")
                value_db_type = type_mapping.get(value_type, "VARCHAR")
                field_defs.append(f"{field_name} MAP({key_db_type}, {value_db_type})")

            else:
                # Handle scalar types
                db_type = type_mapping.get(field_type, "VARCHAR")
                field_defs.append(f"{field_name} {db_type}")

        return f"({', '.join(field_defs)})"

    def _prepare_dataframe_for_duckdb(
        self, df: "pd.DataFrame", mock_table: BaseMockTable
    ) -> "pd.DataFrame":
        """Prepare DataFrame for DuckDB.

        DuckDB can handle most Python types natively, but we need to handle
        some special cases for structs and complex types.
        """
        from dataclasses import is_dataclass

        import pandas as pd

        from .._types import is_pydantic_model_class, is_struct_type

        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        column_types = mock_table.get_column_types()

        for col_name, col_type in column_types.items():
            # Handle Optional types (both Optional[X] and X | None)
            if is_union_type(col_type):
                # Extract the non-None type from Optional[T] or T | None
                non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                if non_none_types:
                    col_type = non_none_types[0]

            # Check if this is a struct type
            if is_struct_type(col_type):
                # Convert struct objects to dictionaries for DuckDB
                def convert_struct_to_dict(val):
                    if pd.isna(val) or val is None:
                        return None
                    elif is_dataclass(val):
                        # Convert dataclass to dict recursively
                        return self._dataclass_to_dict(val)
                    elif is_pydantic_model_class(type(val)):
                        # Convert Pydantic model to dict
                        return val.model_dump() if hasattr(val, "model_dump") else val.dict()
                    else:
                        return val

                df_copy[col_name] = df_copy[col_name].apply(convert_struct_to_dict)

            # Check if this is a list of structs
            elif hasattr(col_type, "__origin__") and col_type.__origin__ is list:
                element_type = get_args(col_type)[0] if get_args(col_type) else str
                if is_struct_type(element_type):
                    # Convert list of structs to list of dicts
                    def convert_struct_list(val_list):
                        if val_list is None:
                            return None
                        # Check for empty list
                        if isinstance(val_list, list) and len(val_list) == 0:
                            return []
                        # Handle pandas NaN
                        try:
                            if pd.isna(val_list):
                                return None
                        except (ValueError, TypeError):
                            # pd.isna() may fail on lists, continue processing
                            pass

                        result = []
                        for val in val_list:
                            if is_dataclass(val):
                                result.append(self._dataclass_to_dict(val))
                            elif is_pydantic_model_class(type(val)):
                                result.append(
                                    val.model_dump() if hasattr(val, "model_dump") else val.dict()
                                )
                            else:
                                result.append(val)
                        return result

                    df_copy[col_name] = df_copy[col_name].apply(convert_struct_list)

            # DuckDB handles dict/map types natively, no conversion needed for basic dict types
            # However, we may need to handle nested complex types within dicts
            elif hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
                # DuckDB can handle dict types directly, but ensure nested objects are converted
                def convert_dict_values(val):
                    if pd.isna(val) or val is None:
                        return None
                    elif isinstance(val, dict):
                        # Convert any nested dataclass/pydantic objects in dict values
                        result = {}
                        for k, v in val.items():
                            if is_dataclass(v):
                                result[k] = self._dataclass_to_dict(v)
                            elif is_pydantic_model_class(type(v)):
                                result[k] = v.model_dump() if hasattr(v, "model_dump") else v.dict()
                            else:
                                result[k] = v
                        return result
                    else:
                        return val

                df_copy[col_name] = df_copy[col_name].apply(convert_dict_values)

        return df_copy

    def _dataclass_to_dict(self, obj: Any) -> Any:
        """Recursively convert dataclass to dict, handling nested structs."""
        from dataclasses import is_dataclass

        if is_dataclass(obj):
            # Get the dict representation
            result = {}
            for field in obj.__dataclass_fields__:
                value = getattr(obj, field)
                if is_dataclass(value):
                    # Recursively convert nested dataclass
                    result[field] = self._dataclass_to_dict(value)
                elif isinstance(value, list):
                    # Handle lists (might contain dataclasses)
                    result[field] = [
                        self._dataclass_to_dict(item) if is_dataclass(item) else item
                        for item in value
                    ]
                elif isinstance(value, dict):
                    # Handle nested dicts
                    result[field] = {
                        k: self._dataclass_to_dict(v) if is_dataclass(v) else v
                        for k, v in value.items()
                    }
                else:
                    # Keep other values as-is (including Decimal)
                    result[field] = value
            return result
        else:
            return obj
