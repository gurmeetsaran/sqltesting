"""Base Presto-compatible adapter implementation for Athena and Trino."""

import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Tuple, Type, Union, get_args

from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter, is_struct_type
from .base import DatabaseAdapter


class PrestoBaseTypeConverter(BaseTypeConverter):
    """Base type converter for Presto-compatible systems."""

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert result value to target type."""
        # Most Presto-compatible systems return proper Python types
        return super().convert(value, target_type)


class PrestoBaseAdapter(DatabaseAdapter):
    """Base adapter for Presto-compatible SQL engines (Athena, Trino)."""

    # Default type mapping from Python types to SQL types
    # Subclasses can override this
    TYPE_MAPPING = {
        str: "VARCHAR",
        int: "BIGINT",
        float: "DOUBLE",
        bool: "BOOLEAN",
        date: "DATE",
        datetime: "TIMESTAMP",
        Decimal: "DECIMAL(38,9)",
    }

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table using CTAS."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"temp_{mock_table.get_table_name()}_{timestamp}"
        qualified_table_name = self._get_qualified_table_name(temp_table_name)

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        return qualified_table_name

    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create a temporary table and return both table name and SQL."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"temp_{mock_table.get_table_name()}_{timestamp}"
        qualified_table_name = self._get_qualified_table_name(temp_table_name)

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        return qualified_table_name, ctas_sql

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for CTE VALUES clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect=self.get_sqlglot_dialect())

    def get_type_converter(self) -> BaseTypeConverter:
        """Get type converter."""
        return PrestoBaseTypeConverter()

    def _get_qualified_table_name(self, table_name: str) -> str:
        """Get the fully qualified table name. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _get_qualified_table_name")

    def _get_sql_type(self, python_type: Type) -> str:
        """Convert Python type to SQL type string."""
        from .._sql_utils import get_sql_type_string

        # Handle Optional types
        if hasattr(python_type, "__origin__") and python_type.__origin__ is Union:
            # Extract the non-None type from Optional[T]
            non_none_types = [arg for arg in get_args(python_type) if arg is not type(None)]
            if non_none_types:
                python_type = non_none_types[0]

        # Handle struct types (dataclass or Pydantic model)
        if is_struct_type(python_type):
            return get_sql_type_string(python_type, self.get_sqlglot_dialect())

        # Handle List types
        if hasattr(python_type, "__origin__") and python_type.__origin__ is list:
            element_type = get_args(python_type)[0] if get_args(python_type) else str
            element_sql_type = self._get_sql_type(element_type)  # Recursive call for nested types
            return f"ARRAY({element_sql_type})"

        # Handle Dict/Map types
        elif hasattr(python_type, "__origin__") and python_type.__origin__ is dict:
            type_args = get_args(python_type)
            key_type = type_args[0] if type_args else str
            value_type = type_args[1] if len(type_args) > 1 else str
            key_sql_type = self._get_sql_type(key_type)  # Recursive call for nested types
            value_sql_type = self._get_sql_type(value_type)  # Recursive call for nested types
            return f"MAP({key_sql_type}, {value_sql_type})"

        # Regular types - use the mapping
        dialect = self.get_sqlglot_dialect()
        return get_sql_type_string(python_type, dialect)

    def _get_struct_sql_type(self, struct_type: Type) -> str:
        """Get SQL ROW type definition for a struct (dataclass or Pydantic model)."""
        from .._sql_utils import get_sql_type_string

        dialect = self.get_sqlglot_dialect()
        return get_sql_type_string(struct_type, dialect)

    def _generate_column_definitions(self, column_types: dict) -> str:
        """Generate SQL column definitions from column types."""
        column_defs = []
        for col_name, col_type in column_types.items():
            sql_type = self._get_sql_type(col_type)
            column_defs.append(f'"{col_name}" {sql_type}')
        return ",\n  ".join(column_defs)

    def _generate_ctas_sql(self, table_name: str, mock_table: BaseMockTable) -> str:
        """Generate CREATE TABLE AS SELECT (CTAS) statement."""
        df = mock_table.to_dataframe()
        column_types = mock_table.get_column_types()
        columns = list(df.columns)

        qualified_table = self._get_qualified_table_name_for_ctas(table_name)

        if df.empty:
            # For empty tables, create an empty table with correct schema
            columns_sql = self._generate_column_definitions(column_types)
            return self._get_empty_table_ddl(qualified_table, columns_sql)
        else:
            # For tables with data, use CTAS with a VALUES clause
            select_sql = self._generate_select_with_unions(df, columns, column_types)
            return self._get_ctas_ddl(qualified_table, select_sql)

    def _get_qualified_table_name_for_ctas(self, table_name: str) -> str:
        """Get the qualified table name for use in CTAS. Can be overridden by subclasses."""
        return table_name

    def _get_empty_table_ddl(self, qualified_table: str, columns_sql: str) -> str:
        """Get DDL for creating an empty table. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _get_empty_table_ddl")

    def _get_ctas_ddl(self, qualified_table: str, select_sql: str) -> str:
        """Get CTAS DDL. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _get_ctas_ddl")

    def _generate_select_with_unions(self, df: Any, columns: List[str], column_types: dict) -> str:
        """Generate SELECT statement with UNION ALL for multiple rows."""
        # Build a SELECT statement with literal values for the first row
        select_expressions = []

        # Generate column expressions for the first row
        first_row = df.iloc[0]
        for col_name in columns:
            col_type = column_types.get(col_name, str)
            value = first_row[col_name]

            # Handle Optional types by extracting the non-None type for proper formatting
            actual_type = col_type
            if hasattr(col_type, "__origin__") and col_type.__origin__ is Union:
                # Extract the non-None type from Optional[T]
                non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                if non_none_types:
                    actual_type = non_none_types[0]

            formatted_value = self.format_value_for_cte(value, actual_type)
            select_expressions.append(f'{formatted_value} AS "{col_name}"')

        # Start with the first row in the SELECT
        select_sql = f"SELECT {', '.join(select_expressions)}"

        # Add UNION ALL for each additional row
        for i in range(1, len(df)):
            row = df.iloc[i]
            row_values = []
            for col_name in columns:
                col_type = column_types.get(col_name, str)
                value = row[col_name]

                # Handle Optional types by extracting the non-None type for proper formatting
                actual_type = col_type
                if hasattr(col_type, "__origin__") and col_type.__origin__ is Union:
                    # Extract the non-None type from Optional[T]
                    non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                    if non_none_types:
                        actual_type = non_none_types[0]

                formatted_value = self.format_value_for_cte(value, actual_type)
                row_values.append(formatted_value)

            select_sql += f"\nUNION ALL SELECT {', '.join(row_values)}"

        return select_sql
