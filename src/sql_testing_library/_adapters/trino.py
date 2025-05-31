"""Trino adapter implementation."""

import logging
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Type, Union, get_args

import pandas as pd

from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter
from .base import DatabaseAdapter


HAS_TRINO = True

try:
    # This is a separate import to keep the module type
    # for type checking, even if the module fails to import
    import trino as _trino_module  # noqa: F401
except ImportError:
    HAS_TRINO = False


class TrinoTypeConverter(BaseTypeConverter):
    """Trino-specific type converter."""

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert Trino result value to target type."""
        # Trino returns proper Python types in most cases, so use base converter
        return super().convert(value, target_type)


class TrinoAdapter(DatabaseAdapter):
    """Trino adapter for SQL testing."""

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: Optional[str] = None,
        catalog: str = "memory",
        schema: str = "default",
        http_scheme: str = "http",
        auth: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not HAS_TRINO:
            raise ImportError(
                "Trino adapter requires trino client. "
                "Install with: pip install sql-testing-library[trino]"
            )

        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.http_scheme = http_scheme
        self.auth = auth
        self.conn = None

        # Create a connection - will validate the connection parameters
        self._get_connection()

    def _get_connection(self) -> Any:
        """Get or create a connection to Trino."""
        import trino

        # Create a new connection if needed
        if self.conn is None:
            self.conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
                http_scheme=self.http_scheme,
                auth=self.auth,
            )

        return self.conn

    def get_sqlglot_dialect(self) -> str:
        """Return Trino dialect for sqlglot."""
        return "trino"

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        conn = self._get_connection()

        # Execute query
        cursor = conn.cursor()
        cursor.execute(query)

        # If this is a SELECT query, return results
        if cursor.description:
            # Get column names from cursor description
            columns = [col[0] for col in cursor.description]

            # Fetch all rows
            rows = cursor.fetchall()

            # Create DataFrame from rows
            return pd.DataFrame(rows, columns=columns)

        # For non-SELECT queries
        return pd.DataFrame()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table in Trino using CREATE TABLE AS SELECT."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"temp_{mock_table.get_table_name()}_{timestamp}"

        # In Trino, tables are qualified with catalog and schema
        qualified_table_name = f"{self.catalog}.{self.schema}.{temp_table_name}"

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        return qualified_table_name

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Clean up temporary tables."""
        for full_table_name in table_names:
            try:
                # Extract just the table name from the fully qualified name
                # Table names can be catalog.schema.table or schema.table
                table_parts = full_table_name.split(".")
                if len(table_parts) == 3:
                    # catalog.schema.table format
                    catalog, schema, table = table_parts
                    drop_query = f'DROP TABLE IF EXISTS {catalog}.{schema}."{table}"'
                elif len(table_parts) == 2:
                    # schema.table format, use default catalog
                    schema, table = table_parts
                    drop_query = f'DROP TABLE IF EXISTS {self.catalog}.{schema}."{table}"'
                else:
                    # Just table name, use default catalog and schema
                    table = full_table_name
                    drop_query = f'DROP TABLE IF EXISTS {self.catalog}.{self.schema}."{table}"'

                self.execute_query(drop_query)
            except Exception as e:
                logging.warning(f"Warning: Failed to drop table {full_table_name}: {e}")

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for Trino CTE VALUES clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect="trino")

    def get_type_converter(self) -> BaseTypeConverter:
        """Get Trino-specific type converter."""
        return TrinoTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes for Trino."""
        # Trino doesn't have a documented size limit, but we'll use a reasonable default
        return 16 * 1024 * 1024  # 16MB

    def _generate_ctas_sql(self, table_name: str, mock_table: BaseMockTable) -> str:
        """Generate CREATE TABLE AS SELECT (CTAS) statement for Trino."""
        df = mock_table.to_dataframe()
        column_types = mock_table.get_column_types()
        columns = list(df.columns)

        # Qualify table name with schema but not catalog
        # Catalog is specified in the current session context
        qualified_table = f"{self.schema}.{table_name}"

        if df.empty:
            # For empty tables, create an empty table with correct schema
            # Type mapping from Python types to Trino types
            type_mapping = {
                str: "VARCHAR",
                int: "BIGINT",
                float: "DOUBLE",
                bool: "BOOLEAN",
                date: "DATE",
                datetime: "TIMESTAMP",
                Decimal: "DECIMAL(38,9)",
            }

            # Generate column definitions
            column_defs = []
            for col_name, col_type in column_types.items():
                # Handle Optional types
                if hasattr(col_type, "__origin__") and col_type.__origin__ is Union:
                    # Extract the non-None type from Optional[T]
                    non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                    if non_none_types:
                        col_type = non_none_types[0]

                trino_type = type_mapping.get(col_type, "VARCHAR")
                column_defs.append(f'"{col_name}" {trino_type}')

            columns_sql = ",\n  ".join(column_defs)

            # Create an empty table with the correct schema
            return f"""
            CREATE TABLE {qualified_table} (
              {columns_sql}
            )
            WITH (format = 'ORC')
            """
        else:
            # For tables with data, use CTAS with a VALUES clause
            # Build a SELECT statement with literal values for the first row
            select_expressions = []

            # Generate column expressions for the first row
            first_row = df.iloc[0]
            for col_name in columns:
                col_type = column_types.get(col_name, str)
                value = first_row[col_name]
                formatted_value = self.format_value_for_cte(value, col_type)
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
                    formatted_value = self.format_value_for_cte(value, col_type)
                    row_values.append(formatted_value)

                select_sql += f"\nUNION ALL SELECT {', '.join(row_values)}"

            # Create the CTAS statement
            return f"""
            CREATE TABLE {qualified_table}
            WITH (format = 'ORC')
            AS {select_sql}
            """
