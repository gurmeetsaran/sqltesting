"""Amazon Redshift adapter implementation."""

import time
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union, get_args


if TYPE_CHECKING:
    import pandas as pd
    import psycopg2
    import psycopg2.extras

# Heavy import moved to function level for better performance
from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter
from .base import DatabaseAdapter


try:
    import psycopg2
    import psycopg2.extras

    has_psycopg2 = True
except ImportError:
    has_psycopg2 = False
    psycopg2 = None  # type: ignore


class RedshiftTypeConverter(BaseTypeConverter):
    """Redshift-specific type converter."""

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert Redshift result value to target type."""
        # Handle None/NULL values first
        if value is None:
            return None

        # Handle Optional types
        if self.is_optional_type(target_type):
            if value is None:
                return None
            target_type = self.get_optional_inner_type(target_type)

        # Handle dict/map types from Redshift SUPER columns
        if hasattr(target_type, "__origin__") and target_type.__origin__ is dict:
            # Redshift returns SUPER types as Python dicts already
            if isinstance(value, dict):
                return value
            # If it's a string (shouldn't happen with psycopg2), parse it
            elif isinstance(value, str):
                import json

                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return {}
            else:
                return {}

        # Redshift returns proper Python types in most cases, so use base converter
        return super().convert(value, target_type)


class RedshiftAdapter(DatabaseAdapter):
    """Amazon Redshift adapter for SQL testing."""

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5439,
    ) -> None:
        if not has_psycopg2:
            raise ImportError(
                "Redshift adapter requires psycopg2. "
                "Install with: pip install sql-testing-library[redshift]"
            )

        assert psycopg2 is not None  # For type checker

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = None

    def _get_connection(self) -> Any:
        """Get or create a connection to Redshift."""
        # Create a new connection if needed
        if self.conn is None:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
            )

        # The connection object will have closed attribute at runtime
        return self.conn

    def get_sqlglot_dialect(self) -> str:
        """Return Redshift dialect for sqlglot."""
        return "redshift"

    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        import pandas as pd

        conn = self._get_connection()

        # Use cursor with dictionary-like results
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            conn.commit()

            # If this is a SELECT query, return results
            if cursor.description:
                results = cursor.fetchall()
                # Convert to DataFrame - we use pandas since it handles the conversion
                # from dict-like objects to DataFrame nicely
                return pd.DataFrame(results)

            # For non-SELECT queries
            return pd.DataFrame()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table in Redshift using CTAS."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"temp_{mock_table.get_table_name()}_{timestamp}"

        # In Redshift, temporary tables are automatically created in a session-specific
        # temporary schema, NOT in the public schema. They are automatically dropped
        # at session end, so we don't need to explicitly clean them up.
        # We return just the table name without schema qualification since temp tables
        # are accessible without schema prefix.

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        # Return just the table name, no schema prefix needed for temp tables
        return temp_table_name

    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create a temporary table and return both table name and SQL."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"temp_{mock_table.get_table_name()}_{timestamp}"

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        # Return just the table name and the SQL
        return temp_table_name, ctas_sql

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Clean up temporary tables."""
        # Redshift temporary tables are automatically dropped at the end of the session
        # This method is included for compatibility with the adapter interface
        # but doesn't need to do anything
        pass

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for Redshift CTE VALUES clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect="redshift")

    def get_type_converter(self) -> BaseTypeConverter:
        """Get Redshift-specific type converter."""
        return RedshiftTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes for Redshift."""
        # Redshift has a 16MB limit for SQL statements
        return 16 * 1024 * 1024  # 16MB

    def _generate_ctas_sql(self, table_name: str, mock_table: BaseMockTable) -> str:
        """Generate CREATE TABLE AS SELECT (CTAS) statement for Redshift.

        Redshift temporary tables are automatically dropped at the end of the session.
        """
        df = mock_table.to_dataframe()
        column_types = mock_table.get_column_types()
        columns = list(df.columns)

        if df.empty:
            # For empty tables, create an empty temporary table with correct schema
            # Type mapping from Python types to Redshift types
            type_mapping = {
                str: "VARCHAR",
                int: "BIGINT",
                float: "DOUBLE PRECISION",
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

                # Handle dict/map types
                if hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
                    redshift_type = "SUPER"
                else:
                    redshift_type = type_mapping.get(col_type, "VARCHAR")
                column_defs.append(f'"{col_name}" {redshift_type}')

            columns_sql = ",\n  ".join(column_defs)

            # Create an empty temporary table with the correct schema
            return f"""
            CREATE TEMPORARY TABLE "{table_name}" (
              {columns_sql}
            )
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
            CREATE TEMPORARY TABLE "{table_name}" AS
            {select_sql}
            """
