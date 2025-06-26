"""Snowflake adapter implementation."""

import logging
import os
import time
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, get_args


if TYPE_CHECKING:
    import pandas as pd

# Heavy import moved to function level for better performance
from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter
from .base import DatabaseAdapter


try:
    import snowflake.connector  # pyright: ignore[reportUnusedImport]

    has_snowflake = True
except ImportError:
    has_snowflake = False
    snowflake = None  # type: ignore


class SnowflakeTypeConverter(BaseTypeConverter):
    """Snowflake-specific type converter."""

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert Snowflake result value to target type."""
        # Handle None/NULL values first
        if value is None:
            return None

        # Handle Optional types
        if self.is_optional_type(target_type):
            if value is None:
                return None
            target_type = self.get_optional_inner_type(target_type)

        # Handle dict/map types from Snowflake VARIANT columns
        if hasattr(target_type, "__origin__") and target_type.__origin__ is dict:
            # Snowflake returns VARIANT types as Python dicts already
            if isinstance(value, dict):
                return value
            # If it's a string, parse it
            elif isinstance(value, str):
                import json

                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return {}
            else:
                return {}

        # Snowflake returns proper Python types in most cases, so use base converter
        return super().convert(value, target_type)


class SnowflakeAdapter(DatabaseAdapter):
    """Snowflake adapter for SQL testing."""

    def __init__(
        self,
        account: str,
        user: str,
        password: Optional[str] = None,
        database: str = "",
        schema: str = "PUBLIC",
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        private_key_path: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
    ) -> None:
        if not has_snowflake:
            raise ImportError(
                "Snowflake adapter requires snowflake-connector-python. "
                "Install with: pip install sql-testing-library[snowflake]"
            )

        assert snowflake is not None  # For type checker

        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.conn: Optional[Any] = None
        self._private_key: Optional[bytes] = None

    def _load_private_key(self) -> bytes:
        """Load private key from file or environment variable and convert to DER format."""
        if self._private_key:
            return self._private_key

        # Try to load from file path
        if self.private_key_path and os.path.exists(self.private_key_path):
            with open(self.private_key_path, "rb") as key_file:
                private_key_data = key_file.read()
        # Try to load from environment variable
        elif os.environ.get("SNOWFLAKE_PRIVATE_KEY"):
            private_key_data = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()
        else:
            raise ValueError(
                "Private key not found. Provide private_key_path or "
                "set SNOWFLAKE_PRIVATE_KEY environment variable"
            )

        # Import cryptography modules
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.serialization import (
            load_der_private_key,
            load_pem_private_key,
        )

        # Determine if we have a passphrase
        passphrase_str = self.private_key_passphrase or os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )
        passphrase = passphrase_str.encode() if passphrase_str else None

        try:
            # Check if it's already in DER format
            if not private_key_data.startswith(b"-----"):
                # Already in DER format, try to load it
                try:
                    private_key_obj = load_der_private_key(
                        private_key_data, password=passphrase, backend=default_backend()
                    )
                    private_key = private_key_data
                except Exception:
                    # Not valid DER, treat as PEM
                    pass

            # Load PEM private key (handles both PKCS#1 and PKCS#8)
            if private_key_data.startswith(b"-----"):
                private_key_obj = load_pem_private_key(
                    private_key_data, password=passphrase, backend=default_backend()
                )

                # Convert to DER format (PKCS#8) for Snowflake
                private_key = private_key_obj.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
        except Exception as e:
            raise ValueError(
                f"Failed to load private key: {e}. "
                "Ensure the key is in PEM format (PKCS#1 or PKCS#8) "
                "and the passphrase (if any) is correct."
            ) from e

        self._private_key = private_key
        return private_key

    def _get_connection(self) -> Any:
        """Get or create a connection to Snowflake."""
        import snowflake.connector

        # Create a new connection if needed
        if self.conn is None:
            conn_params: Dict[str, Any] = {
                "account": self.account,
                "user": self.user,
            }

            # Add optional parameters
            if self.database:
                conn_params["database"] = self.database

            if self.schema:
                conn_params["schema"] = self.schema

            # Handle authentication
            if self.private_key_path or os.environ.get("SNOWFLAKE_PRIVATE_KEY"):
                # Use key-pair authentication
                conn_params["private_key"] = self._load_private_key()
            elif self.password:
                conn_params["password"] = self.password
            else:
                raise ValueError(
                    "No authentication method provided. Please provide one of: "
                    "1) password, 2) private_key_path, or "
                    "3) set SNOWFLAKE_PRIVATE_KEY environment variable"
                )

            if self.warehouse:
                conn_params["warehouse"] = self.warehouse

            if self.role:
                conn_params["role"] = self.role

            self.conn = snowflake.connector.connect(**conn_params)

        return self.conn

    def get_sqlglot_dialect(self) -> str:
        """Return Snowflake dialect for sqlglot."""
        return "snowflake"

    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        import pandas as pd

        conn = self._get_connection()

        # Execute query
        cursor = conn.cursor()

        # Ensure we have an active warehouse if one is specified
        if self.warehouse:
            try:
                cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            except Exception as e:
                raise RuntimeError(
                    f"Failed to use warehouse '{self.warehouse}'. "
                    f"Please check that the warehouse exists and you have USAGE permission. "
                    f"Original error: {e}"
                ) from e

        cursor.execute(query)

        # If this is a SELECT query, return results
        if cursor.description:
            # Get column names from cursor description and normalize to lowercase
            # Snowflake returns uppercase column names by default
            columns = [col[0].lower() for col in cursor.description]

            # Fetch all rows
            rows = cursor.fetchall()

            # Create DataFrame from rows
            df = pd.DataFrame(rows)
            df.columns = columns
            return df

        # For non-SELECT queries
        return pd.DataFrame()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table in Snowflake."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"TEMP_{mock_table.get_table_name()}_{timestamp}"

        # Use the adapter's configured database and schema for temporary tables
        # This avoids permission issues with creating schemas in other databases
        target_schema = self.schema

        # For temporary tables, Snowflake doesn't support full database qualification
        # Return schema.table format for temporary tables
        qualified_table_name = f"{target_schema}.{temp_table_name}"

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table, target_schema)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        return qualified_table_name

    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create a temporary table and return both table name and SQL."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"TEMP_{mock_table.get_table_name()}_{timestamp}"

        # Use the adapter's configured database and schema for temporary tables
        # This avoids permission issues with creating schemas in other databases
        target_schema = self.schema

        # For temporary tables, Snowflake doesn't support full database qualification
        # Return schema.table format for temporary tables
        qualified_table_name = f"{target_schema}.{temp_table_name}"

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table, target_schema)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        return qualified_table_name, ctas_sql

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Clean up temporary tables."""
        for full_table_name in table_names:
            try:
                # Extract just the table name from the fully qualified name
                # Table names can be database.schema.table or schema.table
                table_parts = full_table_name.split(".")
                if len(table_parts) == 3:
                    # database.schema.table format
                    database, schema, table = table_parts
                    drop_query = f'DROP TABLE IF EXISTS "{database}"."{schema}"."{table}"'
                elif len(table_parts) == 2:
                    # schema.table format, use default database
                    schema, table = table_parts
                    drop_query = f'DROP TABLE IF EXISTS "{self.database}"."{schema}"."{table}"'
                else:
                    # Just table name, use default database and schema
                    table = full_table_name
                    drop_query = f'DROP TABLE IF EXISTS "{self.database}"."{self.schema}"."{table}"'  # noqa: E501

                self.execute_query(drop_query)
            except Exception as e:
                logging.warning(f"Warning: Failed to drop table {full_table_name}: {e}")

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for Snowflake CTE VALUES clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect="snowflake")

    def get_type_converter(self) -> BaseTypeConverter:
        """Get Snowflake-specific type converter."""
        return SnowflakeTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes for Snowflake."""
        # Snowflake has a 1MB limit for SQL statements
        return 1 * 1024 * 1024  # 1MB

    def _generate_ctas_sql(
        self, table_name: str, mock_table: BaseMockTable, schema: Optional[str] = None
    ) -> str:
        """Generate CREATE TABLE AS SELECT (CTAS) statement for Snowflake."""
        df = mock_table.to_dataframe()
        column_types = mock_table.get_column_types()
        columns = list(df.columns)

        # For temporary tables in Snowflake, only use schema.table, not database.schema.table
        # Temporary tables are session-specific and don't support full qualification
        target_schema = schema if schema is not None else self.schema
        qualified_table = f'"{target_schema}"."{table_name}"'

        if df.empty:
            # For empty tables, create an empty table with correct schema
            # Type mapping from Python types to Snowflake types
            type_mapping = {
                str: "VARCHAR",
                int: "NUMBER",
                float: "FLOAT",
                bool: "BOOLEAN",
                date: "DATE",
                datetime: "TIMESTAMP",
                Decimal: "NUMBER(38,9)",
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
                    snowflake_type = "VARIANT"
                else:
                    snowflake_type = type_mapping.get(col_type, "VARCHAR")
                column_defs.append(f"{col_name} {snowflake_type}")

            columns_sql = ",\n  ".join(column_defs)

            # Create an empty temporary table with the correct schema
            return f"""
            CREATE TEMPORARY TABLE {qualified_table} (
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
                select_expressions.append(f"{formatted_value} AS {col_name}")

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

            # Create the CTAS statement using temporary table
            return f"""
            CREATE TEMPORARY TABLE {qualified_table} AS
            {select_sql}
            """
