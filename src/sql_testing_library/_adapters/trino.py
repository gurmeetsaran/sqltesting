"""Trino adapter implementation."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional


if TYPE_CHECKING:
    import pandas as pd
    import trino

# Heavy import moved to function level for better performance
from .._types import BaseTypeConverter
from .presto import PrestoBaseAdapter, PrestoBaseTypeConverter


try:
    import trino

    has_trino = True
except ImportError:
    has_trino = False
    trino = None  # type: ignore


class TrinoTypeConverter(PrestoBaseTypeConverter):
    """Trino-specific type converter."""

    def convert(self, value: Any, target_type: type) -> Any:
        """Convert Trino result value to target type."""
        # Trino returns proper Python types in most cases, so use base converter
        return super().convert(value, target_type)


class TrinoAdapter(PrestoBaseAdapter):
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
        if not has_trino:
            raise ImportError(
                "Trino adapter requires trino client. "
                "Install with: pip install sql-testing-library[trino]"
            )

        assert trino is not None  # For type checker

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

    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        import pandas as pd

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
            df = pd.DataFrame(rows)
            df.columns = columns
            return df

        # For non-SELECT queries
        return pd.DataFrame()

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

    def get_type_converter(self) -> BaseTypeConverter:
        """Get Trino-specific type converter."""
        return TrinoTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes for Trino."""
        # Trino doesn't have a documented size limit, but we'll use a reasonable default
        return 16 * 1024 * 1024  # 16MB

    def _get_qualified_table_name(self, table_name: str) -> str:
        """Get the fully qualified table name for Trino."""
        return f"{self.catalog}.{self.schema}.{table_name}"

    def _get_qualified_table_name_for_ctas(self, table_name: str) -> str:
        """Get the qualified table name for use in CTAS (schema.table)."""
        return f"{self.schema}.{table_name}"

    def _get_empty_table_ddl(self, qualified_table: str, columns_sql: str) -> str:
        """Get DDL for creating an empty table in Trino."""
        if self.catalog == "memory":
            return f"""
            CREATE TABLE {qualified_table} (
              {columns_sql}
            )
            """
        else:
            return f"""
            CREATE TABLE {qualified_table} (
              {columns_sql}
            )
            WITH (format = 'ORC')
            """

    def _get_ctas_ddl(self, qualified_table: str, select_sql: str) -> str:
        """Get CTAS DDL for Trino."""
        if self.catalog == "memory":
            return f"""
            CREATE TABLE {qualified_table}
            AS {select_sql}
            """
        else:
            return f"""
            CREATE TABLE {qualified_table}
            WITH (format = 'ORC')
            AS {select_sql}
            """
