"""Amazon Athena adapter implementation."""

import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional, Type, Union, get_args

import boto3
import pandas as pd

from ..mock_table import BaseMockTable
from ..types import BaseTypeConverter
from .base import DatabaseAdapter


HAS_BOTO3 = True

try:
    # This is a separate import to keep the module type
    # for type checking, even if the module fails to import
    import boto3 as _boto3_module  # noqa: F401
except ImportError:
    HAS_BOTO3 = False


class AthenaTypeConverter(BaseTypeConverter):
    """Athena-specific type converter."""

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert Athena result value to target type."""
        # Handle Athena NULL values (returned as string "NULL")
        if value == "NULL":
            return None

        # Athena returns proper Python types in most cases, so use base converter
        return super().convert(value, target_type)


class AthenaAdapter(DatabaseAdapter):
    """Amazon Athena adapter for SQL testing."""

    def __init__(
        self,
        database: str,
        s3_output_location: str,
        region: str = "us-west-2",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ) -> None:
        if not HAS_BOTO3:
            raise ImportError(
                "Athena adapter requires boto3. "
                "Install with: pip install sql-testing-library[athena]"
            )

        self.database = database
        self.s3_output_location = s3_output_location
        self.region = region

        # Initialize Athena client
        if aws_access_key_id and aws_secret_access_key:
            self.client = boto3.client(
                "athena",
                region_name=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            # Use default credentials from ~/.aws/credentials or environment variables
            self.client = boto3.client("athena", region_name=region)

    def get_sqlglot_dialect(self) -> str:
        """Return Presto dialect for sqlglot (Athena uses Presto SQL)."""
        return "presto"

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        # Start query execution
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.s3_output_location},
        )

        query_execution_id = response["QueryExecutionId"]

        # Wait for query to complete
        query_status = self._wait_for_query(query_execution_id)
        if query_status != "SUCCEEDED":
            raise Exception(f"Athena query failed with status: {query_status}")

        # Get query results
        results = self.client.get_query_results(QueryExecutionId=query_execution_id)

        # Convert to DataFrame
        if "ResultSet" in results and "Rows" in results["ResultSet"]:
            rows = results["ResultSet"]["Rows"]
            if not rows:
                return pd.DataFrame()

            # First row is header
            header = [col["VarCharValue"] for col in rows[0]["Data"]]

            # Rest are data
            data = []
            for row in rows[1:]:
                data.append([col.get("VarCharValue") for col in row["Data"]])

            return pd.DataFrame(data, columns=header)
        else:
            return pd.DataFrame()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table in Athena using CTAS."""
        timestamp = int(time.time() * 1000)
        temp_table_name = f"temp_{mock_table.get_table_name()}_{timestamp}"
        qualified_table_name = f"{self.database}.{temp_table_name}"

        # Generate CTAS statement (CREATE TABLE AS SELECT)
        ctas_sql = self._generate_ctas_sql(temp_table_name, mock_table)

        # Execute CTAS query
        self.execute_query(ctas_sql)

        return qualified_table_name

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Clean up temporary tables."""
        for full_table_name in table_names:
            try:
                # Extract just the table name, not the database.table format
                if "." in full_table_name:
                    table_name = full_table_name.split(".")[-1]
                else:
                    table_name = full_table_name

                drop_query = f"DROP TABLE IF EXISTS {table_name}"
                self.execute_query(drop_query)
            except Exception as e:
                print(f"Warning: Failed to drop table {full_table_name}: {e}")

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for Athena/Presto CTE VALUES clause."""
        if value is None:
            return "NULL"
        elif column_type is str:
            # Escape single quotes
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"
        elif column_type in (int, float):
            return str(value)
        elif column_type is bool:
            return "TRUE" if value else "FALSE"
        elif column_type is date:
            return f"DATE '{value}'"
        elif column_type == datetime:
            return f"TIMESTAMP '{value.isoformat()}'"
        elif column_type == Decimal:
            return str(value)
        else:
            # Default to string representation
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"

    def get_type_converter(self) -> BaseTypeConverter:
        """Get Athena-specific type converter."""
        return AthenaTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes for Athena."""
        # Athena has a 256KB limit for query strings
        return 256 * 1024  # 256KB

    def _wait_for_query(self, query_execution_id: str, max_retries: int = 60) -> str:
        """Wait for query to complete, returns final status."""
        for _ in range(max_retries):
            response = self.client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response["QueryExecution"]["Status"]["State"]

            # Explicitly cast to string to satisfy type checker
            query_status: str = str(status)

            if query_status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return query_status

            # Wait before checking again
            time.sleep(1)

        # If we reached here, we timed out
        return "TIMEOUT"

    def _generate_ctas_sql(self, table_name: str, mock_table: BaseMockTable) -> str:
        """Generate CREATE TABLE AS SELECT (CTAS) statement for Athena."""
        df = mock_table.to_dataframe()
        column_types = mock_table.get_column_types()
        columns = list(df.columns)

        if df.empty:
            # For empty tables, create an empty table with correct schema
            # Type mapping from Python types to Athena types
            type_mapping = {
                str: "STRING",
                int: "INTEGER",
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
                    non_none_types = [
                        arg for arg in get_args(col_type) if arg is not type(None)
                    ]
                    if non_none_types:
                        col_type = non_none_types[0]

                athena_type = type_mapping.get(col_type, "STRING")
                column_defs.append(f"`{col_name}` {athena_type}")

            columns_sql = ",\n  ".join(column_defs)

            # Create an empty table with the correct schema
            return f"""
            CREATE TABLE {table_name} (
              {columns_sql}
            )
            STORED AS PARQUET
            LOCATION '{self.s3_output_location}/{table_name}/'
            WITH (is_external = false)
            """
        else:
            # For tables with data, use CTAS with a VALUES clause
            # Build a SELECT statement with literal values
            select_expressions = []

            # Generate column expressions for the first row
            first_row = df.iloc[0]
            for col_name in columns:
                col_type = column_types.get(col_name, str)
                value = first_row[col_name]
                formatted_value = self.format_value_for_cte(value, col_type)
                select_expressions.append(f"{formatted_value} AS `{col_name}`")

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
            CREATE TABLE {table_name}
            WITH (
                format = 'PARQUET',
                external_location = '{self.s3_output_location}/{table_name}/',
                is_external = false
            )
            AS {select_sql}
            """
