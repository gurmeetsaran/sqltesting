"""BigQuery adapter implementation."""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union, get_args


if TYPE_CHECKING:
    import pandas as pd
    from google.cloud import bigquery

# Heavy imports moved to function level for better performance
from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter
from .base import DatabaseAdapter


try:
    from google.cloud import bigquery

    has_bigquery = True
except ImportError:
    has_bigquery = False
    bigquery = None  # type: ignore


class BigQueryTypeConverter(BaseTypeConverter):
    """BigQuery-specific type converter."""

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert BigQuery result value to target type."""
        # Handle None/NULL values first
        if value is None:
            return None

        # Handle Optional types
        if self.is_optional_type(target_type):
            if value is None:
                return None
            target_type = self.get_optional_inner_type(target_type)

        # Handle dict/map types from BigQuery STRING columns containing JSON
        if hasattr(target_type, "__origin__") and target_type.__origin__ is dict:
            # BigQuery returns JSON stored as strings, so we need to parse them
            if isinstance(value, str):
                import json

                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return {}
            elif isinstance(value, dict):
                # Already a dict (shouldn't happen with STRING columns, but handle it)
                return value
            else:
                return {}

        # BigQuery typically returns proper Python types, so use base converter
        return super().convert(value, target_type)


class BigQueryAdapter(DatabaseAdapter):
    """Google BigQuery adapter for SQL testing."""

    def __init__(
        self, project_id: str, dataset_id: str, credentials_path: Optional[str] = None
    ) -> None:
        if not has_bigquery:
            raise ImportError(
                "BigQuery adapter requires google-cloud-bigquery. "
                "Install with: pip install sql-testing-library[bigquery]"
            )

        assert bigquery is not None  # For type checker

        self.project_id = project_id
        self.dataset_id = dataset_id

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(credentials_path)
        else:
            self.client = bigquery.Client(project=project_id)

    def get_sqlglot_dialect(self) -> str:
        """Return BigQuery dialect for sqlglot."""
        return "bigquery"

    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        job = self.client.query(query)
        return job.to_dataframe()

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create temporary table in BigQuery."""
        import time

        temp_table_name = f"temp_{mock_table.get_table_name()}_{int(time.time() * 1000)}"
        table_id = f"{self.project_id}.{self.dataset_id}.{temp_table_name}"

        # Create table schema from mock table
        schema = self._get_bigquery_schema(mock_table)

        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)

        # Insert data
        df = mock_table.to_dataframe()
        if not df.empty:
            # Convert dict columns to JSON strings for BigQuery
            df = self._prepare_dataframe_for_bigquery(df, mock_table)

            job_config = bigquery.LoadJobConfig()
            job = self.client.load_table_from_dataframe(df, table, job_config=job_config)
            job.result()  # Wait for job to complete

        return table_id

    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create temporary table and return both table name and SQL."""
        import time

        temp_table_name = f"temp_{mock_table.get_table_name()}_{int(time.time() * 1000)}"
        table_id = f"{self.project_id}.{self.dataset_id}.{temp_table_name}"

        # Generate CREATE TABLE SQL
        schema = self._get_bigquery_schema(mock_table)
        column_defs = []
        for field in schema:
            column_defs.append(f"`{field.name}` {field.field_type}")

        columns_sql = ",\n  ".join(column_defs)
        create_sql = f"CREATE TABLE `{table_id}` (\n  {columns_sql}\n)"

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
            insert_sql = f"INSERT INTO `{table_id}` VALUES\n{values_sql}"
            full_sql = f"{create_sql};\n\n{insert_sql};"
        else:
            full_sql = create_sql + ";"

        # Actually create the table
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)

        # Insert data if any
        if not df.empty:
            # Convert dict columns to JSON strings for BigQuery
            df = self._prepare_dataframe_for_bigquery(df, mock_table)

            job_config = bigquery.LoadJobConfig()
            job = self.client.load_table_from_dataframe(df, table, job_config=job_config)
            job.result()

        return table_id, full_sql

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Delete temporary tables."""
        for table_name in table_names:
            try:
                self.client.delete_table(table_name)
            except Exception as e:
                logging.warning(f"Warning: Failed to delete table {table_name}: {e}")

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for BigQuery CTE VALUES clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect="bigquery")

    def get_type_converter(self) -> BaseTypeConverter:
        """Get BigQuery-specific type converter."""
        return BigQueryTypeConverter()

    def _get_bigquery_schema(self, mock_table: BaseMockTable) -> List[Any]:
        """Convert mock table schema to BigQuery schema."""
        column_types = mock_table.get_column_types()

        # Type mapping from Python types to BigQuery types
        type_mapping = {
            str: bigquery.enums.SqlTypeNames.STRING,
            int: bigquery.enums.SqlTypeNames.INT64,
            float: bigquery.enums.SqlTypeNames.FLOAT64,
            bool: bigquery.enums.SqlTypeNames.BOOL,
            date: bigquery.enums.SqlTypeNames.DATE,
            datetime: bigquery.enums.SqlTypeNames.DATETIME,
            Decimal: bigquery.enums.SqlTypeNames.NUMERIC,
        }

        schema = []
        for col_name, col_type in column_types.items():
            # Handle Optional types
            if hasattr(col_type, "__origin__") and col_type.__origin__ is Union:
                # Extract the non-None type from Optional[T]
                non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                if non_none_types:
                    col_type = non_none_types[0]

            # Handle List/Array types
            if hasattr(col_type, "__origin__") and col_type.__origin__ is list:
                # Get the element type from List[T]
                element_type = get_args(col_type)[0] if get_args(col_type) else str

                # Map element type to BigQuery type
                element_bq_type = type_mapping.get(element_type, bigquery.enums.SqlTypeNames.STRING)

                # Create field with mode=REPEATED for arrays
                schema.append(bigquery.SchemaField(col_name, element_bq_type, mode="REPEATED"))
            # Handle Dict/Map types
            elif hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
                # BigQuery stores JSON data as STRING type
                schema.append(bigquery.SchemaField(col_name, bigquery.enums.SqlTypeNames.STRING))
            else:
                # Handle scalar types
                bq_type = type_mapping.get(col_type, bigquery.enums.SqlTypeNames.STRING)
                schema.append(bigquery.SchemaField(col_name, bq_type))

        return schema

    def _prepare_dataframe_for_bigquery(
        self, df: "pd.DataFrame", mock_table: BaseMockTable
    ) -> "pd.DataFrame":
        """Prepare DataFrame for BigQuery by converting dict columns to JSON strings."""
        import json

        import pandas as pd

        from .._sql_utils import DecimalEncoder

        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        column_types = mock_table.get_column_types()

        for col_name, col_type in column_types.items():
            # Handle Optional types
            if hasattr(col_type, "__origin__") and col_type.__origin__ is Union:
                # Extract the non-None type from Optional[T]
                non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
                if non_none_types:
                    col_type = non_none_types[0]

            # Check if this is a dict type
            if hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
                # Convert dict values to JSON strings
                def convert_dict_to_json(val):
                    if pd.isna(val) or val is None:
                        return None
                    elif isinstance(val, dict):
                        # Use DecimalEncoder to handle Decimal values in dicts
                        return json.dumps(val, cls=DecimalEncoder)
                    else:
                        return val

                df_copy[col_name] = df_copy[col_name].apply(convert_dict_to_json)

        return df_copy
