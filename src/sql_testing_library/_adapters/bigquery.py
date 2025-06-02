"""BigQuery adapter implementation."""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, List, Optional, Type, Union, get_args


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
            job_config = bigquery.LoadJobConfig()
            job = self.client.load_table_from_dataframe(df, table, job_config=job_config)
            job.result()  # Wait for job to complete

        return table_id

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

            bq_type = type_mapping.get(col_type, bigquery.enums.SqlTypeNames.STRING)
            schema.append(bigquery.SchemaField(col_name, bq_type))

        return schema
