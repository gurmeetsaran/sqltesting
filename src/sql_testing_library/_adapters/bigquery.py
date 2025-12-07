"""BigQuery adapter implementation."""

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
    import pandas as pd
    from google.cloud import bigquery

# Heavy imports moved to function level for better performance
from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter, is_union_type
from .base import DatabaseAdapter


try:
    from google.cloud import bigquery

    has_bigquery = True
except ImportError:
    has_bigquery = False
    bigquery = None  # type: ignore


class BigQueryTypeConverter(BaseTypeConverter):
    """BigQuery-specific type converter."""

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
        """Convert BigQuery result value to target type."""
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
                # BigQuery returns structs as dict-like objects
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
        temp_table_name = self.get_temp_table_name(mock_table)
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
        temp_table_name = self.get_temp_table_name(mock_table)
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
        from .._types import is_struct_type

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
                    # Create nested struct schema
                    nested_fields = self._get_struct_fields(element_type)
                    schema.append(
                        bigquery.SchemaField(
                            col_name,
                            bigquery.enums.SqlTypeNames.STRUCT,
                            mode="REPEATED",
                            fields=nested_fields,
                        )
                    )
                else:
                    # Map element type to BigQuery type
                    element_bq_type = type_mapping.get(
                        element_type, bigquery.enums.SqlTypeNames.STRING
                    )
                    # Create field with mode=REPEATED for arrays
                    schema.append(bigquery.SchemaField(col_name, element_bq_type, mode="REPEATED"))
            # Handle Dict/Map types
            elif hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
                # BigQuery stores JSON data as STRING type
                schema.append(bigquery.SchemaField(col_name, bigquery.enums.SqlTypeNames.STRING))
            # Handle Struct types
            elif is_struct_type(col_type):
                # Create nested struct schema
                nested_fields = self._get_struct_fields(col_type)
                schema.append(
                    bigquery.SchemaField(
                        col_name,
                        bigquery.enums.SqlTypeNames.STRUCT,
                        fields=nested_fields,
                    )
                )
            else:
                # Handle scalar types
                bq_type = type_mapping.get(col_type, bigquery.enums.SqlTypeNames.STRING)
                schema.append(bigquery.SchemaField(col_name, bq_type))

        return schema

    def _get_struct_fields(self, struct_type: Type) -> List[Any]:
        """Convert struct type fields to BigQuery schema fields."""
        from .._types import is_struct_type

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

        fields = []
        type_hints = get_type_hints(struct_type)

        for field_name, field_type in type_hints.items():
            # Handle Optional types (both Optional[X] and X | None)
            if is_union_type(field_type):
                # Extract the non-None type from Optional[T] or T | None
                non_none_types = [arg for arg in get_args(field_type) if arg is not type(None)]
                if non_none_types:
                    field_type = non_none_types[0]

            # Handle nested structs
            if is_struct_type(field_type):
                nested_fields = self._get_struct_fields(field_type)
                fields.append(
                    bigquery.SchemaField(
                        field_name,
                        bigquery.enums.SqlTypeNames.STRUCT,
                        fields=nested_fields,
                    )
                )
            # Handle List types in structs
            elif hasattr(field_type, "__origin__") and field_type.__origin__ is list:
                element_type = get_args(field_type)[0] if get_args(field_type) else str
                if is_struct_type(element_type):
                    # List of structs
                    nested_fields = self._get_struct_fields(element_type)
                    fields.append(
                        bigquery.SchemaField(
                            field_name,
                            bigquery.enums.SqlTypeNames.STRUCT,
                            mode="REPEATED",
                            fields=nested_fields,
                        )
                    )
                else:
                    # List of scalars
                    element_bq_type = type_mapping.get(
                        element_type, bigquery.enums.SqlTypeNames.STRING
                    )
                    fields.append(
                        bigquery.SchemaField(field_name, element_bq_type, mode="REPEATED")
                    )
            # Handle Dict types in structs - BigQuery stores them as JSON strings
            elif hasattr(field_type, "__origin__") and field_type.__origin__ is dict:
                fields.append(bigquery.SchemaField(field_name, bigquery.enums.SqlTypeNames.STRING))
            else:
                # Handle scalar types
                bq_type = type_mapping.get(field_type, bigquery.enums.SqlTypeNames.STRING)
                fields.append(bigquery.SchemaField(field_name, bq_type))

        return fields

    def _prepare_dataframe_for_bigquery(
        self, df: "pd.DataFrame", mock_table: BaseMockTable
    ) -> "pd.DataFrame":
        """Prepare DataFrame for BigQuery.

        Converts dict columns to JSON strings and struct columns to dicts.
        """
        import json
        from dataclasses import is_dataclass

        import pandas as pd

        from .._sql_utils import DecimalEncoder
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
                # Convert struct objects to dictionaries for BigQuery
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

            # Check if this is a dict type
            elif hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
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

    def _dataclass_to_dict(self, obj: Any) -> Any:
        """Recursively convert dataclass to dict, handling nested structs."""
        import json
        from dataclasses import is_dataclass

        from .._sql_utils import DecimalEncoder

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
                    # Convert dict fields to JSON strings for BigQuery
                    result[field] = json.dumps(value, cls=DecimalEncoder)
                else:
                    # Keep other values as-is (including Decimal)
                    result[field] = value
            return result
        else:
            return obj
