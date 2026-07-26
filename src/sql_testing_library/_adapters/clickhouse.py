"""ClickHouse adapter implementation."""

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
    import clickhouse_connect
    import pandas as pd

from .._mock_table import BaseMockTable
from .._types import BaseTypeConverter, is_union_type
from .base import DatabaseAdapter


try:
    import clickhouse_connect

    has_clickhouse_connect = True
except ImportError:
    has_clickhouse_connect = False
    clickhouse_connect = None  # type: ignore


# Type mapping from Python types to ClickHouse types (non-nullable base)
CLICKHOUSE_TYPE_MAPPING = {
    str: "String",
    int: "Int64",
    float: "Float64",
    bool: "Bool",
    date: "Date",
    datetime: "DateTime64(6)",
    Decimal: "Decimal(38, 9)",
}


class ClickHouseTypeConverter(BaseTypeConverter):
    """ClickHouse-specific type converter."""

    def _create_struct_instance(self, struct_type: Type, field_values: dict) -> Any:
        """Create a struct instance from field values."""
        from dataclasses import is_dataclass

        from .._types import is_pydantic_model_class

        if is_dataclass(struct_type):
            return struct_type(**field_values)
        elif is_pydantic_model_class(struct_type):
            return struct_type(**field_values)
        else:
            try:
                return struct_type(**field_values)
            except Exception:
                return struct_type()

    def convert(self, value: Any, target_type: Type) -> Any:
        """Convert ClickHouse result value to target type."""
        from .._types import is_struct_type

        if value is None:
            return None

        was_optional = self.is_optional_type(target_type)
        if was_optional:
            target_type = self.get_optional_inner_type(target_type)

        # ClickHouse Tuple values come back as Python tuples; Map values come
        # back as dicts. Arrays come back as lists.
        if is_struct_type(target_type):
            type_hints = get_type_hints(target_type)
            field_names = list(type_hints.keys())

            # ClickHouse can't wrap Tuple in Nullable, so a NULL struct
            # round-trips as an all-None tuple/dict. When the target is
            # Optional[Struct], treat all-None as None.
            def _all_none(items: Any) -> bool:
                if isinstance(items, dict):
                    return all(v is None or _all_none(v) for v in items.values())
                if isinstance(items, (tuple, list)):
                    return all(v is None or _all_none(v) for v in items)
                return items is None

            if was_optional and _all_none(value):
                return None

            if isinstance(value, dict):
                field_values = {
                    name: self.convert(value.get(name), type_hints[name]) for name in field_names
                }
                return self._create_struct_instance(target_type, field_values)

            if isinstance(value, (tuple, list)):
                field_values = {}
                for idx, name in enumerate(field_names):
                    if idx < len(value):
                        field_values[name] = self.convert(value[idx], type_hints[name])
                    else:
                        field_values[name] = None
                return self._create_struct_instance(target_type, field_values)

            return value

        if hasattr(target_type, "__origin__") and target_type.__origin__ is list:
            if isinstance(value, (list, tuple)):
                element_type = get_args(target_type)[0] if get_args(target_type) else str
                return [self.convert(item, element_type) for item in value]
            return value

        if hasattr(target_type, "__origin__") and target_type.__origin__ is dict:
            if isinstance(value, dict):
                return value
            return value

        return super().convert(value, target_type)


class ClickHouseAdapter(DatabaseAdapter):
    """ClickHouse adapter for SQL testing (uses clickhouse-connect HTTP client)."""

    def __init__(
        self,
        host: str,
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "default",
        secure: bool = False,
        **kwargs: Any,
    ) -> None:
        if not has_clickhouse_connect:
            raise ImportError(
                "ClickHouse adapter requires clickhouse-connect. "
                "Install with: pip install sql-testing-library[clickhouse]"
            )

        assert clickhouse_connect is not None  # For type checker

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.secure = secure

        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            secure=secure,
            **kwargs,
        )

    def get_sqlglot_dialect(self) -> str:
        """Return ClickHouse dialect for sqlglot."""
        return "clickhouse"

    def execute_query(self, query: str) -> "pd.DataFrame":
        """Execute query and return results as DataFrame."""
        import pandas as pd

        result = self.client.query(query)
        if not result.column_names:
            return pd.DataFrame()
        df = pd.DataFrame(result.result_rows)
        df.columns = list(result.column_names)
        return df

    def create_temp_table(self, mock_table: BaseMockTable) -> str:
        """Create a temporary table in ClickHouse using the Memory engine."""
        temp_table_name = self.get_temp_table_name(mock_table)

        create_sql = self._generate_create_table_sql(mock_table, temp_table_name)
        self.client.command(create_sql)

        df = mock_table.to_dataframe()
        if not df.empty:
            self._insert_dataframe(temp_table_name, df, mock_table)

        return temp_table_name

    def create_temp_table_with_sql(self, mock_table: BaseMockTable) -> Tuple[str, str]:
        """Create a temporary table and return both table name and SQL."""
        temp_table_name = self.get_temp_table_name(mock_table)

        create_sql = self._generate_create_table_sql(mock_table, temp_table_name)

        df = mock_table.to_dataframe()
        insert_sql = ""
        if not df.empty:
            values_rows = []
            for _, row in df.iterrows():
                values = []
                for col in df.columns:
                    value = row[col]
                    col_type = mock_table.get_column_types().get(col, str)
                    values.append(self.format_value_for_cte(value, col_type))
                values_rows.append(f"({', '.join(values)})")
            values_sql = ",\n".join(values_rows)
            insert_sql = f"INSERT INTO {temp_table_name} VALUES\n{values_sql}"

        full_sql = create_sql + ";"
        if insert_sql:
            full_sql += f"\n\n{insert_sql};"

        self.client.command(create_sql)

        if not df.empty:
            self._insert_dataframe(temp_table_name, df, mock_table)

        return temp_table_name, full_sql

    def cleanup_temp_tables(self, table_names: List[str]) -> None:
        """Drop temporary tables."""
        for table_name in table_names:
            try:
                self.client.command(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                logging.warning(f"Warning: Failed to drop table {table_name}: {e}")

    def format_value_for_cte(self, value: Any, column_type: type) -> str:
        """Format value for ClickHouse CTE (UNION ALL SELECT) clause."""
        from .._sql_utils import format_sql_value

        return format_sql_value(value, column_type, dialect="clickhouse")

    def get_type_converter(self) -> BaseTypeConverter:
        """Get ClickHouse-specific type converter."""
        return ClickHouseTypeConverter()

    def get_query_size_limit(self) -> Optional[int]:
        """Return query size limit in bytes.

        ClickHouse's default HTTP max_query_size is 256 KiB but is configurable
        server-side, so we don't preemptively fail.
        """
        return None

    def _get_column_sql_type(self, col_type: Type, top_level: bool = True) -> str:
        """Recursively convert a Python type to a ClickHouse SQL type.

        Nullability rules (ClickHouse forbids ``Nullable(Array(...))``,
        ``Nullable(Map(...))``, and ``Nullable(Tuple(...))``):
        - Top-level scalars: wrapped in ``Nullable(...)`` so NULLs can be
          inserted into columns.
        - Arrays / Maps: never Nullable themselves; NULL round-trips as an
          empty array/map (see ``format_sql_value``).
        - Tuples (structs): never Nullable themselves, but their primitive
          fields are wrapped in ``Nullable(...)`` so a NULL struct can be
          emitted as ``tuple(NULL, NULL, ...)``.
        """
        from .._types import is_struct_type

        if is_union_type(col_type):
            non_none_types = [arg for arg in get_args(col_type) if arg is not type(None)]
            if non_none_types:
                col_type = non_none_types[0]

        # Array / List — element type recursed as non-top-level
        if hasattr(col_type, "__origin__") and col_type.__origin__ is list:
            element_type = get_args(col_type)[0] if get_args(col_type) else str
            element_sql = self._get_column_sql_type(element_type, top_level=False)
            return f"Array({element_sql})"

        # Map / Dict — key/value types recursed as non-top-level
        if hasattr(col_type, "__origin__") and col_type.__origin__ is dict:
            key_type = get_args(col_type)[0] if get_args(col_type) else str
            value_type = get_args(col_type)[1] if len(get_args(col_type)) > 1 else str
            key_sql = self._get_column_sql_type(key_type, top_level=False)
            value_sql = self._get_column_sql_type(value_type, top_level=False)
            return f"Map({key_sql}, {value_sql})"

        # Struct — fields wrapped in Nullable() (unless already Nullable, or an
        # Array/Map/Tuple which cannot be Nullable in ClickHouse) so that a
        # NULL struct can round-trip via tuple(NULL, NULL, ...).
        if is_struct_type(col_type):
            hints = get_type_hints(col_type)
            fields = []
            for name, t in hints.items():
                field_sql = self._get_column_sql_type(t, top_level=False)
                if not (
                    field_sql.startswith("Nullable(")
                    or field_sql.startswith("Array(")
                    or field_sql.startswith("Map(")
                    or field_sql.startswith("Tuple(")
                ):
                    field_sql = f"Nullable({field_sql})"
                fields.append(f"{name} {field_sql}")
            return f"Tuple({', '.join(fields)})"

        base = CLICKHOUSE_TYPE_MAPPING.get(col_type, "String")
        return f"Nullable({base})" if top_level else base

    def _generate_create_table_sql(self, mock_table: BaseMockTable, table_name: str) -> str:
        """Generate CREATE TABLE SQL for ClickHouse using the Memory engine."""
        column_types = mock_table.get_column_types()
        column_defs = []
        for col_name, col_type in column_types.items():
            sql_type = self._get_column_sql_type(col_type)
            column_defs.append(f"`{col_name}` {sql_type}")

        columns_sql = ",\n  ".join(column_defs)
        return f"CREATE TABLE {table_name} (\n  {columns_sql}\n) ENGINE = Memory"

    def _insert_dataframe(
        self, table_name: str, df: "pd.DataFrame", mock_table: BaseMockTable
    ) -> None:
        """Insert a DataFrame into a ClickHouse table row-by-row via VALUES.

        Uses formatted SQL literals (rather than `client.insert()`) so we get
        identical value handling to the CTE code path: dataclass/pydantic
        structs, nested lists, and typed NULLs all render consistently.
        """
        column_types = mock_table.get_column_types()

        values_rows = []
        for _, row in df.iterrows():
            values = []
            for col in df.columns:
                col_type = column_types.get(col, str)
                values.append(self.format_value_for_cte(row[col], col_type))
            values_rows.append(f"({', '.join(values)})")

        insert_sql = f"INSERT INTO {table_name} VALUES {', '.join(values_rows)}"
        self.client.command(insert_sql)
