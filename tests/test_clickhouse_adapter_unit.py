"""Unit tests for ClickHouse adapter type converter and helper methods.

These tests do NOT require a live ClickHouse server. Where the adapter
constructor is exercised we patch out ``clickhouse_connect.get_client``.
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from sql_testing_library._adapters.clickhouse import (
    CLICKHOUSE_TYPE_MAPPING,
    ClickHouseAdapter,
    ClickHouseTypeConverter,
)
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class SimpleStruct:
    id: int
    name: str


@dataclass
class NestedStruct:
    id: int
    inner: SimpleStruct


class SimplePydantic(BaseModel):
    id: int
    value: str


class _Users(BaseMockTable):
    def get_database_name(self) -> str:
        return "default"

    def get_table_name(self) -> str:
        return "users"


def _make_adapter() -> ClickHouseAdapter:
    """Return a ClickHouseAdapter with clickhouse_connect.get_client patched out."""
    with patch("sql_testing_library._adapters.clickhouse.clickhouse_connect") as mock_module:
        mock_client = MagicMock()
        mock_module.get_client.return_value = mock_client
        adapter = ClickHouseAdapter(host="localhost")
    return adapter


class TestClickHouseTypeConverter:
    def test_convert_none(self):
        c = ClickHouseTypeConverter()
        assert c.convert(None, int) is None
        assert c.convert(None, str) is None
        assert c.convert(None, Optional[int]) is None

    def test_convert_optional_with_value(self):
        c = ClickHouseTypeConverter()
        assert c.convert(42, Optional[int]) == 42

    def test_convert_struct_from_tuple(self):
        c = ClickHouseTypeConverter()
        result = c.convert((1, "alice"), SimpleStruct)
        assert isinstance(result, SimpleStruct)
        assert result.id == 1
        assert result.name == "alice"

    def test_convert_struct_from_dict(self):
        c = ClickHouseTypeConverter()
        result = c.convert({"id": 2, "name": "bob"}, SimpleStruct)
        assert isinstance(result, SimpleStruct)
        assert result.id == 2
        assert result.name == "bob"

    def test_convert_nested_struct_from_tuple(self):
        c = ClickHouseTypeConverter()
        # Outer tuple: (id, inner_tuple)
        result = c.convert((10, (20, "inner")), NestedStruct)
        assert isinstance(result, NestedStruct)
        assert result.id == 10
        assert isinstance(result.inner, SimpleStruct)
        assert result.inner.id == 20
        assert result.inner.name == "inner"

    def test_convert_pydantic_struct(self):
        c = ClickHouseTypeConverter()
        result = c.convert((5, "hello"), SimplePydantic)
        assert isinstance(result, SimplePydantic)
        assert result.id == 5
        assert result.value == "hello"

    def test_convert_struct_short_tuple_pads_none(self):
        c = ClickHouseTypeConverter()
        result = c.convert((1,), SimpleStruct)
        assert result.id == 1
        assert result.name is None

    def test_convert_list_of_structs(self):
        c = ClickHouseTypeConverter()
        raw = [(1, "a"), (2, "b")]
        result = c.convert(raw, List[SimpleStruct])
        assert len(result) == 2
        assert all(isinstance(x, SimpleStruct) for x in result)
        assert result[0].name == "a"
        assert result[1].id == 2

    def test_convert_dict_map(self):
        c = ClickHouseTypeConverter()
        result = c.convert({"a": 1, "b": 2}, Dict[str, int])
        assert result == {"a": 1, "b": 2}


class TestClickHouseAdapterHelpers:
    def test_get_sqlglot_dialect(self):
        adapter = _make_adapter()
        assert adapter.get_sqlglot_dialect() == "clickhouse"

    def test_get_query_size_limit_returns_none(self):
        adapter = _make_adapter()
        assert adapter.get_query_size_limit() is None

    def test_get_type_converter(self):
        adapter = _make_adapter()
        assert isinstance(adapter.get_type_converter(), ClickHouseTypeConverter)

    def test_format_value_for_cte_scalar(self):
        adapter = _make_adapter()
        assert adapter.format_value_for_cte("hi", str) == "'hi'"
        assert adapter.format_value_for_cte(7, int) == "7"
        assert adapter.format_value_for_cte(True, bool) == "TRUE"
        assert adapter.format_value_for_cte(1.5, float) == "1.5"

    def test_format_value_for_cte_date_and_datetime(self):
        adapter = _make_adapter()
        assert adapter.format_value_for_cte(date(2023, 1, 15), date) == "toDate('2023-01-15')"
        dt = datetime(2023, 1, 15, 10, 30, 45, 123456)
        assert adapter.format_value_for_cte(dt, datetime) == (
            "toDateTime64('2023-01-15 10:30:45.123456', 6)"
        )

    def test_format_value_for_cte_decimal(self):
        adapter = _make_adapter()
        result = adapter.format_value_for_cte(Decimal("12.50"), Decimal)
        assert result == "CAST('12.50' AS Decimal(38, 9))"

    def test_format_value_for_cte_null_scalar(self):
        adapter = _make_adapter()
        assert adapter.format_value_for_cte(None, int) == "CAST(NULL AS Nullable(Int64))"
        assert adapter.format_value_for_cte(None, str) == "CAST(NULL AS Nullable(String))"

    def test_format_value_for_cte_list(self):
        adapter = _make_adapter()
        assert adapter.format_value_for_cte([1, 2, 3], List[int]) == "[1, 2, 3]"
        assert adapter.format_value_for_cte([], List[int]) == "CAST([] AS Array(Int64))"

    def test_format_value_for_cte_map(self):
        adapter = _make_adapter()
        result = adapter.format_value_for_cte({"a": 1, "b": 2}, Dict[str, int])
        assert result == "map('a', 1, 'b', 2)"

    def test_format_value_for_cte_empty_map(self):
        adapter = _make_adapter()
        result = adapter.format_value_for_cte({}, Dict[str, int])
        assert result == "CAST(map() AS Map(String, Int64))"

    def test_get_column_sql_type_scalars_wrap_nullable(self):
        """Top-level scalar columns are always wrapped in Nullable()."""
        adapter = _make_adapter()
        for py_type, ch_type in CLICKHOUSE_TYPE_MAPPING.items():
            assert adapter._get_column_sql_type(py_type) == f"Nullable({ch_type})"

    def test_get_column_sql_type_optional_wraps_nullable(self):
        adapter = _make_adapter()
        assert adapter._get_column_sql_type(Optional[int]) == "Nullable(Int64)"
        assert adapter._get_column_sql_type(Optional[str]) == "Nullable(String)"

    def test_get_column_sql_type_inner_is_strict(self):
        """Element/key/value types inside Array/Map are NOT wrapped in Nullable()."""
        adapter = _make_adapter()
        assert adapter._get_column_sql_type(int, top_level=False) == "Int64"

    def test_get_column_sql_type_array_and_map(self):
        adapter = _make_adapter()
        assert adapter._get_column_sql_type(List[int]) == "Array(Int64)"
        assert adapter._get_column_sql_type(List[List[str]]) == "Array(Array(String))"
        assert adapter._get_column_sql_type(Dict[str, int]) == "Map(String, Int64)"

    def test_get_column_sql_type_struct(self):
        """Tuple itself is not Nullable (CH forbids it), but scalar fields inside are."""
        adapter = _make_adapter()
        assert adapter._get_column_sql_type(SimpleStruct) == (
            "Tuple(id Nullable(Int64), name Nullable(String))"
        )
        # Nested Tuple stays non-Nullable, but the inner Tuple's fields are Nullable
        assert adapter._get_column_sql_type(NestedStruct) == (
            "Tuple(id Nullable(Int64), inner Tuple(id Nullable(Int64), name Nullable(String)))"
        )

    def test_generate_create_table_sql(self):
        adapter = _make_adapter()

        @dataclass
        class Row:
            id: int
            name: str
            active: Optional[bool]
            tags: List[str]

        mock = _Users([Row(1, "a", True, ["x"]), Row(2, "b", None, [])])
        sql = adapter._generate_create_table_sql(mock, "tmp_tbl")
        assert "CREATE TABLE tmp_tbl" in sql
        # Every scalar column is wrapped in Nullable so the CTE-mode NULL
        # literals (CAST(NULL AS Nullable(...))) can round-trip through
        # temp tables as well.
        assert "`id` Nullable(Int64)" in sql
        assert "`name` Nullable(String)" in sql
        assert "`active` Nullable(Bool)" in sql
        # Array is never Nullable itself in ClickHouse
        assert "`tags` Array(String)" in sql
        assert "ENGINE = Memory" in sql

    def test_import_error_when_client_missing(self):
        """If clickhouse_connect isn't installed, __init__ raises ImportError."""
        with patch("sql_testing_library._adapters.clickhouse.has_clickhouse_connect", False):
            with pytest.raises(ImportError, match="clickhouse-connect"):
                ClickHouseAdapter(host="localhost")

    def test_cleanup_temp_tables_swallows_errors(self, caplog):
        adapter = _make_adapter()
        adapter.client.command.side_effect = RuntimeError("boom")
        adapter.cleanup_temp_tables(["t1", "t2"])
        # Both drops attempted; RuntimeError is logged, not raised
        assert adapter.client.command.call_count == 2

    def test_execute_query_empty(self):
        adapter = _make_adapter()
        adapter.client.query.return_value = MagicMock(column_names=[], result_rows=[])
        df = adapter.execute_query("SELECT 1 WHERE 0")
        assert df.empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
