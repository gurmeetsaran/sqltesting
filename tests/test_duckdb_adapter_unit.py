"""Unit tests for DuckDB adapter type converter and helper methods."""

from dataclasses import dataclass
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library._adapters.duckdb import DuckDBAdapter, DuckDBTypeConverter


@dataclass
class SimpleStruct:
    """Simple struct for testing."""

    id: int
    name: str


@dataclass
class NestedStruct:
    """Nested struct for testing."""

    id: int
    data: SimpleStruct


class SimplePydanticModel(BaseModel):
    """Pydantic model for testing."""

    id: int
    value: str


class TestDuckDBTypeConverter:
    """Test DuckDB type converter."""

    def test_create_struct_instance_with_dataclass(self):
        """Test creating struct instance from dataclass."""
        converter = DuckDBTypeConverter()

        field_values = {"id": 1, "name": "test"}
        result = converter._create_struct_instance(SimpleStruct, field_values)

        assert isinstance(result, SimpleStruct)
        assert result.id == 1
        assert result.name == "test"

    def test_create_struct_instance_with_pydantic(self):
        """Test creating struct instance from Pydantic model."""
        converter = DuckDBTypeConverter()

        field_values = {"id": 1, "value": "test"}
        result = converter._create_struct_instance(SimplePydanticModel, field_values)

        assert isinstance(result, SimplePydanticModel)
        assert result.id == 1
        assert result.value == "test"

    def test_create_struct_instance_fallback(self):
        """Test fallback when type is not dataclass or Pydantic."""

        class CustomClass:
            def __init__(self, **kwargs):
                self.data = kwargs

        converter = DuckDBTypeConverter()

        field_values = {"a": 1, "b": 2}
        result = converter._create_struct_instance(CustomClass, field_values)

        assert isinstance(result, CustomClass)
        assert result.data == field_values

    def test_create_struct_instance_fallback_empty(self):
        """Test fallback with empty constructor when kwargs fail."""

        class EmptyClass:
            def __init__(self):
                self.value = "empty"

        converter = DuckDBTypeConverter()

        # Passing kwargs should fail, fallback to empty constructor
        result = converter._create_struct_instance(EmptyClass, {"bad": "arg"})

        assert isinstance(result, EmptyClass)
        assert result.value == "empty"

    def test_convert_none_value(self):
        """Test converting None values."""
        converter = DuckDBTypeConverter()

        result = converter.convert(None, int)
        assert result is None

        result = converter.convert(None, str)
        assert result is None

    def test_convert_optional_with_none(self):
        """Test converting Optional[T] with None."""
        converter = DuckDBTypeConverter()

        result = converter.convert(None, Optional[int])
        assert result is None

    def test_convert_optional_with_value(self):
        """Test converting Optional[T] with actual value."""
        converter = DuckDBTypeConverter()

        # Mock the base convert to test the Optional unwrapping
        result = converter.convert(42, Optional[int])
        assert result == 42

    def test_convert_struct_from_dict(self):
        """Test converting dict to struct type."""
        converter = DuckDBTypeConverter()

        value_dict = {"id": 1, "name": "test"}
        result = converter.convert(value_dict, SimpleStruct)

        assert isinstance(result, SimpleStruct)
        assert result.id == 1
        assert result.name == "test"

    def test_convert_nested_struct(self):
        """Test converting nested struct."""
        converter = DuckDBTypeConverter()

        value_dict = {"id": 1, "data": {"id": 2, "name": "inner"}}
        result = converter.convert(value_dict, NestedStruct)

        assert isinstance(result, NestedStruct)
        assert result.id == 1
        assert isinstance(result.data, SimpleStruct)
        assert result.data.id == 2
        assert result.data.name == "inner"

    def test_convert_struct_with_missing_fields(self):
        """Test struct conversion with missing optional fields."""

        @dataclass
        class StructWithOptional:
            id: int
            optional_field: Optional[str]

        converter = DuckDBTypeConverter()

        # Missing optional_field
        value_dict = {"id": 1}
        result = converter.convert(value_dict, StructWithOptional)

        assert result.id == 1
        assert result.optional_field is None

    def test_convert_non_dict_struct_returns_as_is(self):
        """Test that non-dict values for struct types are returned as-is."""
        converter = DuckDBTypeConverter()

        # If value is not a dict, return it unchanged
        result = converter.convert("not_a_dict", SimpleStruct)
        assert result == "not_a_dict"


class TestDuckDBAdapterHelperMethods:
    """Test DuckDB adapter helper methods."""

    def test_get_sqlglot_dialect(self):
        """Test get_sqlglot_dialect returns correct dialect."""
        adapter = DuckDBAdapter(database=":memory:")
        assert adapter.get_sqlglot_dialect() == "duckdb"

    def test_get_query_size_limit_returns_none(self):
        """Test that DuckDB has no query size limit."""
        adapter = DuckDBAdapter(database=":memory:")
        assert adapter.get_query_size_limit() is None

    def test_get_type_converter(self):
        """Test get_type_converter returns DuckDBTypeConverter."""
        adapter = DuckDBAdapter(database=":memory:")
        converter = adapter.get_type_converter()
        assert isinstance(converter, DuckDBTypeConverter)

    def test_format_value_for_cte(self):
        """Test format_value_for_cte delegates to format_sql_value."""
        adapter = DuckDBAdapter(database=":memory:")

        # Test various values
        result = adapter.format_value_for_cte("test", str)
        assert "test" in result

        result = adapter.format_value_for_cte(123, int)
        assert "123" in result

    def test_connection_initialization(self):
        """Test adapter initializes with connection."""
        adapter = DuckDBAdapter(database=":memory:")
        assert adapter.connection is not None

    def test_dataclass_to_dict_helper(self):
        """Test _dataclass_to_dict helper method."""

        @dataclass
        class TestData:
            id: int
            name: str
            value: Optional[str] = None

        adapter = DuckDBAdapter(database=":memory:")

        obj = TestData(id=1, name="test", value="data")
        result = adapter._dataclass_to_dict(obj)

        assert result == {"id": 1, "name": "test", "value": "data"}

    def test_dataclass_to_dict_with_none(self):
        """Test _dataclass_to_dict with None values."""

        @dataclass
        class TestData:
            id: int
            optional: Optional[str] = None

        adapter = DuckDBAdapter(database=":memory:")

        obj = TestData(id=1, optional=None)
        result = adapter._dataclass_to_dict(obj)

        assert result == {"id": 1, "optional": None}

    def test_dataclass_to_dict_non_dataclass_returns_original(self):
        """Test that non-dataclass objects are returned as-is."""
        adapter = DuckDBAdapter(database=":memory:")

        # Dict should be returned as-is
        input_dict = {"id": 1, "name": "test"}
        result = adapter._dataclass_to_dict(input_dict)
        assert result == input_dict


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
