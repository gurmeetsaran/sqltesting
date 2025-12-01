"""Unit tests for BigQuery adapter type converter and helper methods."""

from dataclasses import dataclass
from typing import Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel


# Only test if bigquery is available
pytest.importorskip("google.cloud.bigquery")

from sql_testing_library._adapters.bigquery import BigQueryAdapter, BigQueryTypeConverter


@dataclass
class SimpleStruct:
    """Simple struct for testing."""

    id: int
    name: str


class SimplePydanticModel(BaseModel):
    """Pydantic model for testing."""

    id: int
    value: str


class TestBigQueryTypeConverter:
    """Test BigQuery type converter."""

    def test_create_struct_instance_with_dataclass(self):
        """Test creating struct instance from dataclass."""
        converter = BigQueryTypeConverter()

        field_values = {"id": 1, "name": "test"}
        result = converter._create_struct_instance(SimpleStruct, field_values)

        assert isinstance(result, SimpleStruct)
        assert result.id == 1
        assert result.name == "test"

    def test_create_struct_instance_with_pydantic(self):
        """Test creating struct instance from Pydantic model."""
        converter = BigQueryTypeConverter()

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

        converter = BigQueryTypeConverter()

        field_values = {"a": 1, "b": 2}
        result = converter._create_struct_instance(CustomClass, field_values)

        assert isinstance(result, CustomClass)
        assert result.data == field_values

    def test_create_struct_instance_fallback_empty(self):
        """Test fallback with empty constructor."""

        class EmptyClass:
            def __init__(self):
                self.value = "empty"

        converter = BigQueryTypeConverter()

        result = converter._create_struct_instance(EmptyClass, {"bad": "arg"})

        assert isinstance(result, EmptyClass)
        assert result.value == "empty"

    def test_convert_none_value(self):
        """Test converting None values."""
        converter = BigQueryTypeConverter()

        result = converter.convert(None, int)
        assert result is None

    def test_convert_optional_with_none(self):
        """Test converting Optional[T] with None."""
        converter = BigQueryTypeConverter()

        result = converter.convert(None, Optional[int])
        assert result is None

    def test_convert_optional_with_value(self):
        """Test converting Optional[T] with actual value."""
        converter = BigQueryTypeConverter()

        result = converter.convert(42, Optional[int])
        assert result == 42

    def test_convert_struct_from_dict(self):
        """Test converting dict to struct type."""
        converter = BigQueryTypeConverter()

        value_dict = {"id": 1, "name": "test"}
        result = converter.convert(value_dict, SimpleStruct)

        assert isinstance(result, SimpleStruct)
        assert result.id == 1
        assert result.name == "test"

    def test_convert_dict_from_json_string(self):
        """Test converting JSON string to dict (BigQuery specific)."""
        converter = BigQueryTypeConverter()

        # BigQuery can return MAP types as JSON strings
        json_string = '{"key1": "value1", "key2": "value2"}'
        result = converter.convert(json_string, Dict[str, str])

        assert isinstance(result, dict)
        assert result["key1"] == "value1"
        assert result["key2"] == "value2"

    def test_convert_dict_from_invalid_json_returns_empty(self):
        """Test that invalid JSON returns empty dict."""
        converter = BigQueryTypeConverter()

        # Invalid JSON should return empty dict
        result = converter.convert("not valid json", Dict[str, str])

        assert result == {}

    def test_convert_dict_already_dict(self):
        """Test dict conversion when value is already a dict."""
        converter = BigQueryTypeConverter()

        value_dict = {"a": 1, "b": 2}
        result = converter.convert(value_dict, Dict[str, int])

        # Should handle dict that's already a dict
        assert isinstance(result, dict)

    def test_convert_struct_with_nested_fields(self):
        """Test struct conversion with nested field conversion."""

        @dataclass
        class Nested:
            id: int
            data: SimpleStruct

        converter = BigQueryTypeConverter()

        value_dict = {"id": 1, "data": {"id": 2, "name": "inner"}}
        result = converter.convert(value_dict, Nested)

        assert result.id == 1
        assert isinstance(result.data, SimpleStruct)
        assert result.data.name == "inner"

    def test_convert_struct_with_missing_field_sets_none(self):
        """Test that missing fields in struct are set to None."""

        @dataclass
        class WithOptional:
            id: int
            optional: Optional[str]

        converter = BigQueryTypeConverter()

        # Missing 'optional' field
        value_dict = {"id": 1}
        result = converter.convert(value_dict, WithOptional)

        assert result.id == 1
        assert result.optional is None

    def test_convert_non_dict_struct_returns_as_is(self):
        """Test non-dict struct values returned unchanged."""
        converter = BigQueryTypeConverter()

        result = converter.convert("string_value", SimpleStruct)
        assert result == "string_value"


class TestBigQueryAdapterHelperMethods:
    """Test BigQuery adapter helper methods without real connection."""

    def test_get_sqlglot_dialect(self):
        """Test get_sqlglot_dialect returns 'bigquery'."""
        # Mock the client to avoid real connection
        with patch("sql_testing_library._adapters.bigquery.bigquery") as mock_bq:
            mock_client = MagicMock()
            mock_bq.Client.return_value = mock_client

            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")
            assert adapter.get_sqlglot_dialect() == "bigquery"

    def test_get_type_converter(self):
        """Test get_type_converter returns BigQueryTypeConverter."""
        with patch("sql_testing_library._adapters.bigquery.bigquery") as mock_bq:
            mock_client = MagicMock()
            mock_bq.Client.return_value = mock_client

            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")
            converter = adapter.get_type_converter()
            assert isinstance(converter, BigQueryTypeConverter)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
