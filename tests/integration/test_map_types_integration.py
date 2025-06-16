"""Integration tests for map types across database adapters that support them."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class MapTypes:
    """Test data class for map types across database engines."""

    # Basic identifier
    id: int

    # Map types - these should work for Athena and Trino
    string_map: Dict[str, str]
    int_map: Dict[str, int]
    decimal_map: Dict[str, Decimal]
    mixed_map: Dict[int, str]  # Different key and value types

    # Optional map types
    optional_string_map: Optional[Dict[str, str]] = None
    optional_int_map: Optional[Dict[str, int]] = None


class MapTypesResult(BaseModel):
    """Result model for map types test."""

    # Basic identifier
    id: int

    # Map types
    string_map: Dict[str, str]
    int_map: Dict[str, int]
    decimal_map: Dict[str, Decimal]
    mixed_map: Dict[int, str]

    # Optional map types
    optional_string_map: Optional[Dict[str, str]]
    optional_int_map: Optional[Dict[str, int]]


class MapTypesMockTable(BaseMockTable):
    """Mock table for map types testing."""

    def __init__(self, data, database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "map_types"


@pytest.mark.integration
@pytest.mark.parametrize("adapter_type", ["athena", "trino", "redshift", "bigquery", "snowflake"])
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestMapTypesIntegration:
    """Integration tests for map types across supported database adapters."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self, adapter_type):
        """Set up test data specific to each adapter."""
        # Common test data structure for all adapters
        self.test_data = [
            MapTypes(
                id=1,
                string_map={"key1": adapter_type.title(), "key2": "maps", "key3": "test"},
                int_map={"count": 42, "total": 100, "items": 3},
                decimal_map={
                    "price": Decimal("19.99"),
                    "tax": Decimal("1.50"),
                    "total": Decimal("21.49"),
                },
                mixed_map={1: "first", 2: "second", 3: "third"},
                optional_string_map={"optional": "value", "test": "data"},
                optional_int_map={"a": 100, "b": 200},
            ),
            MapTypes(
                id=2,
                string_map={"hello": "world"},
                int_map={"single": 1},
                decimal_map={"amount": Decimal("99.99")},
                mixed_map={10: "ten"},
                optional_string_map=None,
                optional_int_map=None,
            ),
            MapTypes(
                id=3,
                string_map={},  # Empty map
                int_map={"zero": 0},
                decimal_map={},
                mixed_map={42: "answer"},
                optional_string_map={},
                optional_int_map={"value": 42},
            ),
        ]

        # Set database name based on adapter type
        if adapter_type == "athena":
            self.database_name = "test_db"
        elif adapter_type == "trino":
            self.database_name = "memory"
        elif adapter_type == "redshift":
            self.database_name = "public"
        elif adapter_type == "bigquery":
            self.database_name = "test_dataset"
        elif adapter_type == "snowflake":
            self.database_name = "PUBLIC"

    def test_map_types_comprehensive(self, adapter_type, use_physical_tables):
        """Test all map types comprehensively for the specified adapter."""

        # Skip physical table mode for Snowflake due to known issues
        if adapter_type == "snowflake" and use_physical_tables:
            pytest.skip("Snowflake has known issues with physical table mode")

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[MapTypesMockTable(self.test_data, self.database_name)],
            result_class=MapTypesResult,
        )
        def query_map_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        string_map,
                        int_map,
                        decimal_map,
                        mixed_map,
                        optional_string_map,
                        optional_int_map
                    FROM map_types
                    ORDER BY id
                """,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_map_types()
        assert len(results) == 3

        # Verify results based on adapter type
        if adapter_type == "athena":
            self._verify_athena_results(results)
        elif adapter_type == "trino":
            self._verify_trino_results(results)
        elif adapter_type == "redshift":
            self._verify_redshift_results(results)
        elif adapter_type == "bigquery":
            self._verify_bigquery_results(results)
        elif adapter_type == "snowflake":
            self._verify_snowflake_results(results)

    def _verify_athena_results(self, results):
        """Verify Athena-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_map == {"key1": "Athena", "key2": "maps", "key3": "test"}
        assert row1.int_map == {"count": 42, "total": 100, "items": 3}
        assert row1.decimal_map == {
            "price": Decimal("19.99"),
            "tax": Decimal("1.50"),
            "total": Decimal("21.49"),
        }
        assert row1.mixed_map == {1: "first", 2: "second", 3: "third"}
        assert row1.optional_string_map == {"optional": "value", "test": "data"}
        assert row1.optional_int_map == {"a": 100, "b": 200}

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_map == {"hello": "world"}
        assert row2.int_map == {"single": 1}
        assert row2.decimal_map == {"amount": Decimal("99.99")}
        assert row2.mixed_map == {10: "ten"}
        assert row2.optional_string_map is None
        assert row2.optional_int_map is None

        # Verify third row (with empty maps)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_map == {}
        assert row3.int_map == {"zero": 0}
        assert row3.decimal_map == {}
        assert row3.mixed_map == {42: "answer"}
        assert row3.optional_string_map == {}
        assert row3.optional_int_map == {"value": 42}

    def _verify_trino_results(self, results):
        """Verify Trino-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_map == {"key1": "Trino", "key2": "maps", "key3": "test"}
        assert row1.int_map == {"count": 42, "total": 100, "items": 3}
        assert row1.decimal_map == {
            "price": Decimal("19.99"),
            "tax": Decimal("1.50"),
            "total": Decimal("21.49"),
        }
        assert row1.mixed_map == {1: "first", 2: "second", 3: "third"}
        assert row1.optional_string_map == {"optional": "value", "test": "data"}
        assert row1.optional_int_map == {"a": 100, "b": 200}

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_map == {"hello": "world"}
        assert row2.int_map == {"single": 1}
        assert row2.decimal_map == {"amount": Decimal("99.99")}
        assert row2.mixed_map == {10: "ten"}
        assert row2.optional_string_map is None
        assert row2.optional_int_map is None

        # Verify third row (with empty maps)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_map == {}
        assert row3.int_map == {"zero": 0}
        assert row3.decimal_map == {}
        assert row3.mixed_map == {42: "answer"}
        assert row3.optional_string_map == {}
        assert row3.optional_int_map == {"value": 42}

    def _verify_redshift_results(self, results):
        """Verify Redshift-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_map == {"key1": "Redshift", "key2": "maps", "key3": "test"}
        assert row1.int_map == {"count": 42, "total": 100, "items": 3}
        assert row1.decimal_map == {
            "price": Decimal("19.99"),
            "tax": Decimal("1.50"),
            "total": Decimal("21.49"),
        }
        # Redshift preserves integer keys in JSON
        assert row1.mixed_map == {1: "first", 2: "second", 3: "third"}
        assert row1.optional_string_map == {"optional": "value", "test": "data"}
        assert row1.optional_int_map == {"a": 100, "b": 200}

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_map == {"hello": "world"}
        assert row2.int_map == {"single": 1}
        assert row2.decimal_map == {"amount": Decimal("99.99")}
        assert row2.mixed_map == {10: "ten"}
        assert row2.optional_string_map is None
        assert row2.optional_int_map is None

        # Verify third row (with empty maps)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_map == {}
        assert row3.int_map == {"zero": 0}
        assert row3.decimal_map == {}
        assert row3.mixed_map == {42: "answer"}
        assert row3.optional_string_map == {}
        assert row3.optional_int_map == {"value": 42}

    def _verify_bigquery_results(self, results):
        """Verify BigQuery-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_map == {"key1": "Bigquery", "key2": "maps", "key3": "test"}
        assert row1.int_map == {"count": 42, "total": 100, "items": 3}
        assert row1.decimal_map == {
            "price": Decimal("19.99"),
            "tax": Decimal("1.50"),
            "total": Decimal("21.49"),
        }
        # BigQuery preserves integer keys in JSON
        assert row1.mixed_map == {1: "first", 2: "second", 3: "third"}
        assert row1.optional_string_map == {"optional": "value", "test": "data"}
        assert row1.optional_int_map == {"a": 100, "b": 200}

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_map == {"hello": "world"}
        assert row2.int_map == {"single": 1}
        assert row2.decimal_map == {"amount": Decimal("99.99")}
        assert row2.mixed_map == {10: "ten"}
        assert row2.optional_string_map is None
        assert row2.optional_int_map is None

        # Verify third row (with empty maps)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_map == {}
        assert row3.int_map == {"zero": 0}
        assert row3.decimal_map == {}
        assert row3.mixed_map == {42: "answer"}
        assert row3.optional_string_map == {}
        assert row3.optional_int_map == {"value": 42}

    def _verify_snowflake_results(self, results):
        """Verify Snowflake-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_map == {"key1": "Snowflake", "key2": "maps", "key3": "test"}
        assert row1.int_map == {"count": 42, "total": 100, "items": 3}
        assert row1.decimal_map == {
            "price": Decimal("19.99"),
            "tax": Decimal("1.50"),
            "total": Decimal("21.49"),
        }
        # Snowflake preserves integer keys in JSON
        assert row1.mixed_map == {1: "first", 2: "second", 3: "third"}
        assert row1.optional_string_map == {"optional": "value", "test": "data"}
        assert row1.optional_int_map == {"a": 100, "b": 200}

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_map == {"hello": "world"}
        assert row2.int_map == {"single": 1}
        assert row2.decimal_map == {"amount": Decimal("99.99")}
        assert row2.mixed_map == {10: "ten"}
        assert row2.optional_string_map is None
        assert row2.optional_int_map is None

        # Verify third row (with empty maps)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_map == {}
        assert row3.int_map == {"zero": 0}
        assert row3.decimal_map == {}
        assert row3.mixed_map == {42: "answer"}
        assert row3.optional_string_map == {}
        assert row3.optional_int_map == {"value": 42}


if __name__ == "__main__":
    pytest.main([__file__])
