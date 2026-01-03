"""Integration tests for complex types (arrays, etc.) across all database adapters."""

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class Address:
    """Address structure for testing struct types."""

    street: str
    city: str
    zipcode: str


@dataclass
class ComplexTypes:
    """Test data class for complex types across database engines."""

    # Basic identifier
    id: int

    # Array types
    string_array: List[str]
    int_array: List[int]
    decimal_array: List[Decimal]

    # Struct type
    address: Address

    # Optional array types
    optional_string_array: Optional[List[str]] = None
    optional_int_array: Optional[List[int]] = None


@dataclass
class ComplexTypesNoStruct:
    """Test data class for adapters without struct support (e.g., Snowflake)."""

    # Basic identifier
    id: int

    # Array types
    string_array: List[str]
    int_array: List[int]
    decimal_array: List[Decimal]

    # Optional array types
    optional_string_array: Optional[List[str]] = None
    optional_int_array: Optional[List[int]] = None


class ComplexTypesResultNoStruct(BaseModel):
    """Base result model for complex types test (arrays only)."""

    # Basic identifier
    id: int

    # Array types
    string_array: List[str]
    int_array: List[int]
    decimal_array: List[Decimal]

    # Optional array types
    optional_string_array: Optional[List[str]]
    optional_int_array: Optional[List[int]]


class ComplexTypesResult(ComplexTypesResultNoStruct):
    """Extended result model with struct support for adapters that support it."""

    # Struct type - use the dataclass directly for proper deserialization
    address: Address


class ComplexTypesMockTable(BaseMockTable):
    """Mock table for complex types testing."""

    def __init__(self, data, database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "complex_types"


@pytest.mark.integration
@pytest.mark.parametrize(
    "adapter_type", ["athena", "bigquery", "redshift", "snowflake", "trino", "duckdb"]
)
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestComplexTypesIntegration:
    """Integration tests for complex types across all database adapters.

    Note: Redshift and Snowflake use a different result model without struct types
    since struct support is not yet implemented for these adapters.
    """

    @pytest.fixture(autouse=True)
    def setup_test_data(self, adapter_type):
        """Set up test data specific to each adapter."""
        # Check if adapter supports struct types
        supports_structs = adapter_type not in ["snowflake", "redshift"]

        # Create test data based on struct support
        if supports_structs:
            self.test_data = [
                ComplexTypes(
                    id=1,
                    string_array=[f"{adapter_type.title()}", "arrays", "test"],
                    int_array=[1, 2, 3, 42],
                    decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                    address=Address("123 Main St", "New York", "10001"),
                    optional_string_array=["optional", "array"],
                    optional_int_array=[100, 200],
                ),
                ComplexTypes(
                    id=2,
                    string_array=["test", "array"],
                    int_array=[10, 20],
                    decimal_array=[Decimal("99.99")],
                    address=Address("456 Oak Ave", "Boston", "02101"),
                    optional_string_array=None,
                    optional_int_array=None,
                ),
                ComplexTypes(
                    id=3,
                    string_array=[],  # Empty array
                    int_array=[0],
                    decimal_array=[],
                    address=Address("789 Pine Rd", "Seattle", "98101"),
                    optional_string_array=[],
                    optional_int_array=[42],
                ),
            ]
        else:
            # For adapters without struct support (e.g., Snowflake)
            self.test_data = [
                ComplexTypesNoStruct(
                    id=1,
                    string_array=[f"{adapter_type.title()}", "arrays", "test"],
                    int_array=[1, 2, 3, 42],
                    decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                    optional_string_array=["optional", "array"],
                    optional_int_array=[100, 200],
                ),
                ComplexTypesNoStruct(
                    id=2,
                    string_array=["test", "array"],
                    int_array=[10, 20],
                    decimal_array=[Decimal("99.99")],
                    optional_string_array=None,
                    optional_int_array=None,
                ),
                ComplexTypesNoStruct(
                    id=3,
                    string_array=[],  # Empty array
                    int_array=[0],
                    decimal_array=[],
                    optional_string_array=[],
                    optional_int_array=[42],
                ),
            ]

        # Set database name based on adapter type
        if adapter_type == "athena":
            self.database_name = "test_db"
        elif adapter_type == "bigquery":
            self.database_name = "test-project.test_dataset"
        elif adapter_type == "redshift":
            self.database_name = "test_db"
        elif adapter_type == "snowflake":
            self.database_name = "test_db.sqltesting"
        elif adapter_type == "trino":
            self.database_name = "memory"
        elif adapter_type == "duckdb":
            self.database_name = ""  # DuckDB doesn't need database prefix

    def test_complex_types_comprehensive(self, adapter_type, use_physical_tables):
        """Test all complex types comprehensively for the specified adapter."""

        # Redshift and Snowflake don't support struct types yet
        supports_structs = adapter_type not in ["snowflake", "redshift"]

        # Choose appropriate result model and query based on struct support
        if supports_structs:
            result_class = ComplexTypesResult
            query_sql = """
                SELECT
                    id,
                    string_array,
                    int_array,
                    decimal_array,
                    address,
                    optional_string_array,
                    optional_int_array
                FROM complex_types
                ORDER BY id
            """
        else:
            result_class = ComplexTypesResultNoStruct
            query_sql = """
                SELECT
                    id,
                    string_array,
                    int_array,
                    decimal_array,
                    optional_string_array,
                    optional_int_array
                FROM complex_types
                ORDER BY id
            """

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[ComplexTypesMockTable(self.test_data, self.database_name)],
            result_class=result_class,
        )
        def query_complex_types():
            return TestCase(
                query=query_sql,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_complex_types()
        assert len(results) == 3

        # Verify results based on adapter type
        if adapter_type == "athena":
            self._verify_athena_results(results)
        elif adapter_type == "bigquery":
            self._verify_bigquery_results(results)
        elif adapter_type == "redshift":
            self._verify_redshift_results(results)
        elif adapter_type == "snowflake":
            self._verify_snowflake_results(results)
        elif adapter_type == "trino":
            self._verify_trino_results(results)
        elif adapter_type == "duckdb":
            self._verify_duckdb_results(results)

    def _verify_athena_results(self, results):
        """Verify Athena-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Athena", "arrays", "test"]
        assert row1.int_array == [1, 2, 3, 42]
        assert row1.decimal_array == [Decimal("1.5"), Decimal("2.7"), Decimal("3.14")]
        assert row1.optional_string_array == ["optional", "array"]
        assert row1.optional_int_array == [100, 200]

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_array == ["test", "array"]
        assert row2.int_array == [10, 20]
        assert row2.decimal_array == [Decimal("99.99")]
        assert row2.optional_string_array is None
        assert row2.optional_int_array is None

        # Verify third row (with empty arrays)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_array == []
        assert row3.int_array == [0]
        assert row3.decimal_array == []
        assert row3.optional_string_array == []
        assert row3.optional_int_array == [42]

    def _verify_bigquery_results(self, results):
        """Verify BigQuery-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Bigquery", "arrays", "test"]
        assert row1.int_array == [1, 2, 3, 42]
        assert row1.decimal_array == [Decimal("1.5"), Decimal("2.7"), Decimal("3.14")]
        assert row1.optional_string_array == ["optional", "array"]
        assert row1.optional_int_array == [100, 200]

        # Verify second row (with nulls)
        # Note: BigQuery returns empty arrays [] instead of None for NULL arrays
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_array == ["test", "array"]
        assert row2.int_array == [10, 20]
        assert row2.decimal_array == [Decimal("99.99")]
        assert (
            row2.optional_string_array == []
        )  # BigQuery-specific: NULL arrays become empty arrays
        assert row2.optional_int_array == []  # BigQuery-specific: NULL arrays become empty arrays

        # Verify third row (with empty arrays)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_array == []
        assert row3.int_array == [0]
        assert row3.decimal_array == []
        assert row3.optional_string_array == []
        assert row3.optional_int_array == [42]

    def _verify_redshift_results(self, results):
        """Verify Redshift-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Redshift", "arrays", "test"]
        assert row1.int_array == [1, 2, 3, 42]
        assert row1.decimal_array == [Decimal("1.5"), Decimal("2.7"), Decimal("3.14")]
        assert row1.optional_string_array == ["optional", "array"]
        assert row1.optional_int_array == [100, 200]

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_array == ["test", "array"]
        assert row2.int_array == [10, 20]
        assert row2.decimal_array == [Decimal("99.99")]
        assert row2.optional_string_array is None
        assert row2.optional_int_array is None

        # Verify third row (with empty arrays)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_array == []
        assert row3.int_array == [0]
        assert row3.decimal_array == []
        assert row3.optional_string_array == []
        assert row3.optional_int_array == [42]

    def _verify_snowflake_results(self, results):
        """Verify Snowflake-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Snowflake", "arrays", "test"]
        assert row1.int_array == [1, 2, 3, 42]
        assert row1.decimal_array == [Decimal("1.5"), Decimal("2.7"), Decimal("3.14")]
        assert row1.optional_string_array == ["optional", "array"]
        assert row1.optional_int_array == [100, 200]

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_array == ["test", "array"]
        assert row2.int_array == [10, 20]
        assert row2.decimal_array == [Decimal("99.99")]
        assert row2.optional_string_array is None
        assert row2.optional_int_array is None

        # Verify third row (with empty arrays)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_array == []
        assert row3.int_array == [0]
        assert row3.decimal_array == []
        assert row3.optional_string_array == []
        assert row3.optional_int_array == [42]

    def _verify_trino_results(self, results):
        """Verify Trino-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Trino", "arrays", "test"]
        assert row1.int_array == [1, 2, 3, 42]
        assert row1.decimal_array == [Decimal("1.5"), Decimal("2.7"), Decimal("3.14")]
        assert row1.address.street == "123 Main St"
        assert row1.address.city == "New York"
        assert row1.address.zipcode == "10001"
        assert row1.optional_string_array == ["optional", "array"]
        assert row1.optional_int_array == [100, 200]

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_array == ["test", "array"]
        assert row2.int_array == [10, 20]
        assert row2.decimal_array == [Decimal("99.99")]
        assert row2.address.street == "456 Oak Ave"
        assert row2.address.city == "Boston"
        assert row2.optional_string_array is None
        assert row2.optional_int_array is None

        # Verify third row (with empty arrays)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_array == []
        assert row3.int_array == [0]
        assert row3.decimal_array == []
        assert row3.address.street == "789 Pine Rd"
        assert row3.address.city == "Seattle"
        assert row3.optional_string_array == []
        assert row3.optional_int_array == [42]

    def _verify_duckdb_results(self, results):
        """Verify DuckDB-specific results."""
        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Duckdb", "arrays", "test"]
        assert row1.int_array == [1, 2, 3, 42]
        assert row1.decimal_array == [Decimal("1.5"), Decimal("2.7"), Decimal("3.14")]
        assert row1.address.street == "123 Main St"
        assert row1.address.city == "New York"
        assert row1.address.zipcode == "10001"
        assert row1.optional_string_array == ["optional", "array"]
        assert row1.optional_int_array == [100, 200]

        # Verify second row (with nulls)
        row2 = results[1]
        assert row2.id == 2
        assert row2.string_array == ["test", "array"]
        assert row2.int_array == [10, 20]
        assert row2.decimal_array == [Decimal("99.99")]
        assert row2.address.street == "456 Oak Ave"
        assert row2.address.city == "Boston"
        assert row2.optional_string_array is None
        assert row2.optional_int_array is None

        # Verify third row (with empty arrays)
        row3 = results[2]
        assert row3.id == 3
        assert row3.string_array == []
        assert row3.int_array == [0]
        assert row3.decimal_array == []
        assert row3.address.street == "789 Pine Rd"
        assert row3.address.city == "Seattle"
        assert row3.optional_string_array == []
        assert row3.optional_int_array == [42]


if __name__ == "__main__":
    pytest.main([__file__])
