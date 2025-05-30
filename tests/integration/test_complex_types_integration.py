"""Integration tests for complex types (arrays, etc.) across all database adapters."""

import os
import unittest
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library.mock_table import BaseMockTable


@dataclass
class ComplexTypes:
    """Test data class for complex types across database engines."""

    # Basic identifier
    id: int

    # Array types - these should fail initially since not implemented
    string_array: List[str]
    int_array: List[int]
    decimal_array: List[Decimal]

    # Optional array types
    optional_string_array: Optional[List[str]] = None
    optional_int_array: Optional[List[int]] = None


class ComplexTypesResult(BaseModel):
    """Result model for complex types test."""

    # Basic identifier
    id: int

    # Array types
    string_array: List[str]
    int_array: List[int]
    decimal_array: List[Decimal]

    # Optional array types
    optional_string_array: Optional[List[str]]
    optional_int_array: Optional[List[int]]


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
class TestComplexTypesIntegration(unittest.TestCase):
    """Integration tests for complex types across all database adapters."""

    def test_athena_complex_types(self):
        """Test complex types with Athena adapter."""

        test_data = [
            ComplexTypes(
                id=1,
                string_array=["hello", "world", "athena"],
                int_array=[1, 2, 3, 42],
                decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                optional_string_array=["optional", "array"],
                optional_int_array=[100, 200],
            ),
            ComplexTypes(
                id=2,
                string_array=["test", "array"],
                int_array=[10, 20],
                decimal_array=[Decimal("99.99")],
                optional_string_array=None,
                optional_int_array=None,
            ),
            ComplexTypes(
                id=3,
                string_array=[],  # Empty array
                int_array=[0],
                decimal_array=[],
                optional_string_array=[],
                optional_int_array=[42],
            ),
        ]

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                ComplexTypesMockTable(test_data, os.getenv("AWS_ATHENA_DATABASE", "test_db"))
            ],
            result_class=ComplexTypesResult,
        )
        def query_athena_complex_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        string_array,
                        int_array,
                        decimal_array,
                        optional_string_array,
                        optional_int_array
                    FROM complex_types
                    ORDER BY id
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_athena_complex_types()

        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["hello", "world", "athena"]
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

    def test_bigquery_complex_types(self):
        """Test complex types with BigQuery adapter."""

        test_data = [
            ComplexTypes(
                id=1,
                string_array=["BigQuery", "arrays", "test"],
                int_array=[1, 2, 3, 42],
                decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                optional_string_array=["optional", "array"],
                optional_int_array=[100, 200],
            ),
            ComplexTypes(
                id=2,
                string_array=["test", "array"],
                int_array=[10, 20],
                decimal_array=[Decimal("99.99")],
                optional_string_array=None,
                optional_int_array=None,
            ),
            ComplexTypes(
                id=3,
                string_array=[],  # Empty array
                int_array=[0],
                decimal_array=[],
                optional_string_array=[],
                optional_int_array=[42],
            ),
        ]

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                ComplexTypesMockTable(test_data, os.getenv("GCP_PROJECT_ID", "test_project"))
            ],
            result_class=ComplexTypesResult,
        )
        def query_bigquery_complex_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        string_array,
                        int_array,
                        decimal_array,
                        optional_string_array,
                        optional_int_array
                    FROM complex_types
                    ORDER BY id
                """,
                execution_database=os.getenv("GCP_PROJECT_ID", "test_project"),
            )

        results = query_bigquery_complex_types()

        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["BigQuery", "arrays", "test"]
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

    def test_redshift_complex_types(self):
        """Test complex types with Redshift adapter."""

        test_data = [
            ComplexTypes(
                id=1,
                string_array=["Redshift", "super", "arrays"],
                int_array=[1, 2, 3, 42],
                decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                optional_string_array=["optional", "array"],
                optional_int_array=[100, 200],
            ),
            ComplexTypes(
                id=2,
                string_array=["test", "array"],
                int_array=[10, 20],
                decimal_array=[Decimal("99.99")],
                optional_string_array=None,
                optional_int_array=None,
            ),
            ComplexTypes(
                id=3,
                string_array=[],  # Empty array
                int_array=[0],
                decimal_array=[],
                optional_string_array=[],
                optional_int_array=[42],
            ),
        ]

        @sql_test(
            adapter_type="redshift",
            mock_tables=[ComplexTypesMockTable(test_data, "test_db")],
            result_class=ComplexTypesResult,
        )
        def query_redshift_complex_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        string_array,
                        int_array,
                        decimal_array,
                        optional_string_array,
                        optional_int_array
                    FROM complex_types
                    ORDER BY id
                """,
                execution_database="test_db",
            )

        results = query_redshift_complex_types()

        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Redshift", "super", "arrays"]
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

    def test_snowflake_complex_types(self):
        """Test complex types with Snowflake adapter."""

        test_data = [
            ComplexTypes(
                id=1,
                string_array=["Snowflake", "variant", "arrays"],
                int_array=[1, 2, 3, 42],
                decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                optional_string_array=["optional", "array"],
                optional_int_array=[100, 200],
            ),
            ComplexTypes(
                id=2,
                string_array=["test", "array"],
                int_array=[10, 20],
                decimal_array=[Decimal("99.99")],
                optional_string_array=None,
                optional_int_array=None,
            ),
            ComplexTypes(
                id=3,
                string_array=[],  # Empty array
                int_array=[0],
                decimal_array=[],
                optional_string_array=[],
                optional_int_array=[42],
            ),
        ]

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[
                ComplexTypesMockTable(test_data, os.getenv("SNOWFLAKE_DATABASE", "test_db"))
            ],
            result_class=ComplexTypesResult,
        )
        def query_snowflake_complex_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        string_array,
                        int_array,
                        decimal_array,
                        optional_string_array,
                        optional_int_array
                    FROM complex_types
                    ORDER BY id
                """,
                execution_database=os.getenv("SNOWFLAKE_DATABASE", "test_db"),
            )

        results = query_snowflake_complex_types()

        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Snowflake", "variant", "arrays"]
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

    def test_trino_complex_types(self):
        """Test complex types with Trino adapter."""

        test_data = [
            ComplexTypes(
                id=1,
                string_array=["Trino", "array", "support"],
                int_array=[1, 2, 3, 42],
                decimal_array=[Decimal("1.5"), Decimal("2.7"), Decimal("3.14")],
                optional_string_array=["optional", "array"],
                optional_int_array=[100, 200],
            ),
            ComplexTypes(
                id=2,
                string_array=["test", "array"],
                int_array=[10, 20],
                decimal_array=[Decimal("99.99")],
                optional_string_array=None,
                optional_int_array=None,
            ),
            ComplexTypes(
                id=3,
                string_array=[],  # Empty array
                int_array=[0],
                decimal_array=[],
                optional_string_array=[],
                optional_int_array=[42],
            ),
        ]

        @sql_test(
            adapter_type="trino",
            mock_tables=[ComplexTypesMockTable(test_data, "memory")],
            result_class=ComplexTypesResult,
        )
        def query_trino_complex_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        string_array,
                        int_array,
                        decimal_array,
                        optional_string_array,
                        optional_int_array
                    FROM complex_types
                    ORDER BY id
                """,
                execution_database="memory",
            )

        results = query_trino_complex_types()

        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.string_array == ["Trino", "array", "support"]
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


if __name__ == "__main__":
    pytest.main([__file__])
