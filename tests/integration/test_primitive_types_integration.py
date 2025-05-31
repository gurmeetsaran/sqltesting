"""Integration tests for primitive types across all database adapters."""

import os
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class PrimitiveTypes:
    """Test data class covering all common primitive types across database engines."""

    # Integer types
    int_col: int

    # Floating point types
    float_col: float

    # Decimal type
    decimal_col: Decimal

    # String types
    string_col: str
    varchar_col: str

    # Boolean type
    boolean_col: bool

    # Date and time types
    date_col: date
    timestamp_col: datetime

    # Optional types (nullable)
    optional_string: Optional[str] = None
    optional_int: Optional[int] = None
    optional_decimal: Optional[Decimal] = None
    optional_bool: Optional[bool] = None
    optional_date: Optional[date] = None
    optional_timestamp: Optional[datetime] = None


class PrimitiveTypesResult(BaseModel):
    """Result model for primitive types test."""

    # Integer types
    int_col: int

    # Floating point types
    float_col: float

    # Decimal type
    decimal_col: Decimal

    # String types
    string_col: str
    varchar_col: str

    # Boolean type
    boolean_col: bool

    # Date and time types
    date_col: date
    timestamp_col: datetime

    # Optional types (nullable)
    optional_string: Optional[str]
    optional_int: Optional[int]
    optional_decimal: Optional[Decimal]
    optional_bool: Optional[bool]
    optional_date: Optional[date]
    optional_timestamp: Optional[datetime]


class PrimitiveTypesMockTable(BaseMockTable):
    """Mock table for primitive types testing."""

    def __init__(self, data, database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "primitive_types"


@pytest.mark.integration
@pytest.mark.parametrize("adapter_type", ["athena", "bigquery", "redshift", "trino"])
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestPrimitiveTypesIntegration:
    """Integration tests for primitive types across all database adapters."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self, adapter_type):
        """Set up test data specific to each adapter."""
        if adapter_type == "athena":
            self.test_data = [
                PrimitiveTypes(
                    int_col=2147483647,  # Max 32-bit int
                    float_col=3.14159265359,
                    decimal_col=Decimal("123456.789"),
                    string_col="Hello Athena",
                    varchar_col="VARCHAR test",
                    boolean_col=True,
                    date_col=date(2023, 12, 25),
                    timestamp_col=datetime(2023, 12, 25, 15, 30, 45),
                    optional_string="Not null",
                    optional_int=42,
                    optional_decimal=Decimal("999.99"),
                    optional_bool=False,
                    optional_date=date(2023, 6, 15),
                    optional_timestamp=datetime(2023, 6, 15, 12, 0, 0),
                ),
                PrimitiveTypes(
                    int_col=-2147483648,  # Min 32-bit int
                    float_col=-2.71828,
                    decimal_col=Decimal("-987654.321"),
                    string_col="Test with 'quotes'",
                    varchar_col="Special chars: !@#$%^&*()",
                    boolean_col=False,
                    date_col=date(2024, 1, 1),
                    timestamp_col=datetime(2024, 1, 1, 0, 0, 0),
                    optional_string=None,
                    optional_int=None,
                    optional_decimal=None,
                    optional_bool=None,
                    optional_date=None,
                    optional_timestamp=None,
                ),
                PrimitiveTypes(
                    int_col=0,
                    float_col=0.0,
                    decimal_col=Decimal("0"),
                    string_col="",
                    varchar_col="Unicode test: 你好世界",
                    boolean_col=True,
                    date_col=date(1970, 1, 1),  # Unix epoch
                    timestamp_col=datetime(1970, 1, 1, 0, 0, 1),
                    optional_string="",
                    optional_int=0,
                    optional_decimal=Decimal("0"),
                    optional_bool=False,
                    optional_date=date(2000, 2, 29),  # Leap year
                    optional_timestamp=datetime(2000, 2, 29, 23, 59, 59),
                ),
            ]
            self.database_name = os.getenv("AWS_ATHENA_DATABASE", "test_db")
            self.expected_results = 3

        elif adapter_type == "bigquery":
            self.test_data = [
                PrimitiveTypes(
                    int_col=9223372036854775807,  # Max 64-bit int
                    float_col=1.7976931348623157e308,  # Near max float64
                    decimal_col=Decimal("99999999999999999999999999999.999999999"),
                    string_col="Hello BigQuery",
                    varchar_col="BigQuery string test",
                    boolean_col=True,
                    date_col=date(9999, 12, 31),  # Max date
                    timestamp_col=datetime(9999, 12, 31, 23, 59, 59),
                    optional_string="BigQuery optional",
                    optional_int=123456789,
                    optional_decimal=Decimal("12345.6789"),
                    optional_bool=True,
                    optional_date=date(2023, 7, 20),
                    optional_timestamp=datetime(2023, 7, 20, 14, 30, 0),
                ),
                PrimitiveTypes(
                    int_col=-9223372036854775808,  # Min 64-bit int
                    float_col=-1.7976931348623157e308,  # Near min float64
                    decimal_col=Decimal("-99999999999999999999999999999.999999999"),
                    string_col="BigQuery 'quoted' string",
                    varchar_col="Newline\nTab\tCarriage\rReturn",
                    boolean_col=False,
                    date_col=date(1, 1, 1),  # Min date
                    timestamp_col=datetime(1, 1, 1, 0, 0, 0),
                    optional_string=None,
                    optional_int=None,
                    optional_decimal=None,
                    optional_bool=None,
                    optional_date=None,
                    optional_timestamp=None,
                ),
            ]
            self.database_name = os.getenv("GCP_PROJECT_ID", "test_project")
            self.expected_results = 2

        elif adapter_type == "redshift":
            self.test_data = [
                PrimitiveTypes(
                    int_col=2147483647,  # Max int
                    float_col=1.234567890123456789,
                    decimal_col=Decimal("999999999999999999999999999.999999999"),
                    string_col="Hello Redshift",
                    varchar_col="Redshift varchar test",
                    boolean_col=True,
                    date_col=date(2262, 4, 11),  # Near max date
                    timestamp_col=datetime(2262, 4, 11, 23, 47, 16),
                    optional_string="Redshift optional",
                    optional_int=987654321,
                    optional_decimal=Decimal("54321.12345"),
                    optional_bool=True,
                    optional_date=date(2023, 8, 15),
                    optional_timestamp=datetime(2023, 8, 15, 16, 45, 30),
                ),
                PrimitiveTypes(
                    int_col=-2147483648,  # Min int
                    float_col=-9.87654321e-10,
                    decimal_col=Decimal("-999999999999999999999999999.999999999"),
                    string_col="Redshift with\ttabs and\nnewlines",
                    varchar_col="Special: áéíóú çñü",
                    boolean_col=False,
                    date_col=date(1900, 1, 1),
                    timestamp_col=datetime(1900, 1, 1, 0, 0, 0),
                    optional_string=None,
                    optional_int=None,
                    optional_decimal=None,
                    optional_bool=None,
                    optional_date=None,
                    optional_timestamp=None,
                ),
            ]
            self.database_name = "test_db"
            self.expected_results = 2

        elif adapter_type == "trino":
            self.test_data = [
                PrimitiveTypes(
                    int_col=9223372036854775807,  # Max bigint
                    float_col=1.23456789012345,
                    decimal_col=Decimal("12345678901234567890123456789.123456789"),
                    string_col="Hello Trino",
                    varchar_col="Trino varchar test",
                    boolean_col=True,
                    date_col=date(2023, 7, 11),  # Max date for Trino
                    timestamp_col=datetime(2023, 12, 31, 23, 59, 59),
                    optional_string="Trino optional",
                    optional_int=555666777,
                    optional_decimal=Decimal("98765.43210"),
                    optional_bool=True,
                    optional_date=date(2023, 9, 10),
                    optional_timestamp=datetime(2023, 9, 10, 18, 30, 45),
                ),
                PrimitiveTypes(
                    int_col=-9223372036854775808,  # Min bigint
                    float_col=-1.23456789012345e-10,
                    decimal_col=Decimal("-12345678901234567890123456789.123456789"),
                    string_col="Trino string with 'single' and \"double\" quotes",
                    varchar_col='JSON-like: {"key": "value"}',
                    boolean_col=False,
                    date_col=date(1582, 10, 15),  # Gregorian calendar start
                    timestamp_col=datetime(1582, 10, 15, 0, 0, 0),
                    optional_string=None,
                    optional_int=None,
                    optional_decimal=None,
                    optional_bool=None,
                    optional_date=None,
                    optional_timestamp=None,
                ),
            ]
            self.database_name = "memory"
            self.expected_results = 2

    def test_primitive_types_comprehensive(self, adapter_type, use_physical_tables):
        """Test all primitive types comprehensively for the specified adapter."""

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[PrimitiveTypesMockTable(self.test_data, self.database_name)],
            result_class=PrimitiveTypesResult,
        )
        def query_primitive_types():
            return TestCase(
                query="""
                    SELECT
                        int_col,
                        float_col,
                        decimal_col,
                        string_col,
                        varchar_col,
                        boolean_col,
                        date_col,
                        timestamp_col,
                        optional_string,
                        optional_int,
                        optional_decimal,
                        optional_bool,
                        optional_date,
                        optional_timestamp
                    FROM primitive_types
                    ORDER BY int_col DESC
                """,
                execution_database=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_primitive_types()
        assert len(results) == self.expected_results

        # Test basic properties based on adapter type
        if adapter_type == "athena":
            self._verify_athena_results(results)
        elif adapter_type == "bigquery":
            self._verify_bigquery_results(results)
        elif adapter_type == "redshift":
            self._verify_redshift_results(results)
        elif adapter_type == "trino":
            self._verify_trino_results(results)

    def _verify_athena_results(self, results):
        """Verify Athena-specific results."""
        # Verify max int row
        max_row = results[0]
        assert max_row.int_col == 2147483647
        assert max_row.float_col == pytest.approx(3.14159265359, rel=1e-9)
        assert max_row.decimal_col == Decimal("123456.789")
        assert max_row.string_col == "Hello Athena"
        assert max_row.varchar_col == "VARCHAR test"
        assert max_row.boolean_col is True
        assert max_row.date_col == date(2023, 12, 25)
        assert max_row.timestamp_col == datetime(2023, 12, 25, 15, 30, 45)
        assert max_row.optional_string == "Not null"
        assert max_row.optional_int == 42
        assert max_row.optional_decimal == Decimal("999.99")
        assert max_row.optional_bool is False
        assert max_row.optional_date == date(2023, 6, 15)
        assert max_row.optional_timestamp == datetime(2023, 6, 15, 12, 0, 0)

        # Verify zero row
        zero_row = results[1]
        assert zero_row.int_col == 0
        assert zero_row.float_col == 0.0
        assert zero_row.decimal_col == Decimal("0")
        assert zero_row.string_col == ""
        assert zero_row.varchar_col == "Unicode test: 你好世界"
        assert zero_row.boolean_col is True
        assert zero_row.date_col == date(1970, 1, 1)
        assert zero_row.timestamp_col == datetime(1970, 1, 1, 0, 0, 1)
        assert zero_row.optional_string == ""
        assert zero_row.optional_int == 0
        assert zero_row.optional_decimal == Decimal("0")
        assert zero_row.optional_bool is False
        assert zero_row.optional_date == date(2000, 2, 29)
        assert zero_row.optional_timestamp == datetime(2000, 2, 29, 23, 59, 59)

        # Verify min int row with nulls
        min_row = results[2]
        assert min_row.int_col == -2147483648
        assert min_row.float_col == pytest.approx(-2.71828, rel=1e-5)
        assert min_row.decimal_col == Decimal("-987654.321")
        assert min_row.string_col == "Test with 'quotes'"
        assert min_row.varchar_col == "Special chars: !@#$%^&*()"
        assert min_row.boolean_col is False
        assert min_row.date_col == date(2024, 1, 1)
        assert min_row.timestamp_col == datetime(2024, 1, 1, 0, 0, 0)
        assert min_row.optional_string is None
        assert min_row.optional_int is None
        assert min_row.optional_decimal is None
        assert min_row.optional_bool is None
        assert min_row.optional_date is None
        assert min_row.optional_timestamp is None

    def _verify_bigquery_results(self, results):
        """Verify BigQuery-specific results."""
        # Verify max values row
        max_row = results[0]
        assert max_row.int_col == 9223372036854775807
        assert max_row.float_col == 1.7976931348623157e308
        # BigQuery returns scientific notation in CTE mode, exact value in physical tables mode
        assert max_row.decimal_col in [
            Decimal("1E+29"),
            Decimal("99999999999999999999999999999.999999999"),
        ]
        assert max_row.string_col == "Hello BigQuery"
        assert max_row.varchar_col == "BigQuery string test"
        assert max_row.boolean_col is True
        assert max_row.date_col == date(9999, 12, 31)
        assert max_row.timestamp_col == datetime(9999, 12, 31, 23, 59, 59)
        assert max_row.optional_string == "BigQuery optional"
        assert max_row.optional_int == 123456789
        assert max_row.optional_decimal == Decimal("12345.6789")
        assert max_row.optional_bool is True
        assert max_row.optional_date == date(2023, 7, 20)
        assert max_row.optional_timestamp == datetime(2023, 7, 20, 14, 30, 0)

        # Verify min values row with nulls
        min_row = results[1]
        assert min_row.int_col == -9223372036854775808
        assert min_row.float_col == -1.7976931348623157e308
        # BigQuery returns scientific notation in CTE mode, exact value in physical tables mode
        assert min_row.decimal_col in [
            Decimal("-1E+29"),
            Decimal("-99999999999999999999999999999.999999999"),
        ]
        assert min_row.string_col == "BigQuery 'quoted' string"
        assert min_row.varchar_col == "Newline\nTab\tCarriage\rReturn"
        assert min_row.boolean_col is False
        assert min_row.date_col == date(1, 1, 1)
        assert min_row.timestamp_col == datetime(1, 1, 1, 0, 0, 0)
        assert min_row.optional_string is None
        assert min_row.optional_int is None
        assert min_row.optional_decimal is None
        assert min_row.optional_bool is None
        assert min_row.optional_date is None
        assert min_row.optional_timestamp is None

    def _verify_redshift_results(self, results):
        """Verify Redshift-specific results."""
        # Verify max values row
        max_row = results[0]
        assert max_row.int_col == 2147483647
        assert max_row.float_col == 1.234567890123456789
        assert max_row.decimal_col == Decimal("999999999999999999999999999.999999999")
        assert max_row.string_col == "Hello Redshift"
        assert max_row.varchar_col == "Redshift varchar test"
        assert max_row.boolean_col is True
        assert max_row.date_col == date(2262, 4, 11)
        assert max_row.timestamp_col == datetime(2262, 4, 11, 23, 47, 16)
        assert max_row.optional_string == "Redshift optional"
        assert max_row.optional_int == 987654321
        assert max_row.optional_decimal == Decimal("54321.12345")
        assert max_row.optional_bool is True
        assert max_row.optional_date == date(2023, 8, 15)
        assert max_row.optional_timestamp == datetime(2023, 8, 15, 16, 45, 30)

        # Verify min values row with nulls
        min_row = results[1]
        assert min_row.int_col == -2147483648
        assert min_row.float_col == -9.87654321e-10
        assert min_row.decimal_col == Decimal("-999999999999999999999999999.999999999")
        assert min_row.string_col == "Redshift with\ttabs and\nnewlines"
        assert min_row.varchar_col == "Special: áéíóú çñü"
        assert min_row.boolean_col is False
        assert min_row.date_col == date(1900, 1, 1)
        assert min_row.timestamp_col == datetime(1900, 1, 1, 0, 0, 0)
        assert min_row.optional_string is None
        assert min_row.optional_int is None
        assert min_row.optional_decimal is None
        assert min_row.optional_bool is None
        assert min_row.optional_date is None
        assert min_row.optional_timestamp is None

    def _verify_trino_results(self, results):
        """Verify Trino-specific results."""
        # Verify max values row
        max_row = results[0]
        assert max_row.int_col == 9223372036854775807
        assert max_row.float_col == 1.23456789012345
        assert max_row.decimal_col == Decimal("12345678901234567890123456789.123456789")
        assert max_row.string_col == "Hello Trino"
        assert max_row.varchar_col == "Trino varchar test"
        assert max_row.boolean_col is True
        assert max_row.date_col == date(2023, 7, 11)
        assert max_row.timestamp_col == datetime(2023, 12, 31, 23, 59, 59)
        assert max_row.optional_string == "Trino optional"
        assert max_row.optional_int == 555666777
        assert max_row.optional_decimal == Decimal("98765.43210")
        assert max_row.optional_bool is True
        assert max_row.optional_date == date(2023, 9, 10)
        assert max_row.optional_timestamp == datetime(2023, 9, 10, 18, 30, 45)

        # Verify min values row with nulls
        min_row = results[1]
        assert min_row.int_col == -9223372036854775808
        assert min_row.float_col == -1.23456789012345e-10
        assert min_row.decimal_col == Decimal("-12345678901234567890123456789.123456789")
        assert min_row.string_col == "Trino string with 'single' and \"double\" quotes"
        assert min_row.varchar_col == 'JSON-like: {"key": "value"}'
        assert min_row.boolean_col is False
        assert min_row.date_col == date(1582, 10, 15)
        assert min_row.timestamp_col == datetime(1582, 10, 15, 0, 0, 0)
        assert min_row.optional_string is None
        assert min_row.optional_int is None
        assert min_row.optional_decimal is None
        assert min_row.optional_bool is None
        assert min_row.optional_date is None
        assert min_row.optional_timestamp is None

    def test_all_optional_fields_none(self, adapter_type, use_physical_tables):
        """Test model with all optional fields set to None values."""

        @dataclass
        class AllOptionalModel:
            """Model with all fields as optional - used for both input data and results."""

            optional_int: Optional[int] = None
            optional_float: Optional[float] = None
            optional_string: Optional[str] = None
            optional_bool: Optional[bool] = None
            optional_date: Optional[date] = None
            optional_timestamp: Optional[datetime] = None
            optional_decimal: Optional[Decimal] = None

        class AllOptionalMockTable(BaseMockTable):
            """Mock table for all optional fields test."""

            def __init__(self, data, database_name: str):
                super().__init__(data)
                self._database_name = database_name

            def get_database_name(self) -> str:
                return self._database_name

            def get_table_name(self) -> str:
                return "all_optional_table"

        # Create test data with all None values
        test_data = [
            AllOptionalModel(),  # All fields default to None
            AllOptionalModel(),  # Second row, also all None
        ]

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[AllOptionalMockTable(test_data, self.database_name)],
            result_class=AllOptionalModel,  # Reuse the same model for results
        )
        def query_all_optional_none():
            return TestCase(
                query="""
                    SELECT
                        optional_int,
                        optional_float,
                        optional_string,
                        optional_bool,
                        optional_date,
                        optional_timestamp,
                        optional_decimal
                    FROM all_optional_table
                    ORDER BY optional_int
                """,
                execution_database=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_all_optional_none()

        # Should return 2 rows
        assert len(results) == 2

        # Verify both rows have all None values
        for row in results:
            assert row.optional_int is None
            assert row.optional_float is None
            assert row.optional_string is None
            assert row.optional_bool is None
            assert row.optional_date is None
            assert row.optional_timestamp is None
            assert row.optional_decimal is None


# Snowflake primitive types test - separate class without parametrization
@pytest.mark.integration
@pytest.mark.snowflake
@pytest.mark.parametrize(
    "use_physical_tables",
    [False],  # Physical tables disabled for Snowflake due to environment limitations
    ids=["cte_mode"],
)
class TestSnowflakePrimitiveTypesIntegration:
    """Integration tests for primitive types with Snowflake adapter (CTE mode only)."""

    @pytest.fixture(autouse=True)
    def setup_snowflake_data(self):
        """Set up Snowflake-specific test data."""
        self.test_data = [
            PrimitiveTypes(
                int_col=9223372036854775807,  # Max number
                float_col=1.7976931348623157e308,
                decimal_col=Decimal("999999999999999999999999999999999999.999999999"),
                string_col="Hello Snowflake",
                varchar_col="Snowflake varchar test",
                boolean_col=True,
                date_col=date(9999, 12, 31),
                timestamp_col=datetime(9999, 12, 31, 23, 59, 59),
                optional_string="Snowflake optional",
                optional_int=111222333,
                optional_decimal=Decimal("13579.24680"),
                optional_bool=True,
                optional_date=date(2023, 10, 5),
                optional_timestamp=datetime(2023, 10, 5, 20, 15, 30),
            ),
            PrimitiveTypes(
                int_col=-9223372036854775808,  # Min number
                float_col=-1.7976931348623157e308,
                decimal_col=Decimal("-999999999999999999999999999999999999.999999999"),
                string_col="Snowflake with 'quotes' and escapes\\backslash",
                varchar_col="Path: C:\\Users\\test\\file.txt",
                boolean_col=False,
                date_col=date(1, 1, 1),
                timestamp_col=datetime(1, 1, 1, 0, 0, 0),
                optional_string=None,
                optional_int=None,
                optional_decimal=None,
                optional_bool=None,
                optional_date=None,
                optional_timestamp=None,
            ),
        ]
        self.database_name = os.getenv("SNOWFLAKE_DATABASE", "test_db")

    def test_snowflake_primitive_types(self, use_physical_tables):
        """Test all primitive types with Snowflake adapter."""

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[PrimitiveTypesMockTable(self.test_data, self.database_name)],
            result_class=PrimitiveTypesResult,
        )
        def query_snowflake_primitive_types():
            return TestCase(
                query="""
                    SELECT
                        int_col,
                        float_col,
                        decimal_col,
                        string_col,
                        varchar_col,
                        boolean_col,
                        date_col,
                        timestamp_col,
                        optional_string,
                        optional_int,
                        optional_decimal,
                        optional_bool,
                        optional_date,
                        optional_timestamp
                    FROM primitive_types
                    ORDER BY int_col DESC
                """,
                execution_database=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_snowflake_primitive_types()

        assert len(results) == 2

        # Verify max values row
        max_row = results[0]
        assert max_row.int_col == 9223372036854775807
        assert max_row.float_col == 1.7976931348623157e308
        assert max_row.decimal_col == Decimal("1E+36")  # Snowflake returns scientific notation
        assert max_row.string_col == "Hello Snowflake"
        assert max_row.varchar_col == "Snowflake varchar test"
        assert max_row.boolean_col is True
        assert max_row.date_col == date(9999, 12, 31)
        assert max_row.timestamp_col == datetime(9999, 12, 31, 23, 59, 59)
        assert max_row.optional_string == "Snowflake optional"
        assert max_row.optional_int == 111222333
        assert max_row.optional_decimal == Decimal(
            "13579.2468"
        )  # Snowflake truncates trailing zeros
        assert max_row.optional_bool is True
        assert max_row.optional_date == date(2023, 10, 5)
        assert max_row.optional_timestamp == datetime(2023, 10, 5, 20, 15, 30)

        # Verify min values row with nulls
        min_row = results[1]
        assert min_row.int_col == -9223372036854775808
        assert min_row.float_col == -1.7976931348623157e308
        assert min_row.decimal_col == Decimal("-1E+36")  # Snowflake returns scientific notation
        assert min_row.string_col == "Snowflake with 'quotes' and escapes\\backslash"
        assert min_row.varchar_col == "Path: C:\\Users\\test\\file.txt"
        assert min_row.boolean_col is False
        assert min_row.date_col == date(1, 1, 1)
        assert min_row.timestamp_col == datetime(1, 1, 1, 0, 0, 0)
        assert min_row.optional_string is None
        assert min_row.optional_int is None
        assert min_row.optional_decimal is None
        assert min_row.optional_bool is None
        assert min_row.optional_date is None
        assert min_row.optional_timestamp is None

    def test_snowflake_all_optional_fields_none(self, use_physical_tables):
        """Test Snowflake model with all optional fields set to None values."""

        @dataclass
        class AllOptionalModel:
            """Model with all fields as optional - used for both input data and results."""

            optional_int: Optional[int] = None
            optional_float: Optional[float] = None
            optional_string: Optional[str] = None
            optional_bool: Optional[bool] = None
            optional_date: Optional[date] = None
            optional_timestamp: Optional[datetime] = None
            optional_decimal: Optional[Decimal] = None

        class AllOptionalMockTable(BaseMockTable):
            """Mock table for all optional fields test."""

            def __init__(self, data, database_name: str):
                super().__init__(data)
                self._database_name = database_name

            def get_database_name(self) -> str:
                return self._database_name

            def get_table_name(self) -> str:
                return "all_optional_table"

        # Create test data with all None values
        test_data = [
            AllOptionalModel(),  # All fields default to None
            AllOptionalModel(),  # Second row, also all None
        ]

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[AllOptionalMockTable(test_data, self.database_name)],
            result_class=AllOptionalModel,  # Reuse the same model for results
        )
        def query_snowflake_all_optional_none():
            return TestCase(
                query="""
                    SELECT
                        optional_int,
                        optional_float,
                        optional_string,
                        optional_bool,
                        optional_date,
                        optional_timestamp,
                        optional_decimal
                    FROM all_optional_table
                    ORDER BY optional_int
                """,
                execution_database=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_snowflake_all_optional_none()

        # Should return 2 rows
        assert len(results) == 2

        # Verify both rows have all None values
        for row in results:
            assert row.optional_int is None
            assert row.optional_float is None
            assert row.optional_string is None
            assert row.optional_bool is None
            assert row.optional_date is None
            assert row.optional_timestamp is None
            assert row.optional_decimal is None
