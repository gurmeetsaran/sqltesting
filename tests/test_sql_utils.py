"""Test SQL utilities for string escaping."""

import pytest

from sql_testing_library._sql_utils import escape_sql_string, format_sql_value


class TestSQLStringEscaping:
    """Test SQL string escaping utility functions."""

    def test_basic_string_escaping(self):
        """Test basic string escaping functionality."""
        test_cases = [
            ("hello", "'hello'"),
            ("", "''"),
            ("O'Brien", "'O''Brien'"),
            ("'quoted'", "'''quoted'''"),
            ("multiple 'quotes' here", "'multiple ''quotes'' here'"),
        ]

        for input_str, expected in test_cases:
            result = escape_sql_string(input_str)
            assert result == expected, (
                f"Failed for {input_str!r}: got {result}, expected {expected}"
            )

    def test_backslash_escaping(self):
        """Test backslash escaping."""
        test_cases = [
            ("C:\\Users\\test", "'C:\\\\Users\\\\test'"),
            ("path\\to\\file", "'path\\\\to\\\\file'"),
            ("single\\backslash", "'single\\\\backslash'"),
        ]

        for input_str, expected in test_cases:
            result = escape_sql_string(input_str)
            assert result == expected, (
                f"Failed for {input_str!r}: got {result}, expected {expected}"
            )

    def test_special_characters(self):
        """Test handling of special characters."""
        test_cases = [
            ("Line1\nLine2", "'Line1\\nLine2'"),  # Newlines escaped
            ("Tab\tSeparated", "'Tab\\tSeparated'"),  # Tabs escaped
            ("Carriage\rReturn", "'Carriage\\rReturn'"),  # Carriage returns escaped
            ("Unicode: 你好", "'Unicode: 你好'"),  # Unicode preserved
        ]

        for input_str, expected in test_cases:
            result = escape_sql_string(input_str)
            assert result == expected, (
                f"Failed for {input_str!r}: got {result}, expected {expected}"
            )

    def test_null_byte_removal(self):
        """Test that null bytes are removed."""
        test_cases = [
            ("test\x00null", "'testnull'"),
            ("\x00start", "'start'"),
            ("end\x00", "'end'"),
            ("mid\x00dle", "'middle'"),
        ]

        for input_str, expected in test_cases:
            result = escape_sql_string(input_str)
            assert result == expected, (
                f"Failed for {input_str!r}: got {result}, expected {expected}"
            )

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are properly escaped."""
        injection_attempts = [
            "'; DROP TABLE users; --",
            "' OR 1=1 --",
            "UNION SELECT * FROM passwords",
            "'; UPDATE users SET admin=1; --",
        ]

        for injection in injection_attempts:
            result = escape_sql_string(injection)
            # Should be properly quoted and escaped
            assert result.startswith("'") and result.endswith("'")
            # Should contain escaped quotes if original had single quotes
            if "'" in injection:
                assert "''" in result


class TestFormatSQLValue:
    """Test format_sql_value function with different types and dialects."""

    def test_none_values(self):
        """Test NULL value handling."""
        assert format_sql_value(None, str) == "NULL"
        assert format_sql_value(None, int) == "NULL"
        assert format_sql_value(None, bool) == "NULL"

    def test_redshift_null_casting(self):
        """Test Redshift-specific NULL type casting."""
        from datetime import date, datetime
        from decimal import Decimal

        # Redshift requires explicit type casting for NULL values
        assert format_sql_value(None, str, "redshift") == "NULL::VARCHAR"
        assert format_sql_value(None, int, "redshift") == "NULL::BIGINT"
        assert format_sql_value(None, float, "redshift") == "NULL::DOUBLE PRECISION"
        assert format_sql_value(None, bool, "redshift") == "NULL::BOOLEAN"
        assert format_sql_value(None, date, "redshift") == "NULL::DATE"
        assert format_sql_value(None, datetime, "redshift") == "NULL::TIMESTAMP"
        assert format_sql_value(None, Decimal, "redshift") == "NULL::DECIMAL(38,9)"

    def test_string_values(self):
        """Test string value formatting."""
        assert format_sql_value("hello", str) == "'hello'"
        assert format_sql_value("O'Brien", str) == "'O''Brien'"
        assert format_sql_value("", str) == "''"

    def test_numeric_values(self):
        """Test numeric value formatting."""
        assert format_sql_value(42, int) == "42"
        assert format_sql_value(3.14, float) == "3.14"
        assert format_sql_value(-123, int) == "-123"

    def test_boolean_values(self):
        """Test boolean value formatting."""
        assert format_sql_value(True, bool) == "TRUE"
        assert format_sql_value(False, bool) == "FALSE"

    def test_date_values(self):
        """Test date value formatting with different dialects."""
        from datetime import date

        test_date = date(2023, 12, 25)

        # BigQuery uses DATE() function
        assert format_sql_value(test_date, date, "bigquery") == "DATE('2023-12-25')"

        # Standard SQL uses DATE literal
        assert format_sql_value(test_date, date, "standard") == "DATE '2023-12-25'"
        assert format_sql_value(test_date, date, "athena") == "DATE '2023-12-25'"

    def test_datetime_values(self):
        """Test datetime value formatting with different dialects."""
        from datetime import datetime

        test_datetime = datetime(2023, 12, 25, 15, 30, 45)

        # BigQuery uses DATETIME() function
        result_bq = format_sql_value(test_datetime, datetime, "bigquery")
        assert result_bq == "DATETIME('2023-12-25T15:30:45')"

        # Athena and Trino use space separator (don't like 'T')
        result_athena = format_sql_value(test_datetime, datetime, "athena")
        assert result_athena == "TIMESTAMP '2023-12-25 15:30:45.000'"

        result_trino = format_sql_value(test_datetime, datetime, "trino")
        assert result_trino == "TIMESTAMP '2023-12-25 15:30:45.000'"

        # Standard uses ISO format
        result_std = format_sql_value(test_datetime, datetime, "standard")
        assert result_std == "TIMESTAMP '2023-12-25T15:30:45'"

    def test_decimal_values(self):
        """Test decimal value formatting."""
        from decimal import Decimal

        assert format_sql_value(Decimal("123.45"), Decimal) == "123.45"
        assert format_sql_value(Decimal("-99.99"), Decimal) == "-99.99"

    def test_unknown_types(self):
        """Test handling of unknown types (should convert to string)."""

        class CustomObject:
            def __str__(self):
                return "custom_value"

        obj = CustomObject()
        result = format_sql_value(obj, type(obj))
        assert result == "'custom_value'"

    def test_null_array_handling(self):
        """Test NULL array handling for different dialects."""
        from datetime import date, datetime
        from decimal import Decimal
        from typing import List

        # Athena NULL arrays
        assert format_sql_value(None, List[str], "athena") == "CAST(NULL AS ARRAY(VARCHAR))"
        assert format_sql_value(None, List[int], "athena") == "CAST(NULL AS ARRAY(INTEGER))"
        assert format_sql_value(None, List[float], "athena") == "CAST(NULL AS ARRAY(DOUBLE))"
        assert format_sql_value(None, List[bool], "athena") == "CAST(NULL AS ARRAY(BOOLEAN))"
        assert (
            format_sql_value(None, List[Decimal], "athena") == "CAST(NULL AS ARRAY(DECIMAL(38,9)))"
        )
        assert format_sql_value(None, List[date], "athena") == "CAST(NULL AS ARRAY(DATE))"
        assert format_sql_value(None, List[datetime], "athena") == "CAST(NULL AS ARRAY(TIMESTAMP))"

        # Trino NULL arrays (uses BIGINT instead of INTEGER)
        assert format_sql_value(None, List[str], "trino") == "CAST(NULL AS ARRAY(VARCHAR))"
        assert format_sql_value(None, List[int], "trino") == "CAST(NULL AS ARRAY(BIGINT))"
        assert format_sql_value(None, List[float], "trino") == "CAST(NULL AS ARRAY(DOUBLE))"
        assert format_sql_value(None, List[bool], "trino") == "CAST(NULL AS ARRAY(BOOLEAN))"
        assert (
            format_sql_value(None, List[Decimal], "trino") == "CAST(NULL AS ARRAY(DECIMAL(38,9)))"
        )

        # Other dialects
        assert format_sql_value(None, List[str], "bigquery") == "NULL"
        assert format_sql_value(None, List[str], "redshift") == "NULL::SUPER"
        assert format_sql_value(None, List[str], "snowflake") == "NULL::VARIANT"
        assert format_sql_value(None, List[str], "standard") == "NULL"

    def test_array_value_formatting(self):
        """Test array value formatting for different dialects."""
        from typing import List

        # Test non-NULL arrays
        assert format_sql_value(["a", "b"], List[str], "athena") == "ARRAY['a', 'b']"
        assert format_sql_value([1, 2, 3], List[int], "athena") == "ARRAY[1, 2, 3]"
        assert format_sql_value([], List[str], "athena") == "ARRAY[]"

        assert format_sql_value(["a", "b"], List[str], "bigquery") == "['a', 'b']"
        assert format_sql_value([1, 2, 3], List[int], "bigquery") == "[1, 2, 3]"
        assert format_sql_value([], List[str], "bigquery") == "[]"

        assert format_sql_value(["a", "b"], List[str], "snowflake") == "ARRAY_CONSTRUCT('a', 'b')"
        assert format_sql_value([1, 2, 3], List[int], "snowflake") == "ARRAY_CONSTRUCT(1, 2, 3)"
        assert format_sql_value([], List[str], "snowflake") == "ARRAY_CONSTRUCT()"


if __name__ == "__main__":
    pytest.main([__file__])
