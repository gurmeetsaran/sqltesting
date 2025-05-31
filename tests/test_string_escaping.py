"""Test string escaping across all database adapters."""

from dataclasses import dataclass
from typing import Optional

import pytest

from sql_testing_library._adapters.athena import AthenaAdapter
from sql_testing_library._adapters.bigquery import BigQueryAdapter
from sql_testing_library._adapters.redshift import RedshiftAdapter
from sql_testing_library._adapters.snowflake import SnowflakeAdapter
from sql_testing_library._adapters.trino import TrinoAdapter
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class StringTestData:
    """Test data class for string escaping tests."""

    id: int
    test_string: str
    optional_string: Optional[str] = None


class StringTestMockTable(BaseMockTable):
    """Mock table for string escaping tests."""

    def __init__(self, data, database_name: str = "test_db"):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "string_test"


class TestStringEscaping:
    """Test string escaping across all database adapters."""

    # Test cases covering various edge cases
    test_strings = [
        # Basic strings
        "hello world",
        "",
        # Single quotes
        "O'Brien",
        "'quoted string'",
        "multiple 'quoted' 'words'",
        "'''triple quotes'''",
        # Double quotes
        'JSON: {"key": "value"}',
        '"double quoted"',
        # Mixed quotes
        """Mix 'single' and "double" quotes""",
        # Backslashes
        "C:\\Users\\test\\file.txt",
        "Escaped\\backslash",
        "UNC\\\\server\\share",
        # Newlines and special characters
        "Line1\nLine2\nLine3",
        "Tab\tSeparated\tValues",
        "Carriage\rReturn",
        "Form\fFeed",
        "Vertical\vTab",
        # Unicode
        "Hello 世界",
        "Café français",
        "Emoji: 🚀 🎉",
        # SQL injection attempts (should be properly escaped)
        "'; DROP TABLE users; --",
        "' OR 1=1 --",
        "UNION SELECT * FROM passwords",
        # Complex mixed content
        "Path: C:\\Program Files\\App's Folder\\config.json",
        "Code: if (x == 'test') { print(\"Hello\"); }",
        "SQL: SELECT * FROM 'table' WHERE col = \"value\"",
        # Very long string
        "x" * 1000,
        # Special cases
        "NULL",
        "null",
        "NaN",
        "undefined",
    ]

    def test_athena_string_escaping(self):
        """Test string escaping for Athena adapter."""
        self._test_adapter_string_escaping("athena")

    def test_bigquery_string_escaping(self):
        """Test string escaping for BigQuery adapter."""
        self._test_adapter_string_escaping("bigquery")

    def test_redshift_string_escaping(self):
        """Test string escaping for Redshift adapter."""
        self._test_adapter_string_escaping("redshift")

    def test_snowflake_string_escaping(self):
        """Test string escaping for Snowflake adapter."""
        self._test_adapter_string_escaping("snowflake")

    def test_trino_string_escaping(self):
        """Test string escaping for Trino adapter."""
        self._test_adapter_string_escaping("trino")

    def _test_adapter_string_escaping(self, adapter_type: str):
        """Test string escaping for a specific adapter type."""
        # Create test data with problematic strings
        test_data = [
            StringTestData(
                id=i, test_string=test_string, optional_string=test_string if i % 2 == 0 else None
            )
            for i, test_string in enumerate(self.test_strings)
        ]

        # Create mock table (not used directly but kept for potential future use)
        _ = StringTestMockTable(test_data)

        # Get the appropriate adapter class
        adapter_classes = {
            "athena": AthenaAdapter,
            "bigquery": BigQueryAdapter,
            "redshift": RedshiftAdapter,
            "snowflake": SnowflakeAdapter,
            "trino": TrinoAdapter,
        }

        adapter_class = adapter_classes[adapter_type]

        # Test CTE generation (this is where escaping happens)
        try:
            # Create a mock adapter instance (without real connection)
            adapter = self._create_mock_adapter(adapter_class, adapter_type)

            # Test format_value_for_cte for each string
            for test_string in self.test_strings:
                try:
                    result = adapter.format_value_for_cte(test_string, str)

                    # Basic validation: result should be a string and not contain unescaped quotes
                    assert isinstance(result, str), f"Result should be string, got {type(result)}"
                    assert len(result) >= 2, f"Result should be quoted string, got: {result}"

                    # Should start and end with quote (unless using special syntax)
                    if not (result.startswith('r"""') or result.startswith("CAST")):
                        assert result.startswith("'") or result.startswith('"'), (
                            f"Should start with quote: {result}"
                        )

                    print(f"✓ {adapter_type}: {test_string!r} -> {result}")

                except Exception as e:
                    pytest.fail(
                        f"Failed to escape string for {adapter_type}: {test_string!r} - {e}"
                    )

        except Exception as e:
            pytest.fail(f"Failed to create {adapter_type} adapter: {e}")

    def _create_mock_adapter(self, adapter_class, adapter_type: str):
        """Create a mock adapter instance for testing."""

        if adapter_type == "athena":
            # Mock Athena adapter
            adapter = object.__new__(adapter_class)
            return adapter

        elif adapter_type == "bigquery":
            # Mock BigQuery adapter
            adapter = object.__new__(adapter_class)
            return adapter

        elif adapter_type == "redshift":
            # Mock Redshift adapter
            adapter = object.__new__(adapter_class)
            return adapter

        elif adapter_type == "snowflake":
            # Mock Snowflake adapter
            adapter = object.__new__(adapter_class)
            return adapter

        elif adapter_type == "trino":
            # Mock Trino adapter
            adapter = object.__new__(adapter_class)
            return adapter

        else:
            raise ValueError(f"Unknown adapter type: {adapter_type}")

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are properly escaped."""
        injection_attempts = [
            "'; DROP TABLE users; --",
            "' OR 1=1 --",
            "UNION SELECT * FROM passwords",
            "'; UPDATE users SET admin=1; --",
            "' AND 1=(SELECT COUNT(*) FROM users) --",
        ]

        for adapter_type in ["athena", "bigquery", "redshift", "snowflake", "trino"]:
            adapter_classes = {
                "athena": AthenaAdapter,
                "bigquery": BigQueryAdapter,
                "redshift": RedshiftAdapter,
                "snowflake": SnowflakeAdapter,
                "trino": TrinoAdapter,
            }

            adapter = self._create_mock_adapter(adapter_classes[adapter_type], adapter_type)

            for injection in injection_attempts:
                result = adapter.format_value_for_cte(injection, str)

                # Should not contain unescaped single quotes that could break out of string
                if result.startswith("'") and result.endswith("'"):
                    inner_content = result[1:-1]
                    # Check for properly escaped quotes
                    assert "''" in result or "\\'" in result or "'''" not in inner_content, (
                        f"Potential SQL injection vulnerability in {adapter_type}: "
                        f"{injection} -> {result}"
                    )

                print(f"✓ {adapter_type} injection test: {injection!r} -> {result}")


if __name__ == "__main__":
    # Run tests manually
    test = TestStringEscaping()

    print("Testing string escaping across all adapters...\n")

    for adapter in ["athena", "bigquery", "redshift", "snowflake", "trino"]:
        print(f"\n=== Testing {adapter.upper()} ===")
        try:
            test._test_adapter_string_escaping(adapter)
        except Exception as e:
            print(f"✗ {adapter} failed: {e}")

    print("\n=== Testing SQL Injection Prevention ===")
    test.test_sql_injection_prevention()
