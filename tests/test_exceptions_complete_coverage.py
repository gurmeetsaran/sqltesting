"""Complete coverage tests for exceptions module."""

from typing import Optional, Union

import pytest

# Import all exceptions to ensure class definitions are covered
from sql_testing_library._exceptions import (
    MockTableNotFoundError,
    QuerySizeLimitExceeded,
    SQLParseError,
    SQLTestingError,
    TypeConversionError,
)


def test_all_exception_classes_exist():
    """Test that all exception classes can be imported and instantiated."""
    # This ensures the class definitions (lines 6-9, 12-21, 24-30, 33-43, 46-66) are covered
    assert SQLTestingError is not None
    assert MockTableNotFoundError is not None
    assert SQLParseError is not None
    assert QuerySizeLimitExceeded is not None
    assert TypeConversionError is not None


def test_sqltest_error_is_exception():
    """Test SQLTestingError base class."""
    exc = SQLTestingError("test message")
    assert isinstance(exc, Exception)
    assert str(exc) == "test message"


def test_mock_table_not_found_with_available():
    """Test MockTableNotFoundError with available tables."""
    exc = MockTableNotFoundError("schema.table", ["mock1", "mock2"])
    assert exc.qualified_table_name == "schema.table"
    assert "mock1" in str(exc)


def test_mock_table_not_found_empty():
    """Test MockTableNotFoundError with no available tables."""
    exc = MockTableNotFoundError("table", [])
    assert "None" in str(exc)


def test_sql_parse_error():
    """Test SQLParseError."""
    exc = SQLParseError("SELECT invalid", "syntax error")
    assert exc.query == "SELECT invalid"
    assert exc.parse_error == "syntax error"
    assert "Failed to parse SQL" in str(exc)


def test_query_size_limit_exceeded():
    """Test QuerySizeLimitExceeded."""
    exc = QuerySizeLimitExceeded(2000, 1000, "bigquery")
    assert exc.actual_size == 2000
    assert exc.limit == 1000
    assert exc.adapter_name == "bigquery"
    assert "2000" in str(exc)
    assert "1000" in str(exc)
    assert "use_physical_tables=True" in str(exc)


def test_type_conversion_error_with_column():
    """Test TypeConversionError with column name."""
    exc = TypeConversionError("bad", int, "my_column")
    assert exc.value == "bad"
    assert exc.target_type is int
    assert exc.column_name == "my_column"
    assert "my_column" in str(exc)
    assert "int" in str(exc)


def test_type_conversion_error_without_column():
    """Test TypeConversionError without column name."""
    exc = TypeConversionError("bad", str, None)
    assert exc.column_name is None
    error_msg = str(exc)
    assert "Cannot convert" in error_msg
    assert "'bad'" in error_msg


def test_type_conversion_error_empty_column():
    """Test TypeConversionError with empty column name."""
    exc = TypeConversionError("bad", float, "")
    error_msg = str(exc)
    # Empty string is falsy, so should not include "for column"
    assert "Cannot convert" in error_msg
    # The exact format depends on the if column_name check


def test_type_conversion_error_type_without_name():
    """Test TypeConversionError with type that has no __name__ attribute."""

    # Create a custom type-like object without __name__
    class FakeType:
        def __repr__(self):
            return "FakeType"

    fake_type = FakeType()
    exc = TypeConversionError("value", fake_type, "col")

    # This should trigger the except AttributeError block (line 57-59)
    error_msg = str(exc)
    assert "Cannot convert" in error_msg


def test_type_conversion_optional_type():
    """Test TypeConversionError with Optional type (which doesn't have __name__)."""
    # Optional[int] doesn't have __name__, so will use str(target_type)
    optional_int = Optional[int]
    exc = TypeConversionError("bad", optional_int, "field")

    error_msg = str(exc)
    assert "Cannot convert" in error_msg
    assert "field" in error_msg
    # The string representation will be used since Optional doesn't have __name__


def test_type_conversion_union_type():
    """Test TypeConversionError with Union type."""
    union_type = Union[int, str]
    exc = TypeConversionError("bad", union_type, "data")

    error_msg = str(exc)
    assert "Cannot convert" in error_msg
    assert "data" in error_msg


def test_all_exceptions_inherit_from_base():
    """Test that all custom exceptions inherit from SQLTestingError."""
    assert issubclass(MockTableNotFoundError, SQLTestingError)
    assert issubclass(SQLParseError, SQLTestingError)
    assert issubclass(QuerySizeLimitExceeded, SQLTestingError)
    assert issubclass(TypeConversionError, SQLTestingError)


def test_all_exceptions_inherit_from_exception():
    """Test that all exceptions ultimately inherit from Exception."""
    for exc_class in [
        SQLTestingError,
        MockTableNotFoundError,
        SQLParseError,
        QuerySizeLimitExceeded,
        TypeConversionError,
    ]:
        assert issubclass(exc_class, Exception)


def test_exception_can_be_raised_and_caught():
    """Test that exceptions can be raised and caught."""
    with pytest.raises(SQLTestingError):
        raise SQLTestingError("test")

    with pytest.raises(MockTableNotFoundError):
        raise MockTableNotFoundError("table", [])

    with pytest.raises(SQLParseError):
        raise SQLParseError("query", "error")

    with pytest.raises(QuerySizeLimitExceeded):
        raise QuerySizeLimitExceeded(100, 50, "adapter")

    with pytest.raises(TypeConversionError):
        raise TypeConversionError("val", int, "col")


def test_exception_attributes_accessible():
    """Test that exception attributes are accessible after raising."""
    try:
        raise MockTableNotFoundError("db.table", ["mock1", "mock2"])
    except MockTableNotFoundError as e:
        assert e.qualified_table_name == "db.table"
        assert e.available_mocks == ["mock1", "mock2"]

    try:
        raise SQLParseError("SELECT bad", "syntax")
    except SQLParseError as e:
        assert e.query == "SELECT bad"
        assert e.parse_error == "syntax"

    try:
        raise QuerySizeLimitExceeded(1000, 500, "redshift")
    except QuerySizeLimitExceeded as e:
        assert e.actual_size == 1000
        assert e.limit == 500
        assert e.adapter_name == "redshift"

    try:
        raise TypeConversionError("x", int, "col")
    except TypeConversionError as e:
        assert e.value == "x"
        assert e.target_type is int
        assert e.column_name == "col"
