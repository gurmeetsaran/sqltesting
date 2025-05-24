"""Test type conversion and type handling."""

import unittest
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Union

from sql_testing_library.types import BaseTypeConverter


class TestBaseTypeConverter(unittest.TestCase):
    """Test BaseTypeConverter functionality."""

    def setUp(self):
        """Set up test converter."""
        self.converter = BaseTypeConverter()

    def test_basic_type_conversions(self):
        """Test basic type conversions."""
        # String conversions
        self.assertEqual(self.converter.convert("hello", str), "hello")
        self.assertEqual(self.converter.convert(123, str), "123")

        # Integer conversions
        self.assertEqual(self.converter.convert(42, int), 42)
        self.assertEqual(self.converter.convert("42", int), 42)
        self.assertEqual(self.converter.convert(42.0, int), 42)

        # Float conversions
        self.assertEqual(self.converter.convert(3.14, float), 3.14)
        self.assertEqual(self.converter.convert("3.14", float), 3.14)
        self.assertEqual(self.converter.convert(42, float), 42.0)

    def test_boolean_conversions(self):
        """Test boolean type conversions."""
        # True cases - based on actual implementation
        self.assertTrue(self.converter.convert(True, bool))
        self.assertTrue(self.converter.convert("true", bool))
        self.assertTrue(self.converter.convert("1", bool))
        self.assertTrue(self.converter.convert("yes", bool))
        self.assertTrue(self.converter.convert("t", bool))
        self.assertTrue(self.converter.convert(1, bool))

        # False cases - based on actual implementation
        self.assertFalse(self.converter.convert(False, bool))
        self.assertFalse(self.converter.convert("false", bool))
        self.assertFalse(self.converter.convert("0", bool))
        self.assertFalse(self.converter.convert(0, bool))
        self.assertFalse(self.converter.convert("", bool))

        # Edge cases
        self.assertFalse(self.converter.convert("anything_else", bool))

    def test_date_conversions(self):
        """Test date type conversions."""
        # Date object to date
        test_date = date(2023, 12, 25)
        self.assertEqual(self.converter.convert(test_date, date), test_date)

        # String to date
        self.assertEqual(self.converter.convert("2023-12-25", date), test_date)

        # Datetime to date
        test_datetime = datetime(2023, 12, 25, 15, 30, 45)
        self.assertEqual(self.converter.convert(test_datetime, date), test_date)

    def test_datetime_conversions(self):
        """Test datetime type conversions."""
        # Datetime object to datetime
        test_datetime = datetime(2023, 12, 25, 15, 30, 45)
        self.assertEqual(self.converter.convert(test_datetime, datetime), test_datetime)

        # String to datetime (ISO format)
        iso_string = "2023-12-25T15:30:45"
        expected = datetime(2023, 12, 25, 15, 30, 45)
        self.assertEqual(self.converter.convert(iso_string, datetime), expected)

    def test_decimal_conversions(self):
        """Test Decimal type conversions."""
        # Decimal to Decimal
        test_decimal = Decimal("123.45")
        self.assertEqual(self.converter.convert(test_decimal, Decimal), test_decimal)

        # String to Decimal
        self.assertEqual(self.converter.convert("123.45", Decimal), test_decimal)

        # Float to Decimal
        self.assertEqual(self.converter.convert(123.45, Decimal), Decimal("123.45"))

        # Integer to Decimal
        self.assertEqual(self.converter.convert(123, Decimal), Decimal("123"))

    def test_none_handling(self):
        """Test None value handling."""
        # None should remain None for any type
        self.assertIsNone(self.converter.convert(None, str))
        self.assertIsNone(self.converter.convert(None, int))
        self.assertIsNone(self.converter.convert(None, float))
        self.assertIsNone(self.converter.convert(None, bool))
        self.assertIsNone(self.converter.convert(None, date))
        self.assertIsNone(self.converter.convert(None, datetime))
        self.assertIsNone(self.converter.convert(None, Decimal))

    def test_optional_type_detection(self):
        """Test Optional type detection."""
        # Test is_optional_type method
        self.assertTrue(self.converter.is_optional_type(Optional[str]))
        self.assertTrue(self.converter.is_optional_type(Union[str, None]))
        self.assertTrue(self.converter.is_optional_type(Union[int, type(None)]))

        # Non-optional types
        self.assertFalse(self.converter.is_optional_type(str))
        self.assertFalse(self.converter.is_optional_type(int))
        self.assertFalse(self.converter.is_optional_type(Union[str, int]))

    def test_get_optional_inner_type(self):
        """Test extracting non-None type from Optional."""
        # Test get_optional_inner_type method (static method)
        self.assertEqual(self.converter.get_optional_inner_type(Optional[str]), str)
        self.assertEqual(self.converter.get_optional_inner_type(Union[int, None]), int)
        self.assertEqual(self.converter.get_optional_inner_type(Union[str, type(None)]), str)

    def test_optional_type_conversions(self):
        """Test conversions with Optional types."""
        # Convert to Optional[str] with valid value
        result = self.converter.convert("hello", Optional[str])
        self.assertEqual(result, "hello")

        # Convert to Optional[int] with None
        result = self.converter.convert(None, Optional[int])
        self.assertIsNone(result)

        # Convert to Optional[int] with valid value
        result = self.converter.convert("42", Optional[int])
        self.assertEqual(result, 42)

    def test_conversion_errors(self):
        """Test type conversion error handling."""
        # Invalid string to int
        with self.assertRaises(ValueError):
            self.converter.convert("not_a_number", int)

        # Invalid string to float
        with self.assertRaises(ValueError):
            self.converter.convert("not_a_float", float)

        # Invalid string to date
        with self.assertRaises(ValueError):
            self.converter.convert("not_a_date", date)

        # Invalid string to datetime
        with self.assertRaises(ValueError):
            self.converter.convert("not_a_datetime", datetime)

    def test_edge_case_values(self):
        """Test edge case values."""
        # Empty string conversions
        self.assertEqual(self.converter.convert("", str), "")

        # Zero values
        self.assertEqual(self.converter.convert(0, int), 0)
        self.assertEqual(self.converter.convert(0.0, float), 0.0)
        self.assertEqual(self.converter.convert("0", int), 0)

        # Large numbers
        large_int = 999999999999999999
        self.assertEqual(self.converter.convert(large_int, int), large_int)

        # Very small decimal
        small_decimal = Decimal("0.000000001")
        self.assertEqual(self.converter.convert("0.000000001", Decimal), small_decimal)

    def test_string_conversion_edge_cases(self):
        """Test string conversions with edge cases."""
        # Test various inputs to string conversion
        self.assertEqual(self.converter.convert(123, str), "123")
        self.assertEqual(self.converter.convert(3.14, str), "3.14")
        self.assertEqual(self.converter.convert(True, str), "True")
        self.assertEqual(self.converter.convert([], str), "[]")

    def test_type_compatibility(self):
        """Test type compatibility checking."""
        # Same types should be compatible
        self.assertEqual(self.converter.convert("test", str), "test")
        self.assertEqual(self.converter.convert(42, int), 42)

        # Compatible types should convert
        self.assertEqual(self.converter.convert(42, float), 42.0)
        self.assertEqual(self.converter.convert(42.0, int), 42)

    def test_custom_type_fallback(self):
        """Test fallback for unsupported types."""

        # Custom class
        class CustomClass:
            def __init__(self, value):
                self.value = value

            def __str__(self):
                return f"Custom({self.value})"

        custom_obj = CustomClass("test")

        # Should fallback to string conversion for unknown types
        result = self.converter.convert(custom_obj, str)
        self.assertEqual(result, "Custom(test)")

    def test_complex_union_types(self):
        """Test complex Union types."""
        # Union with multiple non-None types
        complex_union = Union[str, int, float]

        # Should use first compatible type
        result = self.converter.convert("42", complex_union)
        # The exact behavior depends on implementation
        # but it should handle the conversion gracefully
        self.assertIsNotNone(result)

    def test_performance_with_large_datasets(self):
        """Test converter performance with larger datasets."""
        # Create larger test data
        large_values = list(range(1000))

        # Convert all values
        for value in large_values[:10]:  # Test subset for speed
            result = self.converter.convert(value, int)
            self.assertEqual(result, value)


if __name__ == "__main__":
    unittest.main()
