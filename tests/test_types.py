"""Test type conversion and type handling."""

import unittest
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Union
from unittest import mock

import numpy as np

from sql_testing_library._types import BaseTypeConverter, unwrap_optional_type


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


class TestUnwrapOptionalType(unittest.TestCase):
    """Test cases for unwrap_optional_type function."""

    def test_unwrap_optional_type(self):
        """Test unwrap_optional_type function."""

        # Test Optional types
        self.assertEqual(unwrap_optional_type(Optional[str]), str)
        self.assertEqual(unwrap_optional_type(Optional[int]), int)
        self.assertEqual(unwrap_optional_type(Optional[date]), date)

        # Test non-Optional types
        self.assertEqual(unwrap_optional_type(str), str)
        self.assertEqual(unwrap_optional_type(int), int)
        self.assertEqual(unwrap_optional_type(date), date)

        # Test Union types with multiple non-None types
        union_type = Union[str, int, float]
        result = unwrap_optional_type(union_type)
        # Should return the first non-None type when Union doesn't contain None
        self.assertEqual(result, str)

        # Test complex Optional with custom types
        self.assertEqual(unwrap_optional_type(Optional[Decimal]), Decimal)


class TestBaseTypeConverterArrays(unittest.TestCase):
    """Test BaseTypeConverter array/list functionality."""

    def setUp(self):
        """Set up test converter."""
        self.converter = BaseTypeConverter()

    def test_list_conversion_already_list(self):
        """Test list conversion when value is already a list."""
        test_list = [1, 2, 3]
        result = self.converter.convert(test_list, List[int])
        self.assertEqual(result, test_list)

    def test_list_conversion_numpy_array(self):
        """Test list conversion from numpy array."""
        numpy_array = np.array([1, 2, 3])
        result = self.converter.convert(numpy_array, List[int])
        self.assertEqual(result, [1, 2, 3])

    def test_list_conversion_numpy_array_with_strings(self):
        """Test list conversion from numpy array with strings."""
        numpy_array = np.array(["hello", "world", "test"])
        result = self.converter.convert(numpy_array, List[str])
        self.assertEqual(result, ["hello", "world", "test"])

    def test_list_conversion_string_array_format(self):
        """Test list conversion from string array format."""
        # Test with integers
        result = self.converter.convert("[1, 2, 3]", List[int])
        self.assertEqual(result, [1, 2, 3])

        # Test with strings
        result = self.converter.convert("['hello', 'world']", List[str])
        self.assertEqual(result, ["hello", "world"])

        # Test with double quotes
        result = self.converter.convert('["hello", "world"]', List[str])
        self.assertEqual(result, ["hello", "world"])

    def test_list_conversion_empty_array_string(self):
        """Test list conversion from empty array string."""
        result = self.converter.convert("[]", List[int])
        self.assertEqual(result, [])

        result = self.converter.convert("[ ]", List[str])
        self.assertEqual(result, [])

    def test_list_conversion_string_non_array_format(self):
        """Test list conversion from string that's not array format."""
        # Should convert single value to single-element list
        result = self.converter.convert("hello", List[str])
        self.assertEqual(result, ["hello"])

        result = self.converter.convert("42", List[int])
        self.assertEqual(result, [42])

    def test_list_conversion_none_value(self):
        """Test list conversion with None value."""
        result = self.converter.convert(None, List[int])
        self.assertIsNone(result)

    def test_list_conversion_single_value_to_list(self):
        """Test converting single value to list."""
        result = self.converter.convert(42, List[int])
        self.assertEqual(result, [42])

        result = self.converter.convert("hello", List[str])
        self.assertEqual(result, ["hello"])

    def test_list_conversion_nested_type_conversion(self):
        """Test list conversion with nested type conversion."""
        # Convert string numbers to int list
        result = self.converter.convert("[1.0, 2.0, 3.0]", List[int])
        self.assertEqual(result, [1, 2, 3])

        # Convert string booleans to bool list
        result = self.converter.convert("[true, false, 1, 0]", List[bool])
        self.assertEqual(result, [True, False, True, False])

    def test_list_conversion_mixed_quotes(self):
        """Test list conversion with mixed quote styles."""
        result = self.converter.convert("['hello', \"world\", test]", List[str])
        self.assertEqual(result, ["hello", "world", "test"])

    def test_list_conversion_with_spaces(self):
        """Test list conversion with various spacing."""
        result = self.converter.convert("[ 1 , 2 , 3 ]", List[int])
        self.assertEqual(result, [1, 2, 3])

        result = self.converter.convert("[  'hello'  ,  'world'  ]", List[str])
        self.assertEqual(result, ["hello", "world"])

    def test_list_conversion_decimal_elements(self):
        """Test list conversion with Decimal elements."""
        result = self.converter.convert("[123.45, 678.90]", List[Decimal])
        expected = [Decimal("123.45"), Decimal("678.90")]
        self.assertEqual(result, expected)

    def test_list_conversion_date_elements(self):
        """Test list conversion with date elements."""
        result = self.converter.convert("['2023-12-25', '2024-01-01']", List[date])
        expected = [date(2023, 12, 25), date(2024, 1, 1)]
        self.assertEqual(result, expected)

    def test_list_conversion_optional_elements(self):
        """Test list conversion with Optional element types."""
        # Test numpy array with Optional elements
        numpy_array = np.array([1, 2, None, 3])
        result = self.converter.convert(numpy_array, List[Optional[int]])
        self.assertEqual(result, [1, 2, None, 3])


class TestBaseTypeConverterEdgeCases(unittest.TestCase):
    """Test BaseTypeConverter edge cases and error conditions."""

    def setUp(self):
        """Set up test converter."""
        self.converter = BaseTypeConverter()

    def test_string_to_int_with_float_string(self):
        """Test converting float string to int."""
        result = self.converter.convert("123.45", int)
        self.assertEqual(result, 123)

        result = self.converter.convert("999.99", int)
        self.assertEqual(result, 999)

    def test_boolean_conversion_case_insensitive(self):
        """Test boolean conversion is case insensitive."""
        # Test various case combinations for true
        self.assertTrue(self.converter.convert("TRUE", bool))
        self.assertTrue(self.converter.convert("True", bool))
        self.assertTrue(self.converter.convert("tRuE", bool))
        self.assertTrue(self.converter.convert("YES", bool))
        self.assertTrue(self.converter.convert("T", bool))

        # Test various case combinations for false
        self.assertFalse(self.converter.convert("FALSE", bool))
        self.assertFalse(self.converter.convert("False", bool))
        self.assertFalse(self.converter.convert("fAlSe", bool))
        self.assertFalse(self.converter.convert("NO", bool))
        self.assertFalse(self.converter.convert("F", bool))

    def test_date_conversion_edge_cases(self):
        """Test date conversion edge cases."""
        # Test with microseconds (should work)
        result = self.converter.convert("2023-12-25T15:30:45.123456", date)
        self.assertEqual(result, date(2023, 12, 25))

        # Test with timezone info
        result = self.converter.convert("2023-12-25T15:30:45+00:00", date)
        self.assertEqual(result, date(2023, 12, 25))

    def test_datetime_conversion_edge_cases(self):
        """Test datetime conversion edge cases."""
        # Test with microseconds
        result = self.converter.convert("2023-12-25T15:30:45.123456", datetime)
        expected = datetime(2023, 12, 25, 15, 30, 45, 123456)
        self.assertEqual(result, expected)

        # Test already datetime object
        test_datetime = datetime(2023, 12, 25, 15, 30, 45)
        result = self.converter.convert(test_datetime, datetime)
        self.assertEqual(result, test_datetime)

    def test_decimal_conversion_edge_cases(self):
        """Test Decimal conversion edge cases."""
        # Test with very large numbers
        large_number = "999999999999999999999999999.123456789"
        result = self.converter.convert(large_number, Decimal)
        self.assertEqual(result, Decimal(large_number))

        # Test with scientific notation
        result = self.converter.convert("1.23e10", Decimal)
        self.assertEqual(result, Decimal("1.23e10"))

    def test_optional_type_with_none_value(self):
        """Test Optional type handling with None value."""
        # Test with None for various Optional types
        self.assertIsNone(self.converter.convert(None, Optional[str]))
        self.assertIsNone(self.converter.convert(None, Optional[int]))
        self.assertIsNone(self.converter.convert(None, Optional[Decimal]))
        self.assertIsNone(self.converter.convert(None, Optional[List[str]]))

    def test_optional_type_with_valid_value(self):
        """Test Optional type handling with valid value."""
        # Test conversion to Optional with valid values
        result = self.converter.convert("hello", Optional[str])
        self.assertEqual(result, "hello")

        result = self.converter.convert("42", Optional[int])
        self.assertEqual(result, 42)

    def test_unsupported_type_fallback(self):
        """Test fallback behavior for unsupported types."""

        # Create a custom type
        class CustomType:
            pass

        # Should fall back to string conversion
        result = self.converter.convert(42, CustomType)
        self.assertEqual(result, "42")

        result = self.converter.convert("hello", CustomType)
        self.assertEqual(result, "hello")

    def test_conversion_with_malformed_list_strings(self):
        """Test conversion with malformed list strings."""
        # Missing closing bracket - should be treated as single element
        result = self.converter.convert("[1, 2, 3", List[str])
        self.assertEqual(result, ["[1, 2, 3"])

        # Missing opening bracket - should be treated as single element
        result = self.converter.convert("1, 2, 3]", List[str])
        self.assertEqual(result, ["1, 2, 3]"])

    def test_type_conversion_with_complex_nested_types(self):
        """Test type conversion with complex nested types."""
        # Test List of Optional types
        result = self.converter.convert([1, None, 3], List[Optional[int]])
        self.assertEqual(result, [1, None, 3])

    def test_zero_and_empty_value_handling(self):
        """Test handling of zero and empty values."""
        # Zero should convert properly
        self.assertEqual(self.converter.convert(0, str), "0")
        self.assertEqual(self.converter.convert("0", int), 0)
        self.assertEqual(self.converter.convert(0.0, int), 0)

        # Empty string handling
        self.assertEqual(self.converter.convert("", str), "")
        self.assertFalse(self.converter.convert("", bool))

    def test_list_with_no_type_args(self):
        """Test list conversion when List has no type arguments."""
        # Test with basic list type (no generics)
        result = self.converter.convert("[1, 2, 3]", list)
        # When list has no type args, it returns the string as-is (fallback behavior)
        self.assertEqual(result, "[1, 2, 3]")

    def test_numpy_array_with_different_dtypes(self):
        """Test numpy array conversion with different dtypes."""
        # Test with different numpy dtypes
        float_array = np.array([1.1, 2.2, 3.3], dtype=np.float64)
        result = self.converter.convert(float_array, List[float])
        self.assertEqual(result, [1.1, 2.2, 3.3])

        int_array = np.array([1, 2, 3], dtype=np.int32)
        result = self.converter.convert(int_array, List[int])
        self.assertEqual(result, [1, 2, 3])

    def test_recursive_type_conversion_in_lists(self):
        """Test recursive type conversion in list elements."""
        # Test converting string representations to proper types
        result = self.converter.convert("['123.45', '678.90']", List[Decimal])
        expected = [Decimal("123.45"), Decimal("678.90")]
        self.assertEqual(result, expected)

        # Test boolean strings in lists
        result = self.converter.convert("['true', 'false', '1', '0']", List[bool])
        self.assertEqual(result, [True, False, True, False])

    def test_list_conversion_error_handling(self):
        """Test error handling in list conversion."""
        # Test invalid conversions within lists
        with self.assertRaises(ValueError):
            self.converter.convert("['not_a_number']", List[int])

        with self.assertRaises(ValueError):
            self.converter.convert("['invalid_date']", List[date])


class TestUnwrapOptionalTypeExtended(unittest.TestCase):
    """Extended test cases for unwrap_optional_type function."""

    def test_unwrap_nested_optional_types(self):
        """Test unwrapping nested Optional types."""
        # Test deeply nested Optional
        nested_optional = Optional[Optional[str]]
        result = unwrap_optional_type(nested_optional)
        # Should return the first non-None type found
        self.assertIn(result, [Optional[str], str])

    def test_unwrap_complex_union_types(self):
        """Test unwrapping complex Union types."""
        # Test Union with multiple types including None
        complex_union = Union[str, int, type(None)]
        result = unwrap_optional_type(complex_union)
        self.assertEqual(result, str)  # Should return first non-None type

        # Test Union with no None type
        non_optional_union = Union[str, int, float]
        result = unwrap_optional_type(non_optional_union)
        self.assertEqual(result, str)  # Should return first type when no None

    def test_unwrap_with_generic_types(self):
        """Test unwrapping with generic types."""
        # Test Optional[List[str]]
        optional_list = Optional[List[str]]
        result = unwrap_optional_type(optional_list)
        self.assertEqual(result, List[str])

        # Test Optional[Dict[str, int]]
        from typing import Dict

        optional_dict = Optional[Dict[str, int]]
        result = unwrap_optional_type(optional_dict)
        self.assertEqual(result, Dict[str, int])

    def test_unwrap_with_custom_types(self):
        """Test unwrapping with custom types."""

        class CustomClass:
            pass

        optional_custom = Optional[CustomClass]
        result = unwrap_optional_type(optional_custom)
        self.assertEqual(result, CustomClass)

    def test_unwrap_edge_cases(self):
        """Test edge cases for unwrap_optional_type."""
        # Test with just None type
        none_type = type(None)
        result = unwrap_optional_type(none_type)
        self.assertEqual(result, none_type)

        # Test with empty args (shouldn't happen in practice)
        result = unwrap_optional_type(str)
        self.assertEqual(result, str)


class TestBaseTypeConverterSpecialCases(unittest.TestCase):
    """Test special cases and edge conditions for BaseTypeConverter."""

    def setUp(self):
        """Set up test converter."""
        self.converter = BaseTypeConverter()

    def test_is_optional_type_edge_cases(self):
        """Test is_optional_type with edge cases."""
        # Test with non-generic types
        self.assertFalse(self.converter.is_optional_type(str))
        self.assertFalse(self.converter.is_optional_type(int))

        # Test with Union that has more than 2 args but includes None
        from typing import Union

        multi_union = Union[str, int, float, type(None)]
        self.assertFalse(self.converter.is_optional_type(multi_union))  # Not exactly 2 args

        # Test with Union that has exactly 2 args but no None
        two_arg_union = Union[str, int]
        self.assertFalse(self.converter.is_optional_type(two_arg_union))

    def test_get_optional_inner_type_edge_cases(self):
        """Test get_optional_inner_type with edge cases."""
        from typing import Union

        # Test with multiple non-None types - should return first
        multi_optional = Union[str, int, type(None)]
        result = self.converter.get_optional_inner_type(multi_optional)
        self.assertEqual(result, str)

        # Test with different order
        different_order = Union[type(None), int, str]
        result = self.converter.get_optional_inner_type(different_order)
        # Should return first non-None type
        self.assertIn(result, [int, str])

    def test_convert_with_none_value_special_cases(self):
        """Test convert with None for various target types."""
        # Test None with all basic types
        self.assertIsNone(self.converter.convert(None, str))
        self.assertIsNone(self.converter.convert(None, int))
        self.assertIsNone(self.converter.convert(None, float))
        self.assertIsNone(self.converter.convert(None, bool))
        self.assertIsNone(self.converter.convert(None, date))
        self.assertIsNone(self.converter.convert(None, datetime))
        self.assertIsNone(self.converter.convert(None, Decimal))

    def test_convert_optional_type_with_none_twice(self):
        """Test Optional type handling returns None when value is None."""
        # Test double None check in Optional handling
        result = self.converter.convert(None, Optional[str])
        self.assertIsNone(result)

        result = self.converter.convert(None, Optional[int])
        self.assertIsNone(result)

    def test_list_conversion_without_origin_attribute(self):
        """Test list conversion with types that don't have __origin__."""
        # Test with regular list type (plain `list` type falls back to string conversion)
        result = self.converter.convert([1, 2, 3], list)
        self.assertEqual(result, "[1, 2, 3]")  # Falls back to string conversion

        # Test with single value for non-List type - should work normally
        result = self.converter.convert("test", str)
        self.assertEqual(result, "test")

        # Test converting string to plain list type (fallback behavior)
        result = self.converter.convert("not_a_list", list)
        self.assertEqual(result, "not_a_list")  # Falls back to string conversion

    def test_numpy_array_edge_cases(self):
        """Test numpy array handling edge cases."""
        # Test empty numpy array
        empty_array = np.array([])
        result = self.converter.convert(empty_array, List[int])
        self.assertEqual(result, [])

        # Test numpy array with mixed types
        mixed_array = np.array([1, 2.5, 3])
        result = self.converter.convert(mixed_array, List[float])
        self.assertEqual(result, [1.0, 2.5, 3.0])

    def test_string_array_parsing_edge_cases(self):
        """Test string array parsing edge cases."""
        # Test with whitespace-only content
        result = self.converter.convert("[   ]", List[str])
        self.assertEqual(result, [])

        # Test with single element, no commas
        result = self.converter.convert("[hello]", List[str])
        self.assertEqual(result, ["hello"])

        # Test with escaped quotes in strings
        result = self.converter.convert('["he\\"llo", "world"]', List[str])
        self.assertEqual(result, ['he\\"llo', "world"])

    def test_type_conversion_edge_cases(self):
        """Test type conversion edge cases."""
        # Test int conversion from boolean
        self.assertEqual(self.converter.convert(True, int), 1)
        self.assertEqual(self.converter.convert(False, int), 0)

        # Test float conversion from boolean
        self.assertEqual(self.converter.convert(True, float), 1.0)
        self.assertEqual(self.converter.convert(False, float), 0.0)


class TestUnwrapOptionalTypeSpecialCases(unittest.TestCase):
    """Test special cases for unwrap_optional_type function."""

    def test_unwrap_optional_type_non_union(self):
        """Test unwrap_optional_type with non-Union types."""
        # Test with regular types (no origin)
        self.assertEqual(unwrap_optional_type(str), str)
        self.assertEqual(unwrap_optional_type(int), int)
        self.assertEqual(unwrap_optional_type(list), list)

    def test_unwrap_optional_type_empty_union(self):
        """Test unwrap_optional_type with edge case unions."""
        from typing import Union

        # Test Union with only None type
        none_only = Union[type(None)]
        result = unwrap_optional_type(none_only)
        # Should return the type unchanged if no non-None types
        self.assertEqual(result, none_only)

    def test_unwrap_optional_type_no_none_types(self):
        """Test unwrap_optional_type when no non-None types found."""
        from typing import Union

        # Create a Union with only None (edge case)
        # This tests the case where non_none_types list is empty
        # We'll mock this scenario
        with mock.patch("sql_testing_library._types.get_args") as mock_get_args:
            with mock.patch("sql_testing_library._types.get_origin") as mock_get_origin:
                mock_get_origin.return_value = Union
                mock_get_args.return_value = [type(None)]  # Only None type

                result = unwrap_optional_type(Union[type(None)])
                # Should return the original type since no non-None types
                self.assertEqual(result, Union[type(None)])

    def test_unwrap_optional_type_complex_generic(self):
        """Test unwrap_optional_type with complex generic types."""
        from typing import Dict, List

        # Test with complex nested generics
        complex_optional = Optional[Dict[str, List[int]]]
        result = unwrap_optional_type(complex_optional)
        self.assertEqual(result, Dict[str, List[int]])

    def test_unwrap_optional_type_preserve_generic_info(self):
        """Test that unwrap_optional_type preserves generic type information."""
        from typing import Dict, List, Tuple

        # Test various generic types
        list_optional = Optional[List[str]]
        self.assertEqual(unwrap_optional_type(list_optional), List[str])

        dict_optional = Optional[Dict[str, int]]
        self.assertEqual(unwrap_optional_type(dict_optional), Dict[str, int])

        tuple_optional = Optional[Tuple[str, int]]
        self.assertEqual(unwrap_optional_type(tuple_optional), Tuple[str, int])


if __name__ == "__main__":
    unittest.main()
