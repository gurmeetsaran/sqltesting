"""Unit tests for struct type support."""

from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from sql_testing_library._sql_utils import format_sql_value
from sql_testing_library._types import BaseTypeConverter, is_pydantic_model_class, is_struct_type


@dataclass
class Address:
    street: str
    city: str
    zip_code: str


@dataclass
class Person:
    name: str
    age: int
    address: Address


class PydanticAddress(BaseModel):
    street: str
    city: str
    zip_code: str


class PydanticPerson(BaseModel):
    name: str
    age: int
    address: PydanticAddress


class TestStructTypeDetection:
    """Test struct type detection functions."""

    def test_is_struct_type_with_dataclass(self):
        """Test that dataclasses are detected as struct types."""
        assert is_struct_type(Person) is True
        assert is_struct_type(Address) is True

    def test_is_struct_type_with_pydantic(self):
        """Test that Pydantic models are detected as struct types."""
        assert is_struct_type(PydanticPerson) is True
        assert is_struct_type(PydanticAddress) is True

    def test_is_struct_type_with_regular_types(self):
        """Test that regular types are not detected as struct types."""
        assert is_struct_type(str) is False
        assert is_struct_type(int) is False
        assert is_struct_type(list) is False
        assert is_struct_type(dict) is False

    def test_is_pydantic_model_class(self):
        """Test Pydantic model class detection."""
        assert is_pydantic_model_class(PydanticPerson) is True
        assert is_pydantic_model_class(PydanticAddress) is True
        assert is_pydantic_model_class(Person) is False
        assert is_pydantic_model_class(str) is False


class TestStructFormatting:
    """Test struct value formatting for SQL."""

    def test_format_dataclass_struct_athena(self):
        """Test formatting dataclass structs for Athena."""
        person = Person(
            name="John Doe",
            age=30,
            address=Address(street="123 Main St", city="New York", zip_code="10001"),
        )

        result = format_sql_value(person, Person, dialect="athena")
        # Should format as CAST(ROW(...) AS ROW(...))
        assert result.startswith("CAST(ROW(")
        assert "'John Doe'" in result
        assert "30" in result
        assert "ROW(" in result  # Nested struct
        assert "'123 Main St'" in result
        assert "'New York'" in result
        assert "'10001'" in result
        assert "AS ROW(" in result  # Type cast
        assert "name VARCHAR" in result
        assert "age BIGINT" in result  # Athena uses BIGINT for integers

    def test_format_dataclass_struct_trino(self):
        """Test formatting dataclass structs for Trino."""
        person = Person(
            name="Jane Smith",
            age=25,
            address=Address(street="456 Oak Ave", city="Boston", zip_code="02101"),
        )

        result = format_sql_value(person, Person, dialect="trino")
        # Should format as CAST(ROW(...) AS ROW(...))
        assert result.startswith("CAST(ROW(")
        assert "'Jane Smith'" in result
        assert "25" in result
        assert "AS ROW(" in result
        assert "age BIGINT" in result  # Trino uses BIGINT

    def test_format_pydantic_struct_athena(self):
        """Test formatting Pydantic structs for Athena."""
        person = PydanticPerson(
            name="Bob Johnson",
            age=45,
            address=PydanticAddress(street="789 Pine Rd", city="Chicago", zip_code="60601"),
        )

        result = format_sql_value(person, PydanticPerson, dialect="athena")
        # Should format as CAST(ROW(...) AS ROW(...))
        assert result.startswith("CAST(ROW(")
        assert "'Bob Johnson'" in result
        assert "45" in result
        assert "AS ROW(" in result

    def test_format_null_struct_athena(self):
        """Test formatting NULL struct for Athena."""
        result = format_sql_value(None, Person, dialect="athena")
        # Should format as CAST(NULL AS ROW(...))
        assert result.startswith("CAST(NULL AS ROW(")
        assert "name VARCHAR" in result
        assert "age BIGINT" in result  # Athena uses BIGINT for integers
        assert "address ROW(" in result  # Nested struct type

    def test_format_null_struct_trino(self):
        """Test formatting NULL struct for Trino."""
        result = format_sql_value(None, Person, dialect="trino")
        # Should format as CAST(NULL AS ROW(...))
        assert result.startswith("CAST(NULL AS ROW(")
        assert "name VARCHAR" in result
        assert "age BIGINT" in result  # Trino uses BIGINT instead of INTEGER

    def test_format_struct_unsupported_dialect(self):
        """Test that struct formatting raises error for unsupported dialects."""
        person = Person(
            name="Test", age=20, address=Address(street="St", city="City", zip_code="12345")
        )

        with pytest.raises(NotImplementedError) as exc_info:
            format_sql_value(person, Person, dialect="mysql")

        assert "Struct type not yet supported for dialect: mysql" in str(exc_info.value)


class TestStructTupleConversion:
    """Test struct conversion from tuple format (as returned by Athena/Trino)."""

    def test_convert_tuple_to_dataclass(self):
        """Test converting tuple to dataclass struct."""
        converter = BaseTypeConverter()

        # Tuple representing Address('123 Main St', 'New York', '10001')
        address_tuple = ("123 Main St", "New York", "10001")
        result = converter.convert(address_tuple, Address)

        assert isinstance(result, Address)
        assert result.street == "123 Main St"
        assert result.city == "New York"
        assert result.zip_code == "10001"

    def test_convert_nested_tuple_to_dataclass(self):
        """Test converting nested tuple to dataclass with nested struct."""
        converter = BaseTypeConverter()

        # Tuple representing Person with nested Address
        person_tuple = ("John Doe", 30, ("123 Main St", "New York", "10001"))
        result = converter.convert(person_tuple, Person)

        assert isinstance(result, Person)
        assert result.name == "John Doe"
        assert result.age == 30
        assert isinstance(result.address, Address)
        assert result.address.street == "123 Main St"
        assert result.address.city == "New York"
        assert result.address.zip_code == "10001"

    def test_convert_tuple_to_pydantic(self):
        """Test converting tuple to Pydantic model."""
        converter = BaseTypeConverter()

        # Tuple representing Address
        address_tuple = ("456 Oak Ave", "Boston", "02101")
        result = converter.convert(address_tuple, PydanticAddress)

        assert isinstance(result, PydanticAddress)
        assert result.street == "456 Oak Ave"
        assert result.city == "Boston"
        assert result.zip_code == "02101"

    def test_convert_nested_tuple_to_pydantic(self):
        """Test converting nested tuple to Pydantic model with nested struct."""
        converter = BaseTypeConverter()

        # Tuple representing Person with nested Address
        person_tuple = ("Jane Smith", 25, ("789 Pine Rd", "Chicago", "60601"))
        result = converter.convert(person_tuple, PydanticPerson)

        assert isinstance(result, PydanticPerson)
        assert result.name == "Jane Smith"
        assert result.age == 25
        assert isinstance(result.address, PydanticAddress)
        assert result.address.street == "789 Pine Rd"
        assert result.address.city == "Chicago"
        assert result.address.zip_code == "60601"


class TestListOfStructs:
    """Test list of structs support."""

    def test_format_list_of_dataclass_structs_athena(self):
        """Test formatting list of dataclass structs for Athena."""
        from typing import List

        addresses = [
            Address(street="123 Main St", city="New York", zip_code="10001"),
            Address(street="456 Oak Ave", city="Boston", zip_code="02101"),
            Address(street="789 Pine Rd", city="Chicago", zip_code="60601"),
        ]

        result = format_sql_value(addresses, List[Address], dialect="athena")
        # Should format as ARRAY[CAST(ROW(...) AS ROW(...)), ...]
        assert result.startswith("ARRAY[")
        assert result.endswith("]")
        assert result.count("CAST(ROW(") == 3  # Three structs
        assert "'123 Main St'" in result
        assert "'New York'" in result
        assert "'456 Oak Ave'" in result
        assert "'Boston'" in result
        assert "'789 Pine Rd'" in result
        assert "'Chicago'" in result

    def test_format_list_of_pydantic_structs_trino(self):
        """Test formatting list of Pydantic structs for Trino."""
        from typing import List

        addresses = [
            PydanticAddress(street="123 Main St", city="New York", zip_code="10001"),
            PydanticAddress(street="456 Oak Ave", city="Boston", zip_code="02101"),
        ]

        result = format_sql_value(addresses, List[PydanticAddress], dialect="trino")
        # Should format as ARRAY[CAST(ROW(...) AS ROW(...)), ...]
        assert result.startswith("ARRAY[")
        assert result.endswith("]")
        assert result.count("CAST(ROW(") == 2  # Two structs
        assert "street VARCHAR" in result
        assert "city VARCHAR" in result
        assert "zip_code VARCHAR" in result

    def test_format_empty_list_of_structs_athena(self):
        """Test formatting empty list of structs for Athena."""
        from typing import List

        result = format_sql_value([], List[Address], dialect="athena")
        # Should format as ARRAY[]
        assert result == "ARRAY[]"

    def test_format_null_list_of_structs_athena(self):
        """Test formatting NULL list of structs for Athena."""
        from typing import List

        result = format_sql_value(None, List[Address], dialect="athena")
        # Should format as CAST(NULL AS ARRAY(ROW(...)))
        assert result.startswith("CAST(NULL AS ARRAY(")
        assert "ROW(" in result
        assert "street VARCHAR" in result
        assert "city VARCHAR" in result
        assert "zip_code VARCHAR" in result

    def test_convert_list_of_tuples_to_dataclass_structs(self):
        """Test converting list of tuples to list of dataclass structs."""
        from typing import List

        converter = BaseTypeConverter()

        # List of tuples representing addresses
        address_tuples = [
            ("123 Main St", "New York", "10001"),
            ("456 Oak Ave", "Boston", "02101"),
            ("789 Pine Rd", "Chicago", "60601"),
        ]

        result = converter.convert(address_tuples, List[Address])

        assert isinstance(result, list)
        assert len(result) == 3

        # Check first address
        assert isinstance(result[0], Address)
        assert result[0].street == "123 Main St"
        assert result[0].city == "New York"
        assert result[0].zip_code == "10001"

        # Check second address
        assert isinstance(result[1], Address)
        assert result[1].street == "456 Oak Ave"
        assert result[1].city == "Boston"
        assert result[1].zip_code == "02101"

        # Check third address
        assert isinstance(result[2], Address)
        assert result[2].street == "789 Pine Rd"
        assert result[2].city == "Chicago"
        assert result[2].zip_code == "60601"

    def test_convert_list_of_string_structs_to_dataclass(self):
        """Test converting list of string representations to list of dataclass structs."""
        from typing import List

        converter = BaseTypeConverter()

        # List of string representations (as might come from Athena)
        address_strings = [
            "{123 Main St, New York, 10001}",
            "{456 Oak Ave, Boston, 02101}",
        ]

        result = converter.convert(address_strings, List[Address])

        assert isinstance(result, list)
        assert len(result) == 2

        # Check addresses
        assert isinstance(result[0], Address)
        assert result[0].street == "123 Main St"
        assert result[0].city == "New York"
        assert result[0].zip_code == "10001"

        assert isinstance(result[1], Address)
        assert result[1].street == "456 Oak Ave"
        assert result[1].city == "Boston"
        assert result[1].zip_code == "02101"


if __name__ == "__main__":
    pytest.main([__file__])
