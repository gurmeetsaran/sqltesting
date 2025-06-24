"""Integration tests for struct types across database adapters."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class Address:
    """Nested struct for testing."""

    street: str
    city: str
    zip_code: str


@dataclass
class Person:
    """Main struct for testing."""

    name: str
    age: int
    salary: Decimal
    address: Address
    is_active: bool = True


class AddressPydantic(BaseModel):
    """Pydantic version of Address struct."""

    street: str
    city: str
    zip_code: str


class PersonPydantic(BaseModel):
    """Pydantic version of Person struct."""

    name: str
    age: int
    salary: Decimal
    address: AddressPydantic
    is_active: bool = True


@dataclass
class StructTypesData:
    """Test data class with struct fields."""

    id: int
    person: Person
    optional_person: Optional[Person] = None


class StructTypesResult(BaseModel):
    """Result model for struct types test."""

    id: int
    person: PersonPydantic
    optional_person: Optional[PersonPydantic]


class StructTypesMockTable(BaseMockTable):
    """Mock table for struct types testing."""

    def __init__(self, data, database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "struct_types"


@pytest.mark.integration
@pytest.mark.parametrize("adapter_type", ["athena", "trino"])
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestStructTypesIntegration:
    """Integration tests for struct types in Athena and Trino."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self, adapter_type):
        """Set up test data specific to each adapter."""
        # Common test data structure for all adapters
        self.test_data = [
            StructTypesData(
                id=1,
                person=Person(
                    name="John Doe",
                    age=30,
                    salary=Decimal("75000.50"),
                    address=Address(street="123 Main St", city="New York", zip_code="10001"),
                    is_active=True,
                ),
                optional_person=Person(
                    name="Jane Smith",
                    age=25,
                    salary=Decimal("65000.00"),
                    address=Address(street="456 Oak Ave", city="Boston", zip_code="02101"),
                    is_active=False,
                ),
            ),
            StructTypesData(
                id=2,
                person=Person(
                    name="Bob Johnson",
                    age=45,
                    salary=Decimal("95000.75"),
                    address=Address(street="789 Pine Rd", city="Chicago", zip_code="60601"),
                    is_active=True,
                ),
                optional_person=None,
            ),
            StructTypesData(
                id=3,
                person=Person(
                    name="Alice Brown",
                    age=35,
                    salary=Decimal("85000.25"),
                    address=Address(street="321 Elm Way", city="Seattle", zip_code="98101"),
                    is_active=True,
                ),
                optional_person=Person(
                    name="Charlie Davis",
                    age=40,
                    salary=Decimal("90000.00"),
                    address=Address(street="654 Maple Dr", city="Portland", zip_code="97201"),
                    is_active=True,
                ),
            ),
        ]

        # Set database name based on adapter type
        if adapter_type == "athena":
            self.database_name = "test_db"
        elif adapter_type == "trino":
            self.database_name = "memory"

    def test_struct_types_basic_query(self, adapter_type, use_physical_tables):
        """Test basic struct type queries."""

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[StructTypesMockTable(self.test_data, self.database_name)],
            result_class=StructTypesResult,
        )
        def query_struct_types():
            return TestCase(
                query="""
                    SELECT
                        id,
                        person,
                        optional_person
                    FROM struct_types
                    ORDER BY id
                """,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_struct_types()
        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.person.name == "John Doe"
        assert row1.person.age == 30
        assert row1.person.salary == Decimal("75000.50")
        assert row1.person.address.street == "123 Main St"
        assert row1.person.address.city == "New York"
        assert row1.person.address.zip_code == "10001"
        assert row1.person.is_active is True
        assert row1.optional_person is not None
        assert row1.optional_person.name == "Jane Smith"

        # Verify second row (with NULL optional_person)
        row2 = results[1]
        assert row2.id == 2
        assert row2.person.name == "Bob Johnson"
        assert row2.optional_person is None

        # Verify third row
        row3 = results[2]
        assert row3.id == 3
        assert row3.person.name == "Alice Brown"
        assert row3.optional_person is not None
        assert row3.optional_person.name == "Charlie Davis"

    def test_struct_field_access_with_dot_notation(self, adapter_type, use_physical_tables):
        """Test accessing struct fields using dot notation."""

        @dataclass
        class DotNotationResult:
            id: int
            person_name: str
            person_age: int
            person_salary: Decimal
            address_city: str
            address_zip: str
            is_active: bool

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[StructTypesMockTable(self.test_data, self.database_name)],
            result_class=DotNotationResult,
        )
        def query_struct_fields():
            return TestCase(
                query="""
                    SELECT
                        id,
                        person.name AS person_name,
                        person.age AS person_age,
                        person.salary AS person_salary,
                        person.address.city AS address_city,
                        person.address.zip_code AS address_zip,
                        person.is_active AS is_active
                    FROM struct_types
                    ORDER BY id
                """,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_struct_fields()
        assert len(results) == 3

        # Verify first row
        row1 = results[0]
        assert row1.id == 1
        assert row1.person_name == "John Doe"
        assert row1.person_age == 30
        assert row1.person_salary == Decimal("75000.50")
        assert row1.address_city == "New York"
        assert row1.address_zip == "10001"
        assert row1.is_active is True

        # Verify other rows
        row2 = results[1]
        assert row2.person_name == "Bob Johnson"
        assert row2.address_city == "Chicago"

        row3 = results[2]
        assert row3.person_name == "Alice Brown"
        assert row3.address_city == "Seattle"

    def test_struct_in_where_clause(self, adapter_type, use_physical_tables):
        """Test using struct fields in WHERE clause."""

        @dataclass
        class WhereClauseResult:
            id: int
            person_name: str
            city: str

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[StructTypesMockTable(self.test_data, self.database_name)],
            result_class=WhereClauseResult,
        )
        def query_with_where():
            return TestCase(
                query="""
                    SELECT
                        id,
                        person.name AS person_name,
                        person.address.city AS city
                    FROM struct_types
                    WHERE person.age > 30
                        AND person.is_active = TRUE
                    ORDER BY id
                """,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_with_where()
        assert len(results) == 2

        # Should return Bob Johnson and Alice Brown
        assert results[0].person_name == "Bob Johnson"
        assert results[1].person_name == "Alice Brown"

    def test_struct_with_null_handling(self, adapter_type, use_physical_tables):
        """Test NULL handling in optional struct fields."""

        @dataclass
        class NullHandlingResult:
            id: int
            has_optional_person: bool
            optional_person_name: Optional[str]

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[StructTypesMockTable(self.test_data, self.database_name)],
            result_class=NullHandlingResult,
        )
        def query_null_handling():
            return TestCase(
                query="""
                    SELECT
                        id,
                        optional_person IS NOT NULL AS has_optional_person,
                        CASE
                            WHEN optional_person IS NOT NULL
                            THEN optional_person.name
                            ELSE NULL
                        END AS optional_person_name
                    FROM struct_types
                    ORDER BY id
                """,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_null_handling()
        assert len(results) == 3

        # First row has optional_person
        assert results[0].has_optional_person is True
        assert results[0].optional_person_name == "Jane Smith"

        # Second row has NULL optional_person
        assert results[1].has_optional_person is False
        assert results[1].optional_person_name is None

        # Third row has optional_person
        assert results[2].has_optional_person is True
        assert results[2].optional_person_name == "Charlie Davis"


if __name__ == "__main__":
    pytest.main([__file__])
