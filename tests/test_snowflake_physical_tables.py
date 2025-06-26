"""Tests for Snowflake using physical tables."""

from datetime import date
from decimal import Decimal

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


class Person(BaseModel):
    """Test person model."""

    id: int
    name: str
    dob: date
    active: bool


class PersonWithAge(BaseModel):
    """Result model with calculated age."""

    id: int
    name: str
    age: int


class PersonMockTable(BaseMockTable):
    """Mock table for persons."""

    def __init__(self, data):
        super().__init__(data)

    def get_database_name(self) -> str:
        return "test_db.sqltesting"

    def get_table_name(self) -> str:
        return "persons"


class Department(BaseModel):
    """Test department model."""

    id: int
    name: str


class PersonWithDept(BaseModel):
    """Test person with department model."""

    id: int
    name: str
    dob: date
    active: bool
    dept_id: int


class PersonDeptResult(BaseModel):
    """Result model for join query."""

    person_id: int
    person_name: str
    department_name: str


class DepartmentMockTable(BaseMockTable):
    """Mock table for departments."""

    def __init__(self, data):
        super().__init__(data)

    def get_database_name(self) -> str:
        return "test_db.sqltesting"

    def get_table_name(self) -> str:
        return "departments"


@pytest.mark.integration
@pytest.mark.snowflake
class TestSnowflakePhysicalTables:
    """Test SQL testing with Snowflake physical tables."""

    def test_basic_query_with_physical_tables(self):
        """Test basic query with physical tables."""
        # Create test data
        persons = [
            Person(id=1, name="Alice", dob=date(1990, 1, 15), active=True),
            Person(id=2, name="Bob", dob=date(1995, 5, 25), active=False),
            Person(id=3, name="Charlie", dob=date(1985, 11, 10), active=True),
        ]

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[PersonMockTable(persons)],
            result_class=PersonWithAge,
        )
        def query_persons_with_age():
            return TestCase(
                query="""
                    SELECT
                        id,
                        name,
                        EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM dob) AS age
                    FROM persons
                    ORDER BY id
                """,
                default_namespace="test_db.sqltesting",
                use_physical_tables=True,
            )

        # Execute the test
        results = query_persons_with_age()

        # Verify results
        assert len(results) == 3
        assert results[0].id == 1
        assert results[0].name == "Alice"
        assert results[0].age >= 33  # Age depends on current year
        assert results[1].id == 2
        assert results[1].name == "Bob"
        assert results[2].id == 3
        assert results[2].name == "Charlie"

    def test_join_query_with_physical_tables(self):
        """Test join query with multiple physical tables."""
        # Create test data
        persons_with_dept = [
            PersonWithDept(id=1, name="Alice", dob=date(1990, 1, 15), active=True, dept_id=1),
            PersonWithDept(id=2, name="Bob", dob=date(1995, 5, 25), active=False, dept_id=2),
            PersonWithDept(id=3, name="Charlie", dob=date(1985, 11, 10), active=True, dept_id=3),
        ]

        departments = [
            Department(id=1, name="Engineering"),
            Department(id=2, name="Marketing"),
            Department(id=3, name="HR"),
        ]

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[
                PersonMockTable(persons_with_dept),
                DepartmentMockTable(departments),
            ],
            result_class=PersonDeptResult,
        )
        def query_persons_with_departments():
            return TestCase(
                query="""
                    SELECT
                        p.id AS person_id,
                        p.name AS person_name,
                        d.name AS department_name
                    FROM persons p
                    JOIN departments d ON p.dept_id = d.id
                    ORDER BY p.id
                """,
                default_namespace="test_db.sqltesting",
                use_physical_tables=True,
            )

        # Execute the test
        results = query_persons_with_departments()

        # Verify results
        assert len(results) == 3
        assert results[0].person_id == 1
        assert results[0].person_name == "Alice"
        assert results[0].department_name == "Engineering"
        assert results[1].person_id == 2
        assert results[1].person_name == "Bob"
        assert results[1].department_name == "Marketing"
        assert results[2].person_id == 3
        assert results[2].person_name == "Charlie"
        assert results[2].department_name == "HR"

    def test_snowflake_specific_functions(self):
        """Test Snowflake-specific functions with physical tables."""
        test_data = [
            {"id": 1, "value": Decimal("123.45"), "description": "test item"},
            {"id": 2, "value": Decimal("678.90"), "description": "another item"},
        ]

        class TestDataMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db.sqltesting"

            def get_table_name(self) -> str:
                return "test_data"

        class SnowflakeResult(BaseModel):
            id: int
            value: Decimal
            rounded_value: int
            description_upper: str

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[TestDataMockTable(test_data)],
            result_class=SnowflakeResult,
        )
        def query_with_snowflake_functions():
            return TestCase(
                query="""
                    SELECT
                        id,
                        value,
                        ROUND(value) AS rounded_value,
                        UPPER(description) AS description_upper
                    FROM test_data
                    ORDER BY id
                """,
                default_namespace="test_db.sqltesting",
                use_physical_tables=True,
            )

        # Execute the test
        results = query_with_snowflake_functions()

        # Verify results
        assert len(results) == 2
        assert results[0].id == 1
        assert results[0].value == Decimal("123.45")
        assert results[0].rounded_value == 123
        assert results[0].description_upper == "TEST ITEM"
        assert results[1].id == 2
        assert results[1].value == Decimal("678.90")
        assert results[1].rounded_value == 679
        assert results[1].description_upper == "ANOTHER ITEM"


if __name__ == "__main__":
    pytest.main([__file__, "-xvs"])
