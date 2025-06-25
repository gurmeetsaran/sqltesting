"""Example demonstrating struct type support in Athena and Trino."""

from dataclasses import dataclass
from decimal import Decimal
from typing import List

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


@dataclass
class Address:
    """Nested struct representing an address."""

    street: str
    city: str
    state: str
    zip_code: str


@dataclass
class Employee:
    """Struct representing an employee."""

    id: int
    name: str
    email: str
    salary: Decimal
    address: Address
    is_active: bool = True


@dataclass
class Department:
    """Data model with struct fields."""

    dept_id: int
    dept_name: str
    manager: Employee
    employees: List[Employee]


class DepartmentMockTable(BaseMockTable):
    """Mock table for departments."""

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "departments"


def example_struct_queries():
    """Demonstrate struct type queries."""

    # Create sample data
    departments = [
        Department(
            dept_id=1,
            dept_name="Engineering",
            manager=Employee(
                id=101,
                name="Alice Johnson",
                email="alice@company.com",
                salary=Decimal("120000.00"),
                address=Address(
                    street="123 Tech Lane", city="San Francisco", state="CA", zip_code="94105"
                ),
                is_active=True,
            ),
            employees=[
                Employee(
                    id=102,
                    name="Bob Smith",
                    email="bob@company.com",
                    salary=Decimal("95000.00"),
                    address=Address(
                        street="456 Code Ave", city="San Francisco", state="CA", zip_code="94107"
                    ),
                    is_active=True,
                ),
                Employee(
                    id=103,
                    name="Charlie Davis",
                    email="charlie@company.com",
                    salary=Decimal("85000.00"),
                    address=Address(
                        street="789 Dev Road", city="Oakland", state="CA", zip_code="94612"
                    ),
                    is_active=True,
                ),
            ],
        )
    ]

    # Example 1: Query struct fields using dot notation
    @sql_test(
        adapter_type="athena",  # or "trino"
        mock_tables=[DepartmentMockTable(departments)],
        result_class=dict,
    )
    def query_manager_info():
        return TestCase(
            query="""
                SELECT
                    dept_id,
                    dept_name,
                    manager.name AS manager_name,
                    manager.email AS manager_email,
                    manager.address.city AS manager_city,
                    manager.address.state AS manager_state
                FROM departments
                WHERE dept_id = 1
            """,
            default_namespace="test_db",
        )

    # Example 2: Query with struct in WHERE clause
    @sql_test(
        adapter_type="athena",
        mock_tables=[DepartmentMockTable(departments)],
        result_class=dict,
    )
    def query_high_salary_managers():
        return TestCase(
            query="""
                SELECT
                    dept_name,
                    manager.name AS manager_name,
                    manager.salary AS salary
                FROM departments
                WHERE manager.salary > 100000
                    AND manager.is_active = TRUE
            """,
            default_namespace="test_db",
        )

    # Example 3: Query entire struct
    @sql_test(
        adapter_type="athena",
        mock_tables=[DepartmentMockTable(departments)],
        result_class=dict,
    )
    def query_full_struct():
        return TestCase(
            query="""
                SELECT
                    dept_id,
                    dept_name,
                    manager
                FROM departments
            """,
            default_namespace="test_db",
        )

    print("Example 1: Manager information using dot notation")
    result1 = query_manager_info()
    for row in result1:
        print(f"  Department: {row['dept_name']}")
        print(f"  Manager: {row['manager_name']} ({row['manager_email']})")
        print(f"  Location: {row['manager_city']}, {row['manager_state']}")
        print()

    print("Example 2: High salary managers")
    result2 = query_high_salary_managers()
    for row in result2:
        print(f"  {row['manager_name']} - ${row['salary']}")
        print()

    print("Example 3: Full struct data")
    result3 = query_full_struct()
    for row in result3:
        print(f"  Department {row['dept_id']}: {row['dept_name']}")
        print(f"  Manager struct: {row['manager']}")
        print()


if __name__ == "__main__":
    # Note: This example requires a configured Athena or Trino connection
    # Set appropriate environment variables before running
    print("Struct Type Support Example")
    print("=" * 50)

    try:
        example_struct_queries()
    except Exception as e:
        print("Note: To run this example, configure your Athena/Trino connection")
        print(f"Error: {e}")
