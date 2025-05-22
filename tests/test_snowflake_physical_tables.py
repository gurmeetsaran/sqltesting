"""Tests for Snowflake using physical tables."""

import unittest
from datetime import date
from unittest import mock

import pandas as pd

from sql_testing_library.adapters.snowflake import SnowflakeAdapter
from sql_testing_library.core import SQLTester, mock_table


class Person:
    """Test person class."""

    def __init__(self, id: int, name: str, dob: date, active: bool) -> None:
        self.id = id
        self.name = name
        self.dob = dob
        self.active = active


@mock.patch("snowflake.connector.connect")
class TestSnowflakePhysicalTables(unittest.TestCase):
    """Test SQL testing with Snowflake physical tables."""

    def setUp(self):
        """Set up test data."""
        self.persons = [
            Person(1, "Alice", date(1990, 1, 15), True),
            Person(2, "Bob", date(1995, 5, 25), False),
            Person(3, "Charlie", date(1985, 11, 10), True),
        ]

    def test_basic_query(self, mock_snowflake_connect):
        """Test basic query with mock data."""
        # Set up mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Mock the cursor results for both the CTAS and the query
        def execute_side_effect(query):
            # For the query execution, set appropriate mock data
            if "SELECT" in query and "TEMP_persons" in query:
                mock_cursor.description = [("id",), ("name",), ("age",)]
                mock_cursor.fetchall.return_value = [
                    (1, "Alice", 33),
                    (2, "Bob", 28),
                    (3, "Charlie", 38),
                ]
            # For cleanup, just return empty result
            elif "DROP TABLE" in query:
                mock_cursor.description = None
                mock_cursor.fetchall.return_value = []
            # For CTAS statement, return empty result
            else:
                mock_cursor.description = None
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = execute_side_effect

        # Create adapter and tester
        adapter = SnowflakeAdapter(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
        )

        tester = SQLTester(adapter)

        # Define the test SQL - a simple query with age calculation
        sql = """
        SELECT
            id,
            name,
            EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM dob) AS age
        FROM persons
        ORDER BY id
        """

        # Create the mock table fixture
        persons_table = mock_table(self.persons)

        # Run the test
        with mock.patch("time.time", return_value=1234567890.123):
            result = tester.run(sql, {"persons": persons_table})

        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(list(result.columns), ["id", "name", "age"])

        # Check that a CTAS statement was executed
        ctas_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "CREATE TEMPORARY TABLE" in call[0][0]
        ]
        self.assertEqual(len(ctas_calls), 1)

        # Check that a cleanup was performed
        drop_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "DROP TABLE" in call[0][0]
        ]
        self.assertEqual(len(drop_calls), 1)

    def test_join_query(self, mock_snowflake_connect):
        """Test join query with multiple mock tables."""
        # Set up mock cursor and connection
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_snowflake_connect.return_value = mock_conn

        # Define a second test class
        class Department:
            def __init__(self, id: int, name: str) -> None:
                self.id = id
                self.name = name

        # Create test data
        departments = [
            Department(1, "Engineering"),
            Department(2, "Marketing"),
            Department(3, "HR"),
        ]

        # Create an extended Person class with department_id
        class PersonWithDept(Person):
            def __init__(
                self, id: int, name: str, dob: date, active: bool, dept_id: int
            ) -> None:
                super().__init__(id, name, dob, active)
                self.dept_id = dept_id

        persons_with_dept = [
            PersonWithDept(1, "Alice", date(1990, 1, 15), True, 1),
            PersonWithDept(2, "Bob", date(1995, 5, 25), False, 2),
            PersonWithDept(3, "Charlie", date(1985, 11, 10), True, 3),
        ]

        # Mock the cursor results for different queries
        def execute_side_effect(query):
            # For the join query execution
            if "SELECT" in query and "JOIN" in query:
                mock_cursor.description = [
                    ("person_id",),
                    ("person_name",),
                    ("department_name",),
                ]
                mock_cursor.fetchall.return_value = [
                    (1, "Alice", "Engineering"),
                    (2, "Bob", "Marketing"),
                    (3, "Charlie", "HR"),
                ]
            # For cleanup or CTAS, return empty result
            else:
                mock_cursor.description = None
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = execute_side_effect

        # Create adapter and tester
        adapter = SnowflakeAdapter(
            account="test_account",
            user="test_user",
            password="test_password",
            database="test_db",
        )

        tester = SQLTester(adapter)

        # Define the test SQL - a join query
        sql = """
        SELECT
            p.id AS person_id,
            p.name AS person_name,
            d.name AS department_name
        FROM persons p
        JOIN departments d ON p.dept_id = d.id
        ORDER BY p.id
        """

        # Create the mock table fixtures
        persons_table = mock_table(persons_with_dept)
        departments_table = mock_table(departments)

        # Run the test
        result = tester.run(
            sql, {"persons": persons_table, "departments": departments_table}
        )

        # Verify results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(
            list(result.columns), ["person_id", "person_name", "department_name"]
        )

        # Check that CTAS statements were executed for both tables
        ctas_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "CREATE TEMPORARY TABLE" in call[0][0]
        ]
        self.assertEqual(len(ctas_calls), 2)

        # Check that cleanup was performed for both tables
        drop_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "DROP TABLE" in call[0][0]
        ]
        self.assertEqual(len(drop_calls), 2)


if __name__ == "__main__":
    unittest.main()
