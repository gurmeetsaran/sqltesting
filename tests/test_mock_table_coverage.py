"""Test mock table functionality and edge cases."""

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sql_testing_library.mock_table import BaseMockTable


@dataclass
class TestUser:
    """Test user dataclass."""

    __test__ = False  # Tell pytest this is not a test class

    id: int
    name: str
    email: Optional[str] = None
    active: bool = True
    created_at: Optional[date] = None


@dataclass
class SimpleUser:
    """Simple user dataclass without optional fields."""

    id: int
    name: str


class TestMockTable(BaseMockTable):
    """Test mock table implementation."""

    __test__ = False  # Tell pytest this is not a test class

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "test_table"


class TestBaseMockTable(unittest.TestCase):
    """Test BaseMockTable functionality."""

    def test_empty_data_initialization(self):
        """Test initialization with empty data."""
        table = TestMockTable([])

        self.assertEqual(table.data, [])
        self.assertEqual(table.get_database_name(), "test_db")
        self.assertEqual(table.get_table_name(), "test_table")

    def test_dataclass_data_initialization(self):
        """Test initialization with dataclass instances."""
        users = [
            TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1)),
            TestUser(2, "Bob", "bob@test.com", False, date(2023, 2, 1)),
        ]

        table = TestMockTable(users)
        self.assertEqual(len(table.data), 2)
        self.assertEqual(table.data[0]["name"], "Alice")

    def test_dictionary_data_initialization(self):
        """Test initialization with dictionary data."""
        dict_data = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False},
        ]

        table = TestMockTable(dict_data)
        self.assertEqual(len(table.data), 2)
        self.assertEqual(table.data[0]["name"], "Alice")

    def test_mixed_data_types_handling(self):
        """Test handling of mixed data types."""
        # The implementation may handle mixed types gracefully
        # by converting everything to dictionaries
        mixed_data = [TestUser(1, "Alice"), {"id": 2, "name": "Bob"}]

        # This should work - implementation normalizes data
        table = TestMockTable(mixed_data)
        self.assertEqual(len(table.data), 2)

    def test_empty_data_to_dataframe(self):
        """Test to_dataframe with empty data."""
        table = TestMockTable([])
        df = table.to_dataframe()

        self.assertTrue(df.empty)
        self.assertEqual(len(df.columns), 0)

    def test_dataclass_to_dataframe(self):
        """Test to_dataframe with dataclass data."""
        users = [
            TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1)),
            TestUser(2, "Bob", None, False, None),
        ]

        table = TestMockTable(users)
        df = table.to_dataframe()

        self.assertEqual(len(df), 2)
        self.assertIn("id", df.columns)
        self.assertIn("name", df.columns)
        self.assertIn("email", df.columns)
        self.assertIn("active", df.columns)
        self.assertIn("created_at", df.columns)

        # Check values
        self.assertEqual(df.iloc[0]["name"], "Alice")
        self.assertEqual(df.iloc[1]["name"], "Bob")
        self.assertIsNone(df.iloc[1]["email"])

    def test_dictionary_to_dataframe(self):
        """Test to_dataframe with dictionary data."""
        dict_data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.2},
        ]

        table = TestMockTable(dict_data)
        df = table.to_dataframe()

        self.assertEqual(len(df), 2)
        self.assertIn("id", df.columns)
        self.assertIn("name", df.columns)
        self.assertIn("score", df.columns)

        self.assertEqual(df.iloc[0]["score"], 95.5)

    def test_get_column_types_empty_data(self):
        """Test get_column_types with empty data."""
        table = TestMockTable([])
        column_types = table.get_column_types()

        self.assertEqual(column_types, {})

    def test_get_column_types_dataclass(self):
        """Test get_column_types with dataclass data."""
        users = [TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1))]
        table = TestMockTable(users)
        column_types = table.get_column_types()

        self.assertEqual(column_types["id"], int)
        self.assertEqual(column_types["name"], str)
        self.assertEqual(column_types["active"], bool)
        # Optional types should resolve to their base type
        self.assertEqual(column_types["email"], str)
        self.assertEqual(column_types["created_at"], date)

    def test_get_column_types_dictionary_fallback(self):
        """Test get_column_types with dictionary data (pandas inference)."""
        dict_data = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False},
        ]

        table = TestMockTable(dict_data)
        column_types = table.get_column_types()

        # Should infer types from pandas
        self.assertIn("id", column_types)
        self.assertIn("name", column_types)
        self.assertIn("score", column_types)
        self.assertIn("active", column_types)

    def test_get_column_types_with_none_values(self):
        """Test get_column_types when first row has None values."""
        # Test when first row has None in some columns
        dict_data = [
            {"id": None, "name": None, "score": None},
            {"id": 1, "name": "Alice", "score": 95.5},
        ]

        table = TestMockTable(dict_data)
        column_types = table.get_column_types()

        # Should still infer correct types from non-None values
        self.assertIn("id", column_types)
        self.assertIn("name", column_types)
        self.assertIn("score", column_types)

    def test_dataclass_to_dict(self):
        """Test _dataclass_to_dict method."""
        user = TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1))
        table = TestMockTable([])

        result = table._dataclass_to_dict(user)

        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Alice")
        self.assertEqual(result["email"], "alice@test.com")
        self.assertEqual(result["active"], True)
        self.assertEqual(result["created_at"], date(2023, 1, 1))

    def test_dataclass_to_dict_with_none_values(self):
        """Test _dataclass_to_dict with None values."""
        user = TestUser(1, "Alice", None, True, None)
        table = TestMockTable([])

        result = table._dataclass_to_dict(user)

        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Alice")
        self.assertIsNone(result["email"])
        self.assertEqual(result["active"], True)
        self.assertIsNone(result["created_at"])

    def test_large_dataset(self):
        """Test with larger dataset."""
        # Create larger dataset
        large_data = [
            TestUser(i, f"User{i}", f"user{i}@test.com", i % 2 == 0, date(2023, 1, 1))
            for i in range(100)
        ]

        table = TestMockTable(large_data)
        df = table.to_dataframe()

        self.assertEqual(len(df), 100)
        self.assertEqual(df.iloc[50]["name"], "User50")

    def test_various_data_types(self):
        """Test with various data types."""

        @dataclass
        class ComplexUser:
            id: int
            name: str
            salary: Decimal
            hired_date: date
            last_login: datetime
            score: float
            active: bool
            notes: Optional[str] = None

        complex_data = [
            ComplexUser(
                1,
                "Alice",
                Decimal("75000.50"),
                date(2023, 1, 1),
                datetime(2023, 12, 1, 10, 30),
                98.5,
                True,
                "Great employee",
            ),
            ComplexUser(
                2,
                "Bob",
                Decimal("85000.00"),
                date(2023, 2, 1),
                datetime(2023, 12, 1, 14, 15),
                92.1,
                False,
                None,
            ),
        ]

        class ComplexMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "hr_db"

            def get_table_name(self) -> str:
                return "employees"

        table = ComplexMockTable(complex_data)
        df = table.to_dataframe()
        column_types = table.get_column_types()

        self.assertEqual(len(df), 2)
        self.assertEqual(column_types["id"], int)
        self.assertEqual(column_types["name"], str)
        self.assertEqual(column_types["salary"], Decimal)
        self.assertEqual(column_types["hired_date"], date)
        self.assertEqual(column_types["last_login"], datetime)
        self.assertEqual(column_types["score"], float)
        self.assertEqual(column_types["active"], bool)
        self.assertEqual(column_types["notes"], str)

    def test_abstract_methods_enforcement(self):
        """Test that abstract methods must be implemented."""

        class IncompleteMockTable(BaseMockTable):
            # Missing implementations
            pass

        with self.assertRaises(TypeError):
            IncompleteMockTable([])

    def test_custom_table_implementations(self):
        """Test custom table implementations."""

        class UsersMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "production"

            def get_table_name(self) -> str:
                return "users"

        class OrdersMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "ecommerce"

            def get_table_name(self) -> str:
                return "orders"

        users_table = UsersMockTable([{"id": 1, "name": "Alice"}])
        orders_table = OrdersMockTable([{"id": 1, "user_id": 1, "amount": 100}])

        self.assertEqual(users_table.get_database_name(), "production")
        self.assertEqual(users_table.get_table_name(), "users")
        self.assertEqual(orders_table.get_database_name(), "ecommerce")
        self.assertEqual(orders_table.get_table_name(), "orders")

    def test_special_characters_in_data(self):
        """Test handling of special characters in data."""
        special_data = [
            {"id": 1, "name": "O'Connor", "notes": "Test with 'quotes'"},
            {"id": 2, "name": "José María", "notes": "Unicode: café ñoño"},
            {"id": 3, "name": "Test\nNewline", "notes": "Tab\tcharacter"},
        ]

        table = TestMockTable(special_data)
        df = table.to_dataframe()

        self.assertEqual(len(df), 3)
        self.assertEqual(df.iloc[0]["name"], "O'Connor")
        self.assertEqual(df.iloc[1]["name"], "José María")
        self.assertIn("\n", df.iloc[2]["name"])

    def test_edge_case_data_handling(self):
        """Test handling of edge case data."""
        # Test with simple valid data
        simple_data = [{"id": 1, "name": "test"}]
        table = TestMockTable(simple_data)
        self.assertEqual(len(table.data), 1)

        # Test dataframe creation works
        df = table.to_dataframe()
        self.assertEqual(len(df), 1)


if __name__ == "__main__":
    unittest.main()
