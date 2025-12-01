"""Additional tests for mock table to improve coverage."""

import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from sql_testing_library._mock_table import BaseMockTable, _is_pydantic_model


# Try to import Pydantic for testing
try:
    from pydantic import BaseModel

    PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = None  # type: ignore
    PYDANTIC_AVAILABLE = False


class TestMockTableAdditional(BaseMockTable):
    """Test mock table implementation."""

    __test__ = False

    def get_database_name(self) -> str:
        return "test_db"

    def get_table_name(self) -> str:
        return "test_table"


@unittest.skipIf(not PYDANTIC_AVAILABLE, "Pydantic not available")
class TestPydanticModelSupport(unittest.TestCase):
    """Test Pydantic model support in mock tables."""

    def test_is_pydantic_model_detection(self):
        """Test _is_pydantic_model function."""

        class User(BaseModel):
            id: int
            name: str
            email: Optional[str] = None

        user = User(id=1, name="Alice", email="alice@test.com")

        # Should detect Pydantic model instance
        self.assertTrue(_is_pydantic_model(user))

        # Should not detect non-Pydantic objects
        self.assertFalse(_is_pydantic_model("string"))
        self.assertFalse(_is_pydantic_model(123))
        self.assertFalse(_is_pydantic_model({}))

    def test_pydantic_model_initialization(self):
        """Test initialization with Pydantic model instances."""

        class User(BaseModel):
            id: int
            name: str
            email: Optional[str] = None
            active: bool = True

        users = [
            User(id=1, name="Alice", email="alice@test.com", active=True),
            User(id=2, name="Bob", email="bob@test.com", active=False),
        ]

        table = TestMockTableAdditional(users)

        # Should convert to dictionaries
        self.assertEqual(len(table.data), 2)
        self.assertEqual(table.data[0]["name"], "Alice")
        self.assertEqual(table.data[0]["email"], "alice@test.com")
        self.assertEqual(table.data[1]["name"], "Bob")

    def test_pydantic_to_dict_conversion(self):
        """Test _pydantic_to_dict method."""

        class Product(BaseModel):
            id: int
            name: str
            price: float
            in_stock: bool

        product = Product(id=1, name="Widget", price=19.99, in_stock=True)

        table = TestMockTableAdditional([])
        result = table._pydantic_to_dict(product)

        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Widget")
        self.assertEqual(result["price"], 19.99)
        self.assertEqual(result["in_stock"], True)

    def test_pydantic_to_dict_with_none_values(self):
        """Test _pydantic_to_dict with None values."""

        class User(BaseModel):
            id: int
            name: str
            email: Optional[str] = None
            phone: Optional[str] = None

        user = User(id=1, name="Alice", email=None, phone=None)

        table = TestMockTableAdditional([])
        result = table._pydantic_to_dict(user)

        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Alice")
        self.assertIsNone(result["email"])
        self.assertIsNone(result["phone"])

    def test_pydantic_model_to_dataframe(self):
        """Test converting Pydantic models to DataFrame."""

        class Order(BaseModel):
            order_id: int
            customer_name: str
            amount: float
            status: str

        orders = [
            Order(order_id=1, customer_name="Alice", amount=100.50, status="completed"),
            Order(order_id=2, customer_name="Bob", amount=75.25, status="pending"),
        ]

        table = TestMockTableAdditional(orders)
        df = table.to_dataframe()

        self.assertEqual(len(df), 2)
        self.assertIn("order_id", df.columns)
        self.assertIn("customer_name", df.columns)
        self.assertEqual(df.iloc[0]["customer_name"], "Alice")
        self.assertEqual(df.iloc[1]["amount"], 75.25)

    def test_pydantic_get_column_types(self):
        """Test get_column_types with Pydantic models."""

        class Employee(BaseModel):
            id: int
            name: str
            salary: float
            active: bool
            hire_date: Optional[str] = None

        employees = [
            Employee(id=1, name="Alice", salary=75000.0, active=True, hire_date="2023-01-01")
        ]

        table = TestMockTableAdditional(employees)
        column_types = table.get_column_types()

        self.assertEqual(column_types["id"], int)
        self.assertEqual(column_types["name"], str)
        self.assertEqual(column_types["salary"], float)
        self.assertEqual(column_types["active"], bool)
        # Optional types should unwrap
        self.assertEqual(column_types["hire_date"], str)


class TestQualifiedNameAndAlias(unittest.TestCase):
    """Test qualified name and CTE alias generation."""

    def test_get_qualified_name(self):
        """Test get_qualified_name method."""

        class CustomTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "production"

            def get_table_name(self) -> str:
                return "users"

        table = CustomTable([{"id": 1}])
        qualified_name = table.get_qualified_name()

        self.assertEqual(qualified_name, "production.users")

    def test_get_qualified_name_with_schema(self):
        """Test qualified name with schema-like database."""

        class SchemaTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "mydb.myschema"

            def get_table_name(self) -> str:
                return "orders"

        table = SchemaTable([{"id": 1}])
        qualified_name = table.get_qualified_name()

        self.assertEqual(qualified_name, "mydb.myschema.orders")

    def test_get_cte_alias(self):
        """Test get_cte_alias method."""

        class SimpleTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "users"

        table = SimpleTable([{"id": 1}])
        cte_alias = table.get_cte_alias()

        self.assertEqual(cte_alias, "test_db__users")

    def test_get_cte_alias_with_special_chars(self):
        """Test CTE alias handles special characters."""

        class SpecialTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "my-database.my.schema"

            def get_table_name(self) -> str:
                return "users"

        table = SpecialTable([{"id": 1}])
        cte_alias = table.get_cte_alias()

        # Should replace hyphens and dots with underscores
        self.assertEqual(cte_alias, "my_database_my_schema__users")
        # Should not contain special characters
        self.assertNotIn("-", cte_alias)
        self.assertNotIn(".", cte_alias.replace("__", ""))


class TestDateTimeHandling(unittest.TestCase):
    """Test datetime and timedelta type handling."""

    def test_datetime_column_type_inference(self):
        """Test get_column_types infers datetime correctly."""
        # Create data with datetime column using dictionary
        data = [
            {"id": 1, "timestamp": datetime(2023, 1, 1, 10, 30)},
            {"id": 2, "timestamp": datetime(2023, 1, 2, 14, 45)},
        ]

        table = TestMockTableAdditional(data)
        column_types = table.get_column_types()

        # Should infer datetime type from pandas dtype
        self.assertEqual(column_types["timestamp"], datetime)

    def test_timedelta_column_type_inference(self):
        """Test get_column_types infers timedelta correctly."""
        # Create data with timedelta column
        data = [
            {"id": 1, "duration": timedelta(hours=2, minutes=30)},
            {"id": 2, "duration": timedelta(hours=1, minutes=15)},
        ]

        table = TestMockTableAdditional(data)
        df = table.to_dataframe()

        # Ensure pandas recognizes it as timedelta
        self.assertTrue(pd.api.types.is_timedelta64_dtype(df["duration"]))

        column_types = table.get_column_types()

        # Should infer timedelta type
        self.assertEqual(column_types["duration"], timedelta)

    def test_mixed_datetime_types(self):
        """Test handling of mixed datetime types in DataFrame."""

        @dataclass
        class Event:
            id: int
            created_at: datetime
            duration: timedelta

        events = [
            Event(1, datetime(2023, 1, 1, 10, 0), timedelta(hours=2)),
            Event(2, datetime(2023, 1, 2, 11, 0), timedelta(hours=3)),
        ]

        table = TestMockTableAdditional(events)
        column_types = table.get_column_types()

        self.assertEqual(column_types["created_at"], datetime)
        self.assertEqual(column_types["duration"], timedelta)


class TestObjectDtypeInference(unittest.TestCase):
    """Test object dtype inference with edge cases."""

    def test_all_null_values_column(self):
        """Test column type inference when all values are null."""
        # Dictionary data with a column that has all None values
        data = [
            {"id": 1, "name": "Alice", "optional_field": None},
            {"id": 2, "name": "Bob", "optional_field": None},
            {"id": 3, "name": "Charlie", "optional_field": None},
        ]

        table = TestMockTableAdditional(data)
        column_types = table.get_column_types()

        # Should default to str for all-null object columns
        self.assertIn("optional_field", column_types)
        self.assertEqual(column_types["optional_field"], str)

    def test_mixed_null_and_valid_values(self):
        """Test column with some null and some valid values."""
        data = [
            {"id": 1, "notes": None},
            {"id": 2, "notes": "Some text"},
            {"id": 3, "notes": None},
        ]

        table = TestMockTableAdditional(data)
        column_types = table.get_column_types()

        # Should infer type from non-null values
        self.assertEqual(column_types["notes"], str)

    def test_object_dtype_with_numbers_as_strings(self):
        """Test object dtype that contains numeric strings."""
        data = [
            {"id": 1, "code": "12345"},
            {"id": 2, "code": "67890"},
        ]

        table = TestMockTableAdditional(data)
        column_types = table.get_column_types()

        # Should treat as strings (object dtype)
        self.assertEqual(column_types["code"], str)


class TestDataclassToDictEdgeCases(unittest.TestCase):
    """Test edge cases for dataclass to dict conversion."""

    def test_dataclass_to_dict_with_non_dataclass(self):
        """Test _dataclass_to_dict with non-dataclass object."""
        # When called with a dict, should return it as-is
        table = TestMockTableAdditional([])
        input_dict = {"id": 1, "name": "test"}

        result = table._dataclass_to_dict(input_dict)

        # Should return the dict unchanged
        self.assertEqual(result, input_dict)

    def test_pydantic_to_dict_with_non_pydantic(self):
        """Test _pydantic_to_dict with non-Pydantic object."""
        table = TestMockTableAdditional([])
        input_dict = {"id": 1, "name": "test"}

        result = table._pydantic_to_dict(input_dict)

        # Should return the dict unchanged
        self.assertEqual(result, input_dict)


if __name__ == "__main__":
    unittest.main()
