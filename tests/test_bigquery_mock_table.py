"""Test BigQueryMockTable functionality."""

import unittest
from dataclasses import dataclass
from datetime import date
from typing import Optional

from sql_testing_library._mock_table import BigQueryMockTable


@dataclass
class TestUser:
    """Test user dataclass."""

    __test__ = False  # Tell pytest this is not a test class

    id: int
    name: str
    email: Optional[str] = None
    active: bool = True
    created_at: Optional[date] = None


class TestBigQueryMockTable(unittest.TestCase):
    """Test BigQueryMockTable functionality."""

    def test_initialization_with_class_variables(self):
        """Test initialization using class variables."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "users"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data)

        self.assertEqual(table.get_project_name(), "test-project")
        self.assertEqual(table.get_dataset_name(), "test_dataset")
        self.assertEqual(table.get_table_name(), "users")
        self.assertEqual(len(table.data), 1)

    def test_missing_class_variables_raises_error(self):
        """Test that missing class variables raise AttributeError."""
        data = [{"id": 1, "name": "Alice"}]
        table = BigQueryMockTable(data)

        # Should raise AttributeError when trying to access unset class variable
        with self.assertRaises(AttributeError):
            table.get_project_name()

    def test_get_database_name_returns_project_and_dataset(self):
        """Test that get_database_name returns project.dataset."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "my-project"
            bigquery_dataset = "analytics"
            bigquery_table = "users"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data)

        self.assertEqual(table.get_database_name(), "my-project.analytics")

    def test_get_qualified_name_returns_two_part(self):
        """Test that get_qualified_name returns database.table (project.dataset.table)."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "my-project"
            bigquery_dataset = "analytics"
            bigquery_table = "users"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data)

        # get_qualified_name() from base class combines database_name + table_name
        # For BigQuery: database_name is "project.dataset", table_name is "table"
        # Result: "project.dataset.table"
        self.assertEqual(table.get_qualified_name(), "my-project.analytics.users")

    def test_get_fully_qualified_name_returns_three_part(self):
        """Test that get_fully_qualified_name returns project.dataset.table."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "my-project"
            bigquery_dataset = "analytics"
            bigquery_table = "users"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data)

        self.assertEqual(table.get_fully_qualified_name(), "my-project.analytics.users")

    def test_get_cte_alias_sanitizes_special_characters(self):
        """Test that get_cte_alias properly sanitizes dots and hyphens."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "my-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "user-data"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data)

        # CTE alias should replace dots and hyphens with underscores
        # database_name = "my-project.test_dataset" -> "my_project_test_dataset"
        # table_name = "user-data" -> "user_data"
        cte_alias = table.get_cte_alias()
        self.assertEqual(cte_alias, "my_project_test_dataset__user_data")
        self.assertNotIn(".", cte_alias)
        self.assertNotIn("-", cte_alias)

    def test_dataclass_data_works_with_bigquery_mock_table(self):
        """Test that BigQueryMockTable works with dataclass data."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "users"

        users = [
            TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1)),
            TestUser(2, "Bob", "bob@test.com", False, date(2023, 2, 1)),
        ]

        table = TestTable(data=users)

        self.assertEqual(len(table.data), 2)
        self.assertEqual(table.data[0]["name"], "Alice")
        self.assertEqual(table.data[1]["name"], "Bob")

    def test_to_dataframe_works_with_bigquery_mock_table(self):
        """Test that to_dataframe works correctly."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "users"

        users = [
            TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1)),
            TestUser(2, "Bob", None, False, None),
        ]

        table = TestTable(data=users)

        df = table.to_dataframe()

        self.assertEqual(len(df), 2)
        self.assertIn("id", df.columns)
        self.assertIn("name", df.columns)
        self.assertIn("email", df.columns)
        self.assertEqual(df.iloc[0]["name"], "Alice")
        self.assertEqual(df.iloc[1]["name"], "Bob")
        self.assertIsNone(df.iloc[1]["email"])

    def test_get_column_types_works_with_bigquery_mock_table(self):
        """Test that get_column_types works correctly."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "users"

        users = [TestUser(1, "Alice", "alice@test.com", True, date(2023, 1, 1))]

        table = TestTable(data=users)

        column_types = table.get_column_types()

        self.assertEqual(column_types["id"], int)
        self.assertEqual(column_types["name"], str)
        self.assertEqual(column_types["active"], bool)
        self.assertEqual(column_types["email"], str)
        self.assertEqual(column_types["created_at"], date)

    def test_empty_data_initialization(self):
        """Test initialization with empty data."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "users"

        table = TestTable(data=[])

        self.assertEqual(table.data, [])
        self.assertEqual(table.get_project_name(), "test-project")
        self.assertEqual(table.get_dataset_name(), "test_dataset")
        self.assertEqual(table.get_table_name(), "users")

    def test_complex_project_and_dataset_names(self):
        """Test with complex project and dataset names containing special chars."""

        class TestTable(BigQueryMockTable):
            bigquery_project = "my-company-prod-123"
            bigquery_dataset = "analytics_v2"
            bigquery_table = "user_events"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data=data)

        self.assertEqual(table.get_project_name(), "my-company-prod-123")
        self.assertEqual(table.get_dataset_name(), "analytics_v2")
        self.assertEqual(table.get_table_name(), "user_events")
        self.assertEqual(
            table.get_fully_qualified_name(),
            "my-company-prod-123.analytics_v2.user_events",
        )

    def test_inheritance_from_base_mock_table(self):
        """Test that BigQueryMockTable properly inherits from BaseMockTable."""
        from sql_testing_library._mock_table import BaseMockTable

        class TestTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "test_dataset"
            bigquery_table = "users"

        data = [{"id": 1, "name": "Alice"}]
        table = TestTable(data=data)

        # Should be instance of both
        self.assertIsInstance(table, BigQueryMockTable)
        self.assertIsInstance(table, BaseMockTable)

        # Should have all base class methods
        self.assertTrue(hasattr(table, "get_database_name"))
        self.assertTrue(hasattr(table, "get_table_name"))
        self.assertTrue(hasattr(table, "get_qualified_name"))
        self.assertTrue(hasattr(table, "get_cte_alias"))
        self.assertTrue(hasattr(table, "to_dataframe"))
        self.assertTrue(hasattr(table, "get_column_types"))

    def test_class_variables_work_across_instances(self):
        """Test that class variables work correctly with multiple instances."""

        class Table1(BigQueryMockTable):
            bigquery_project = "shared-project"
            bigquery_dataset = "dataset1"
            bigquery_table = "table1"

        class Table2(BigQueryMockTable):
            bigquery_project = "shared-project"
            bigquery_dataset = "dataset2"
            bigquery_table = "table2"

        data1 = [{"id": 1, "name": "Alice"}]
        table1 = Table1(data=data1)

        data2 = [{"id": 2, "name": "Bob"}]
        table2 = Table2(data=data2)

        # Both should use the same project
        self.assertEqual(table1.get_project_name(), "shared-project")
        self.assertEqual(table2.get_project_name(), "shared-project")

        # But have different datasets/tables
        self.assertEqual(table1.get_dataset_name(), "dataset1")
        self.assertEqual(table2.get_dataset_name(), "dataset2")

    def test_class_variable_pattern(self):
        """Test the class variable pattern for defining BigQuery mock tables."""

        class CustomUsersMockTable(BigQueryMockTable):
            bigquery_project = "my-project"
            bigquery_dataset = "my_dataset"
            bigquery_table = "users"

        data = [{"id": 1, "name": "Alice"}]
        table = CustomUsersMockTable(data)

        self.assertEqual(table.get_project_name(), "my-project")
        self.assertEqual(table.get_dataset_name(), "my_dataset")
        self.assertEqual(table.get_table_name(), "users")
        self.assertEqual(table.get_database_name(), "my-project.my_dataset")
        self.assertEqual(table.get_fully_qualified_name(), "my-project.my_dataset.users")

    def test_class_variable_with_dataclass(self):
        """Test class variable pattern works with dataclass data."""

        class OrdersMockTable(BigQueryMockTable):
            bigquery_project = "test-project"
            bigquery_dataset = "sales"
            bigquery_table = "orders"

        @dataclass
        class Order:
            order_id: int
            amount: float

        orders = [Order(1, 99.99), Order(2, 149.99)]
        table = OrdersMockTable(orders)

        self.assertEqual(len(table.data), 2)
        self.assertEqual(table.get_fully_qualified_name(), "test-project.sales.orders")

    def test_all_class_variables_together(self):
        """Test using all three class variables together."""

        class ProductsMockTable(BigQueryMockTable):
            bigquery_project = "my-project"
            bigquery_dataset = "inventory"
            bigquery_table = "products"

        data = [{"id": 1, "name": "Widget"}]
        table = ProductsMockTable(data)

        # Should use all class variables
        self.assertEqual(table.get_project_name(), "my-project")
        self.assertEqual(table.get_dataset_name(), "inventory")
        self.assertEqual(table.get_table_name(), "products")
        self.assertEqual(table.get_fully_qualified_name(), "my-project.inventory.products")


if __name__ == "__main__":
    unittest.main()
