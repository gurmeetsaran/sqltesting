"""Test that both Optional[X] and X | None syntax work correctly."""

import sys
import unittest
from typing import Optional

from pydantic import BaseModel


# Only run these tests on Python 3.10+ where | None syntax is supported
PYTHON_310_PLUS = sys.version_info >= (3, 10)


class TestUnionSyntaxSupport(unittest.TestCase):
    """Test Optional vs | None syntax support."""

    def test_optional_syntax_in_is_union_type(self):
        """Test that is_union_type works with Optional syntax."""
        from sql_testing_library._types import is_union_type

        class TestModel(BaseModel):
            optional_field: Optional[str] = None

        from typing import get_type_hints

        hints = get_type_hints(TestModel)
        self.assertTrue(is_union_type(hints["optional_field"]))

    @unittest.skipIf(not PYTHON_310_PLUS, "Python 3.10+ required for | None syntax")
    def test_pipe_none_syntax_in_is_union_type(self):
        """Test that is_union_type works with | None syntax."""
        from sql_testing_library._types import is_union_type

        # Use exec to avoid syntax error on Python 3.9
        exec(
            """
class TestModel(BaseModel):
    union_field: str | None = None
""",
            globals(),
        )

        from typing import get_type_hints

        hints = get_type_hints(globals()["TestModel"])
        self.assertTrue(is_union_type(hints["union_field"]))

    def test_optional_syntax_in_unwrap_optional_type(self):
        """Test that unwrap_optional_type works with Optional syntax."""
        from sql_testing_library._types import unwrap_optional_type

        optional_type = Optional[str]
        unwrapped = unwrap_optional_type(optional_type)
        self.assertEqual(unwrapped, str)

    @unittest.skipIf(not PYTHON_310_PLUS, "Python 3.10+ required for | None syntax")
    def test_pipe_none_syntax_in_unwrap_optional_type(self):
        """Test that unwrap_optional_type works with | None syntax."""
        from typing import get_type_hints

        from sql_testing_library._types import unwrap_optional_type

        # Use exec to avoid syntax error on Python 3.9
        exec(
            """
class TestModel(BaseModel):
    union_field: str | None = None
""",
            globals(),
        )

        hints = get_type_hints(globals()["TestModel"])
        unwrapped = unwrap_optional_type(hints["union_field"])
        self.assertEqual(unwrapped, str)

    def test_optional_syntax_in_is_struct_type(self):
        """Test that is_struct_type works with Optional syntax."""
        from sql_testing_library._types import is_struct_type

        class InnerModel(BaseModel):
            name: str

        class TestModel(BaseModel):
            optional_struct: Optional[InnerModel] = None

        from typing import get_type_hints

        hints = get_type_hints(TestModel)
        self.assertTrue(is_struct_type(hints["optional_struct"]))

    @unittest.skipIf(not PYTHON_310_PLUS, "Python 3.10+ required for | None syntax")
    def test_pipe_none_syntax_in_is_struct_type(self):
        """Test that is_struct_type works with | None syntax."""
        from sql_testing_library._types import is_struct_type

        class InnerModel(BaseModel):
            name: str

        # Use exec to avoid syntax error on Python 3.9
        exec(
            """
class TestModel(BaseModel):
    union_struct: InnerModel | None = None
""",
            {"BaseModel": BaseModel, "InnerModel": InnerModel},
            globals(),
        )

        from typing import get_type_hints

        hints = get_type_hints(globals()["TestModel"])
        self.assertTrue(is_struct_type(hints["union_struct"]))

    @unittest.skipIf(not PYTHON_310_PLUS, "Python 3.10+ required for | None syntax")
    def test_pipe_none_syntax_with_mock_table(self):
        """Test that | None syntax works with mock tables."""
        from typing import get_type_hints

        from sql_testing_library._mock_table import BaseMockTable

        # Use exec to avoid syntax error on Python 3.9
        exec(
            """
class TestModel(BaseModel):
    id: int
    name: str
    optional_email: str | None = None
""",
            {"BaseModel": BaseModel},
            globals(),
        )

        TestModel = globals()["TestModel"]

        class TestMockTable(BaseMockTable):
            def get_database_name(self) -> str:
                return "test_db"

            def get_table_name(self) -> str:
                return "test_table"

        # Create instances and verify they work
        data = [
            TestModel(id=1, name="Alice", optional_email="alice@example.com"),
            TestModel(id=2, name="Bob", optional_email=None),
        ]
        mock_table = TestMockTable(data)

        # Verify column types can be retrieved (unwrapped by design)
        column_types = mock_table.get_column_types()
        self.assertIn("optional_email", column_types)
        # Note: get_column_types() intentionally unwraps Optional types for SQL schema generation
        self.assertEqual(column_types["optional_email"], str)

        # Verify that the original type hints preserve the union type
        hints = get_type_hints(TestModel)
        from sql_testing_library._types import is_union_type

        self.assertTrue(is_union_type(hints["optional_email"]))


if __name__ == "__main__":
    unittest.main()
