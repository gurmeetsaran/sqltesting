"""Integration tests using Pydantic models for both input and output data across all adapters."""

import os
import unittest
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

import pytest
from pydantic import BaseModel, Field

from sql_testing_library import TestCase, sql_test
from sql_testing_library.mock_table import BaseMockTable


class UserInput(BaseModel):
    """Pydantic model for user input data."""

    user_id: int = Field(..., description="Unique user identifier")
    email: str = Field(..., description="User email address")
    first_name: str = Field(..., description="User's first name")
    last_name: str = Field(..., description="User's last name")
    age: int = Field(..., ge=0, le=150, description="User age")
    salary: Decimal = Field(..., description="User salary")
    is_active: bool = Field(default=True, description="Whether user is active")
    created_date: date = Field(..., description="Date user was created")
    last_login: Optional[datetime] = Field(default=None, description="Last login timestamp")
    department_id: Optional[int] = Field(default=None, description="Department ID")


class OrderInput(BaseModel):
    """Pydantic model for order input data."""

    order_id: int = Field(..., description="Unique order identifier")
    user_id: int = Field(..., description="User who placed the order")
    product_name: str = Field(..., description="Name of the product")
    quantity: int = Field(..., gt=0, description="Quantity ordered")
    unit_price: Decimal = Field(..., gt=0, description="Price per unit")
    order_date: date = Field(..., description="Date order was placed")
    is_shipped: bool = Field(default=False, description="Whether order is shipped")
    notes: Optional[str] = Field(default=None, description="Order notes")


class UserOrderSummary(BaseModel):
    """Pydantic model for aggregated user order data output."""

    user_id: int
    email: str
    full_name: str
    total_orders: int
    total_spent: Decimal
    average_order_value: Decimal
    first_order_date: date
    last_order_date: Optional[date]
    is_active: bool
    department_id: Optional[int]


class UserMockTable(BaseMockTable):
    """Mock table for user data."""

    def __init__(self, data: List[UserInput], database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "users"


class OrderMockTable(BaseMockTable):
    """Mock table for order data."""

    def __init__(self, data: List[OrderInput], database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "orders"


@pytest.mark.integration
class TestPydanticModelsIntegration(unittest.TestCase):
    """Integration tests using Pydantic models for both input and output."""

    def setUp(self):
        """Set up test data using Pydantic models."""
        self.user_data = [
            UserInput(
                user_id=1,
                email="john.doe@example.com",
                first_name="John",
                last_name="Doe",
                age=30,
                salary=Decimal("75000.00"),
                is_active=True,
                created_date=date(2022, 1, 15),
                last_login=datetime(2023, 12, 1, 10, 30, 0),
                department_id=101,
            ),
            UserInput(
                user_id=2,
                email="jane.smith@example.com",
                first_name="Jane",
                last_name="Smith",
                age=28,
                salary=Decimal("82000.50"),
                is_active=True,
                created_date=date(2022, 3, 22),
                last_login=datetime(2023, 12, 5, 14, 15, 30),
                department_id=102,
            ),
            UserInput(
                user_id=3,
                email="bob.wilson@example.com",
                first_name="Bob",
                last_name="Wilson",
                age=35,
                salary=Decimal("68000.75"),
                is_active=False,
                created_date=date(2021, 8, 10),
                last_login=None,
                department_id=None,
            ),
        ]

        self.order_data = [
            OrderInput(
                order_id=1001,
                user_id=1,
                product_name="Laptop",
                quantity=1,
                unit_price=Decimal("1299.99"),
                order_date=date(2023, 6, 15),
                is_shipped=True,
                notes="Express shipping requested",
            ),
            OrderInput(
                order_id=1002,
                user_id=1,
                product_name="Mouse",
                quantity=2,
                unit_price=Decimal("29.99"),
                order_date=date(2023, 7, 20),
                is_shipped=True,
                notes=None,
            ),
            OrderInput(
                order_id=1003,
                user_id=2,
                product_name="Keyboard",
                quantity=1,
                unit_price=Decimal("149.99"),
                order_date=date(2023, 8, 5),
                is_shipped=False,
                notes="Gift wrap requested",
            ),
            OrderInput(
                order_id=1004,
                user_id=2,
                product_name="Monitor",
                quantity=1,
                unit_price=Decimal("399.99"),
                order_date=date(2023, 9, 10),
                is_shipped=True,
                notes=None,
            ),
            OrderInput(
                order_id=1005,
                user_id=1,
                product_name="Webcam",
                quantity=1,
                unit_price=Decimal("89.99"),
                order_date=date(2023, 10, 1),
                is_shipped=False,
                notes="Back ordered",
            ),
        ]

    def test_bigquery_pydantic_models(self):
        """Test BigQuery adapter with Pydantic models for input and output."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                UserMockTable(self.user_data, os.getenv("GCP_PROJECT_ID", "test_project")),
                OrderMockTable(self.order_data, os.getenv("GCP_PROJECT_ID", "test_project")),
            ],
            result_class=UserOrderSummary,
        )
        def query_user_order_summary():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.email,
                        CONCAT(u.first_name, ' ', u.last_name) as full_name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.quantity * o.unit_price) as total_spent,
                        AVG(o.quantity * o.unit_price) as average_order_value,
                        MIN(o.order_date) as first_order_date,
                        MAX(o.order_date) as last_order_date,
                        u.is_active,
                        u.department_id
                    FROM users u
                    INNER JOIN orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.is_active,
                             u.department_id
                    ORDER BY total_spent DESC
                """,
                execution_database=os.getenv("GCP_PROJECT_ID", "test_project"),
            )

        results = query_user_order_summary()

        assert len(results) == 2

        # Verify John Doe's summary (highest spender)
        john_summary = results[0]
        assert john_summary.user_id == 1
        assert john_summary.email == "john.doe@example.com"
        assert john_summary.full_name == "John Doe"
        assert john_summary.total_orders == 3
        assert john_summary.total_spent == Decimal("1449.96")  # 1299.99 + 59.98 + 89.99
        assert john_summary.average_order_value == pytest.approx(
            Decimal("483.32"), abs=Decimal("0.01")
        )
        assert john_summary.first_order_date == date(2023, 6, 15)
        assert john_summary.last_order_date == date(2023, 10, 1)
        assert john_summary.is_active is True
        assert john_summary.department_id == 101

        # Verify Jane Smith's summary
        jane_summary = results[1]
        assert jane_summary.user_id == 2
        assert jane_summary.email == "jane.smith@example.com"
        assert jane_summary.full_name == "Jane Smith"
        assert jane_summary.total_orders == 2
        assert jane_summary.total_spent == Decimal("549.98")  # 149.99 + 399.99
        assert jane_summary.average_order_value == Decimal("274.99")
        assert jane_summary.first_order_date == date(2023, 8, 5)
        assert jane_summary.last_order_date == date(2023, 9, 10)
        assert jane_summary.is_active is True
        assert jane_summary.department_id == 102

    def test_athena_pydantic_models(self):
        """Test Athena adapter with Pydantic models for input and output."""

        @sql_test(
            adapter_type="athena",
            mock_tables=[
                UserMockTable(self.user_data, os.getenv("AWS_ATHENA_DATABASE", "test_db")),
                OrderMockTable(self.order_data, os.getenv("AWS_ATHENA_DATABASE", "test_db")),
            ],
            result_class=UserOrderSummary,
        )
        def query_user_order_summary():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.email,
                        CONCAT(u.first_name, ' ', u.last_name) as full_name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.quantity * o.unit_price) as total_spent,
                        AVG(o.quantity * o.unit_price) as average_order_value,
                        MIN(o.order_date) as first_order_date,
                        MAX(o.order_date) as last_order_date,
                        u.is_active,
                        u.department_id
                    FROM users u
                    INNER JOIN orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.is_active,
                             u.department_id
                    ORDER BY total_spent DESC
                """,
                execution_database=os.getenv("AWS_ATHENA_DATABASE", "test_db"),
            )

        results = query_user_order_summary()

        assert len(results) == 2
        assert isinstance(results[0], UserOrderSummary)
        assert isinstance(results[1], UserOrderSummary)

        # Verify that Pydantic validation works
        john_summary = results[0]
        assert john_summary.user_id == 1
        assert john_summary.total_orders == 3
        assert john_summary.is_active is True

    def test_redshift_pydantic_models(self):
        """Test Redshift adapter with Pydantic models for input and output."""

        @sql_test(
            adapter_type="redshift",
            mock_tables=[
                UserMockTable(self.user_data, "test_db"),
                OrderMockTable(self.order_data, "test_db"),
            ],
            result_class=UserOrderSummary,
        )
        def query_user_order_summary():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.email,
                        u.first_name || ' ' || u.last_name as full_name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.quantity * o.unit_price) as total_spent,
                        AVG(o.quantity * o.unit_price) as average_order_value,
                        MIN(o.order_date) as first_order_date,
                        MAX(o.order_date) as last_order_date,
                        u.is_active,
                        u.department_id
                    FROM users u
                    INNER JOIN orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.is_active,
                             u.department_id
                    ORDER BY total_spent DESC
                """,
                execution_database="test_db",
            )

        results = query_user_order_summary()

        assert len(results) == 2
        assert isinstance(results[0], UserOrderSummary)
        assert isinstance(results[1], UserOrderSummary)

        # Verify Pydantic model validation
        for result in results:
            assert hasattr(result, "user_id")
            assert hasattr(result, "email")
            assert hasattr(result, "full_name")
            assert hasattr(result, "total_orders")

    def test_trino_pydantic_models(self):
        """Test Trino adapter with Pydantic models for input and output."""

        @sql_test(
            adapter_type="trino",
            mock_tables=[
                UserMockTable(self.user_data, "memory"),
                OrderMockTable(self.order_data, "memory"),
            ],
            result_class=UserOrderSummary,
        )
        def query_user_order_summary():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.email,
                        CONCAT(u.first_name, ' ', u.last_name) as full_name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.quantity * o.unit_price) as total_spent,
                        AVG(o.quantity * o.unit_price) as average_order_value,
                        MIN(o.order_date) as first_order_date,
                        MAX(o.order_date) as last_order_date,
                        u.is_active,
                        u.department_id
                    FROM users u
                    INNER JOIN orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.is_active,
                             u.department_id
                    ORDER BY total_spent DESC
                """,
                execution_database="memory",
            )

        results = query_user_order_summary()

        assert len(results) == 2
        assert isinstance(results[0], UserOrderSummary)
        assert isinstance(results[1], UserOrderSummary)

    def test_snowflake_pydantic_models(self):
        """Test Snowflake adapter with Pydantic models for input and output."""

        @sql_test(
            adapter_type="snowflake",
            mock_tables=[
                UserMockTable(self.user_data, os.getenv("SNOWFLAKE_DATABASE", "test_db")),
                OrderMockTable(self.order_data, os.getenv("SNOWFLAKE_DATABASE", "test_db")),
            ],
            result_class=UserOrderSummary,
        )
        def query_user_order_summary():
            return TestCase(
                query="""
                    SELECT
                        u.user_id,
                        u.email,
                        CONCAT(u.first_name, ' ', u.last_name) as full_name,
                        COUNT(o.order_id) as total_orders,
                        SUM(o.quantity * o.unit_price) as total_spent,
                        AVG(o.quantity * o.unit_price) as average_order_value,
                        MIN(o.order_date) as first_order_date,
                        MAX(o.order_date) as last_order_date,
                        u.is_active,
                        u.department_id
                    FROM users u
                    INNER JOIN orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.is_active,
                             u.department_id
                    ORDER BY total_spent DESC
                """,
                execution_database=os.getenv("SNOWFLAKE_DATABASE", "test_db"),
            )

        results = query_user_order_summary()

        assert len(results) == 2
        assert isinstance(results[0], UserOrderSummary)
        assert isinstance(results[1], UserOrderSummary)


class TestPydanticValidation(unittest.TestCase):
    """Test Pydantic model validation features."""

    def test_pydantic_field_validation(self):
        """Test that Pydantic field validation works correctly."""

        # Test valid user creation
        valid_user = UserInput(
            user_id=1,
            email="test@example.com",
            first_name="Test",
            last_name="User",
            age=25,
            salary=Decimal("50000.00"),
            created_date=date(2023, 1, 1),
        )
        assert valid_user.user_id == 1
        assert valid_user.is_active is True  # Default value

        # Test invalid age (should raise validation error)
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            UserInput(
                user_id=2,
                email="invalid@example.com",
                first_name="Invalid",
                last_name="User",
                age=200,  # Invalid age > 150
                salary=Decimal("50000.00"),
                created_date=date(2023, 1, 1),
            )

        # Test invalid quantity in order (should raise validation error)
        with pytest.raises(ValidationError):
            OrderInput(
                order_id=1,
                user_id=1,
                product_name="Test Product",
                quantity=0,  # Invalid quantity <= 0
                unit_price=Decimal("10.00"),
                order_date=date(2023, 1, 1),
            )

    def test_pydantic_optional_fields(self):
        """Test that optional fields work correctly with Pydantic models."""

        # Test user with optional fields set to None
        user_with_nulls = UserInput(
            user_id=3,
            email="nullable@example.com",
            first_name="Nullable",
            last_name="User",
            age=30,
            salary=Decimal("60000.00"),
            created_date=date(2023, 1, 1),
            last_login=None,
            department_id=None,
        )

        assert user_with_nulls.last_login is None
        assert user_with_nulls.department_id is None
        assert user_with_nulls.is_active is True  # Default value should still work

        # Test order with optional notes
        order_no_notes = OrderInput(
            order_id=2,
            user_id=3,
            product_name="Test Product",
            quantity=1,
            unit_price=Decimal("25.00"),
            order_date=date(2023, 1, 1),
            notes=None,
        )

        assert order_no_notes.notes is None
        assert order_no_notes.is_shipped is False  # Default value


if __name__ == "__main__":
    unittest.main()
