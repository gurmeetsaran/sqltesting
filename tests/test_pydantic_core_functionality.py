"""Unit tests for core SQL testing framework functionality using Pydantic models."""

import unittest
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from unittest.mock import MagicMock

from pydantic import BaseModel, Field

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


class Product(BaseModel):
    """Pydantic model for product data."""

    id: int = Field(..., description="Product ID")
    name: str = Field(..., min_length=1, description="Product name")
    price: Decimal = Field(..., gt=0, description="Product price")
    category: str = Field(..., description="Product category")
    in_stock: bool = Field(default=True, description="Whether product is in stock")
    created_at: date = Field(..., description="Date product was created")
    last_updated: Optional[datetime] = Field(default=None, description="Last update timestamp")


class Sale(BaseModel):
    """Pydantic model for sales data."""

    sale_id: int = Field(..., description="Sale ID")
    product_id: int = Field(..., description="Product ID")
    quantity: int = Field(..., gt=0, description="Quantity sold")
    sale_date: date = Field(..., description="Date of sale")
    customer_name: str = Field(..., description="Customer name")
    discount_percent: Optional[Decimal] = Field(
        default=None, ge=0, le=100, description="Discount percentage"
    )


class ProductSalesSummary(BaseModel):
    """Pydantic model for aggregated product sales data."""

    product_id: int
    product_name: str
    category: str
    total_quantity_sold: int
    total_revenue: Decimal
    average_sale_quantity: Decimal
    first_sale_date: date
    last_sale_date: date
    unique_customers: int


class ProductMockTable(BaseMockTable):
    """Mock table for product data using Pydantic models."""

    def __init__(self, data: List[Product], database_name: str = "test_db"):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "products"


class SaleMockTable(BaseMockTable):
    """Mock table for sales data using Pydantic models."""

    def __init__(self, data: List[Sale], database_name: str = "test_db"):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "sales"


class TestPydanticCoreFramework(unittest.TestCase):
    """Test core framework functionality with Pydantic models."""

    def setUp(self):
        """Set up test data using Pydantic models."""
        self.products = [
            Product(
                id=1,
                name="Wireless Headphones",
                price=Decimal("199.99"),
                category="Electronics",
                in_stock=True,
                created_at=date(2023, 1, 15),
                last_updated=datetime(2023, 6, 10, 14, 30, 0),
            ),
            Product(
                id=2,
                name="Coffee Mug",
                price=Decimal("12.50"),
                category="Kitchen",
                in_stock=True,
                created_at=date(2023, 2, 20),
                last_updated=None,
            ),
            Product(
                id=3,
                name="Running Shoes",
                price=Decimal("89.99"),
                category="Sports",
                in_stock=False,
                created_at=date(2023, 3, 5),
                last_updated=datetime(2023, 8, 15, 9, 45, 30),
            ),
        ]

        self.sales = [
            Sale(
                sale_id=1,
                product_id=1,
                quantity=2,
                sale_date=date(2023, 6, 15),
                customer_name="Alice Johnson",
                discount_percent=Decimal("10.0"),
            ),
            Sale(
                sale_id=2,
                product_id=1,
                quantity=1,
                sale_date=date(2023, 7, 20),
                customer_name="Bob Smith",
                discount_percent=None,
            ),
            Sale(
                sale_id=3,
                product_id=2,
                quantity=5,
                sale_date=date(2023, 8, 5),
                customer_name="Carol Davis",
                discount_percent=Decimal("5.0"),
            ),
            Sale(
                sale_id=4,
                product_id=2,
                quantity=3,
                sale_date=date(2023, 8, 10),
                customer_name="David Wilson",
                discount_percent=None,
            ),
            Sale(
                sale_id=5,
                product_id=1,
                quantity=1,
                sale_date=date(2023, 9, 1),
                customer_name="Alice Johnson",
                discount_percent=Decimal("15.0"),
            ),
        ]

    def test_pydantic_mock_table_creation(self):
        """Test that mock tables can be created with Pydantic model data."""
        product_table = ProductMockTable(self.products, "test_database")
        sale_table = SaleMockTable(self.sales, "test_database")

        # Verify table properties
        assert product_table.get_database_name() == "test_database"
        assert product_table.get_table_name() == "products"
        assert sale_table.get_database_name() == "test_database"
        assert sale_table.get_table_name() == "sales"

        # Verify data is stored correctly
        assert len(product_table.data) == 3
        assert len(sale_table.data) == 5

        # Verify data is normalized to dictionaries (not original model instances)
        assert isinstance(product_table.data[0], dict)
        assert isinstance(sale_table.data[0], dict)

        # Test specific field access
        first_product = product_table.data[0]
        assert first_product["id"] == 1
        assert first_product["name"] == "Wireless Headphones"
        assert first_product["price"] == Decimal("199.99")
        assert first_product["in_stock"] is True

        first_sale = sale_table.data[0]
        assert first_sale["sale_id"] == 1
        assert first_sale["product_id"] == 1
        assert first_sale["quantity"] == 2
        assert first_sale["discount_percent"] == Decimal("10.0")

    def test_sql_test_decorator_with_pydantic_models(self):
        """Test that the sql_test decorator works with Pydantic models."""

        @sql_test(
            adapter_type="bigquery",
            mock_tables=[
                ProductMockTable(self.products, "test_project"),
                SaleMockTable(self.sales, "test_project"),
            ],
            result_class=ProductSalesSummary,
        )
        def query_product_sales_summary():
            return TestCase(
                query="""
                    SELECT
                        p.id as product_id,
                        p.name as product_name,
                        p.category,
                        SUM(s.quantity) as total_quantity_sold,
                        SUM(s.quantity * p.price) as total_revenue,
                        AVG(s.quantity) as average_sale_quantity,
                        MIN(s.sale_date) as first_sale_date,
                        MAX(s.sale_date) as last_sale_date,
                        COUNT(DISTINCT s.customer_name) as unique_customers
                    FROM products p
                    INNER JOIN sales s ON p.id = s.product_id
                    GROUP BY p.id, p.name, p.category
                    ORDER BY total_revenue DESC
                """,
                default_namespace="test_project",
            )

        # Mock the adapter to avoid actual database connection
        mock_adapter = MagicMock()
        mock_adapter.get_sqlglot_dialect.return_value = "bigquery"

        # Test that the decorator creates the proper function structure
        test_function = query_product_sales_summary
        assert callable(test_function)

        # Verify the function has the expected attributes set by the decorator
        assert hasattr(test_function, "__wrapped__")

    def test_pydantic_model_validation_during_creation(self):
        """Test that Pydantic model validation occurs during data creation."""

        # Test valid product creation
        valid_product = Product(
            id=100,
            name="Valid Product",
            price=Decimal("50.00"),
            category="Test",
            created_at=date(2023, 1, 1),
        )
        assert valid_product.id == 100
        assert valid_product.in_stock is True  # Default value

        # Test that validation errors are raised for invalid data
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            Product(
                id=101,
                name="",  # Invalid: empty name
                price=Decimal("50.00"),
                category="Test",
                created_at=date(2023, 1, 1),
            )

        with self.assertRaises(ValidationError):
            Product(
                id=102,
                name="Invalid Price Product",
                price=Decimal("-10.00"),  # Invalid: negative price
                category="Test",
                created_at=date(2023, 1, 1),
            )

        # Test valid sale creation
        valid_sale = Sale(
            sale_id=200,
            product_id=1,
            quantity=3,
            sale_date=date(2023, 1, 1),
            customer_name="Test Customer",
        )
        assert valid_sale.sale_id == 200
        assert valid_sale.discount_percent is None  # Default value

        # Test invalid sale (zero quantity)
        with self.assertRaises(ValidationError):
            Sale(
                sale_id=201,
                product_id=1,
                quantity=0,  # Invalid: quantity must be > 0
                sale_date=date(2023, 1, 1),
                customer_name="Test Customer",
            )

        # Test invalid discount percentage
        with self.assertRaises(ValidationError):
            Sale(
                sale_id=202,
                product_id=1,
                quantity=1,
                sale_date=date(2023, 1, 1),
                customer_name="Test Customer",
                discount_percent=Decimal("150.0"),  # Invalid: > 100%
            )

    def test_pydantic_model_serialization(self):
        """Test that Pydantic models can be properly serialized for SQL generation."""

        product = self.products[0]
        sale = self.sales[0]

        # Test model_dump (Pydantic v2) functionality
        product_dict = product.model_dump()
        sale_dict = sale.model_dump()

        # Verify all fields are present
        expected_product_fields = {
            "id",
            "name",
            "price",
            "category",
            "in_stock",
            "created_at",
            "last_updated",
        }
        expected_sale_fields = {
            "sale_id",
            "product_id",
            "quantity",
            "sale_date",
            "customer_name",
            "discount_percent",
        }

        assert set(product_dict.keys()) == expected_product_fields
        assert set(sale_dict.keys()) == expected_sale_fields

        # Verify values are correctly serialized
        assert product_dict["id"] == 1
        assert product_dict["name"] == "Wireless Headphones"
        assert product_dict["price"] == Decimal("199.99")
        assert product_dict["in_stock"] is True

        assert sale_dict["sale_id"] == 1
        assert sale_dict["product_id"] == 1
        assert sale_dict["quantity"] == 2
        assert sale_dict["discount_percent"] == Decimal("10.0")

    def test_mixed_pydantic_and_dataclass_compatibility(self):
        """Test that Pydantic models work alongside existing dataclass-based tests."""
        from dataclasses import dataclass

        @dataclass
        class SimpleProduct:
            id: int
            name: str
            price: float

        # Create mixed data types
        pydantic_product = Product(
            id=1,
            name="Pydantic Product",
            price=Decimal("100.00"),
            category="Electronics",
            created_at=date(2023, 1, 1),
        )

        dataclass_product = SimpleProduct(id=2, name="Dataclass Product", price=50.0)

        # Both should be valid for mock table creation
        # (though in practice, a single table should use consistent model types)
        pydantic_table = ProductMockTable([pydantic_product])
        assert len(pydantic_table.data) == 1
        assert isinstance(pydantic_table.data[0], dict)  # Data is normalized to dict

        # This demonstrates that the framework can handle both approaches
        assert pydantic_product.id == 1
        assert dataclass_product.id == 2

    def test_complex_pydantic_field_types(self):
        """Test Pydantic models with complex field types and validation."""

        class ComplexProduct(BaseModel):
            id: int = Field(..., gt=0, description="Product ID must be positive")
            name: str = Field(..., min_length=2, max_length=100, description="Product name")
            price: Decimal = Field(
                ..., gt=0, decimal_places=2, description="Price with 2 decimal places"
            )
            tags: List[str] = Field(default_factory=list, description="Product tags")
            metadata: Optional[dict] = Field(default=None, description="Additional metadata")

        # Test valid complex product
        complex_product = ComplexProduct(
            id=1,
            name="Complex Product",
            price=Decimal("99.99"),
            tags=["electronics", "wireless", "premium"],
            metadata={"brand": "TechCorp", "warranty_years": 2},
        )

        assert complex_product.id == 1
        assert len(complex_product.tags) == 3
        assert complex_product.metadata["brand"] == "TechCorp"

        # Test validation failures
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):  # Invalid ID (not positive)
            ComplexProduct(id=0, name="Invalid Product", price=Decimal("99.99"))

        with self.assertRaises(ValidationError):  # Invalid name (too short)
            ComplexProduct(id=1, name="A", price=Decimal("99.99"))


class TestPydanticResultProcessing(unittest.TestCase):
    """Test result processing with Pydantic output models."""

    def test_result_class_instantiation(self):
        """Test that result classes are properly instantiated from query results."""

        # Simulate raw query result data
        raw_result_data = {
            "product_id": 1,
            "product_name": "Test Product",
            "category": "Electronics",
            "total_quantity_sold": 10,
            "total_revenue": Decimal("999.90"),
            "average_sale_quantity": Decimal("3.33"),
            "first_sale_date": date(2023, 1, 1),
            "last_sale_date": date(2023, 12, 31),
            "unique_customers": 5,
        }

        # Test that ProductSalesSummary can be created from raw data
        summary = ProductSalesSummary(**raw_result_data)

        assert summary.product_id == 1
        assert summary.product_name == "Test Product"
        assert summary.category == "Electronics"
        assert summary.total_quantity_sold == 10
        assert summary.total_revenue == Decimal("999.90")
        assert summary.unique_customers == 5

        # Test that the model provides proper type conversion and validation
        assert isinstance(summary.product_id, int)
        assert isinstance(summary.total_revenue, Decimal)
        assert isinstance(summary.first_sale_date, date)

    def test_optional_fields_in_results(self):
        """Test handling of optional fields in result models."""

        class OptionalFieldResult(BaseModel):
            required_field: str
            optional_field: Optional[str] = None
            optional_with_default: Optional[int] = 42

        # Test with all fields provided
        result_full = OptionalFieldResult(
            required_field="test", optional_field="provided", optional_with_default=100
        )
        assert result_full.optional_field == "provided"
        assert result_full.optional_with_default == 100

        # Test with minimal fields (using defaults)
        result_minimal = OptionalFieldResult(required_field="test")
        assert result_minimal.optional_field is None
        assert result_minimal.optional_with_default == 42

        # Test with null values
        result_with_nulls = OptionalFieldResult(
            required_field="test", optional_field=None, optional_with_default=None
        )
        assert result_with_nulls.optional_field is None
        assert result_with_nulls.optional_with_default is None


if __name__ == "__main__":
    unittest.main()
