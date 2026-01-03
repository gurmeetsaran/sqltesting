"""Integration tests for deeply nested complex types across all database adapters.

This module tests deeply nested complex types including:
- Nested lists (arrays of arrays)
- Lists of structs (arrays of ROW/STRUCT types)
- Nested lists of structs
- 3D arrays
- Maps with complex values
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List

import pytest
from pydantic import BaseModel

from sql_testing_library import TestCase, sql_test
from sql_testing_library._mock_table import BaseMockTable


# Data structures for nested complex types
@dataclass
class Address:
    """Address structure."""

    street: str
    city: str
    zipcode: str


@dataclass
class ContactInfo:
    """Contact information structure."""

    email: str
    phone: str


@dataclass
class OrderItem:
    """Order item structure."""

    item_id: int
    product_name: str
    quantity: int
    price: Decimal


@dataclass
class DeeplyNestedTypes:
    """Data class with deeply nested complex types."""

    # Basic identifier
    id: int
    name: str

    # Simple arrays
    tags: List[str]
    scores: List[int]

    # Array of structs
    addresses: List[Address]
    contacts: List[ContactInfo]

    # Nested arrays (2D)
    interaction_matrix: List[List[int]]
    category_hierarchy: List[List[str]]

    # Array of arrays of structs (3D)
    order_history: List[List[OrderItem]]

    # 3D nested array
    nested_scores: List[List[List[int]]]

    # Maps
    metadata: Dict[str, str]
    settings: Dict[str, int]


class AddressPydantic(BaseModel):
    """Pydantic version of Address for result deserialization."""

    street: str
    city: str
    zipcode: str


class ContactInfoPydantic(BaseModel):
    """Pydantic version of ContactInfo for result deserialization."""

    email: str
    phone: str


class OrderItemPydantic(BaseModel):
    """Pydantic version of OrderItem for result deserialization."""

    item_id: int
    product_name: str
    quantity: int
    price: Decimal


class DeeplyNestedTypesResult(BaseModel):
    """Result model for deeply nested types - selecting all fields."""

    id: int
    name: str
    tags: List[str]
    scores: List[int]
    addresses: List[AddressPydantic]  # Use proper struct models
    contacts: List[ContactInfoPydantic]  # Use proper struct models
    interaction_matrix: List[List[int]]
    category_hierarchy: List[List[str]]
    order_history: List[List[OrderItemPydantic]]  # Use proper struct models
    nested_scores: List[List[List[int]]]
    metadata: Dict[str, str]
    settings: Dict[str, int]


class NestedAccessResult(BaseModel):
    """Result model for accessing nested elements."""

    id: int
    name: str
    address_count: int
    first_address_street: str
    first_address_city: str
    second_contact_email: str
    first_interaction_value: int
    first_category: str
    first_order_item_count: int
    first_order_item_name: str
    nested_score_value: int
    status_metadata: str
    notification_setting: int


class DeeplyNestedTypesMockTable(BaseMockTable):
    """Mock table for deeply nested types testing."""

    def __init__(self, data, database_name: str):
        super().__init__(data)
        self._database_name = database_name

    def get_database_name(self) -> str:
        return self._database_name

    def get_table_name(self) -> str:
        return "deeply_nested_types"


@pytest.mark.integration
# TODO: Re-enable BigQuery, Snowflake once struct/nested array support is implemented
# - BigQuery: Doesn't support nested arrays - database limitation
# - Snowflake: Struct type support not implemented
@pytest.mark.parametrize(
    "adapter_type", ["athena", "trino", "duckdb", "redshift"]
)  # TODO: Add "bigquery", "snowflake" after fixing
@pytest.mark.parametrize(
    "use_physical_tables", [False, True], ids=["cte_mode", "physical_tables_mode"]
)
class TestDeeplyNestedTypesIntegration:
    """Integration tests for deeply nested types across all database adapters.

    Currently Athena, Trino, DuckDB, and Redshift support deeply nested complex types including:
    - Arrays of structs (ROW/STRUCT/SUPER types): List[Address]
    - Nested arrays (2D, 3D+): List[List[int]], List[List[List[int]]]
    - Arrays of arrays of structs: List[List[OrderItem]]
    - Maps with complex values: Dict[str, str], Dict[str, int]
    - Recursive type resolution supporting infinite nesting levels

    All 16 tests pass across the 4 adapters (both CTE and physical tables modes).
    """

    @pytest.fixture(autouse=True)
    def setup_test_data(self, adapter_type):
        """Set up test data specific to each adapter."""
        # Common test data structure for all adapters
        self.test_data = [
            DeeplyNestedTypes(
                id=1,
                name=f"{adapter_type.title()} User",
                tags=["premium", "vip", "loyal"],
                scores=[95, 87, 92],
                addresses=[
                    Address("123 Main St", "New York", "10001"),
                    Address("456 Oak Ave", "Boston", "02101"),
                ],
                contacts=[
                    ContactInfo("user1@email.com", "555-0101"),
                    ContactInfo("user1@work.com", "555-0102"),
                ],
                interaction_matrix=[
                    [10, 25, 30],
                    [15, 20, 35],
                ],
                category_hierarchy=[
                    ["Electronics", "Laptops"],
                    ["Home", "Furniture"],
                ],
                order_history=[
                    [
                        OrderItem(101, "Laptop", 1, Decimal("1299.99")),
                        OrderItem(102, "Mouse", 2, Decimal("29.99")),
                    ],
                    [
                        OrderItem(201, "Monitor", 1, Decimal("399.99")),
                    ],
                ],
                nested_scores=[
                    [[1, 2, 3], [4, 5, 6]],
                    [[7, 8, 9], [10, 11, 12]],
                ],
                metadata={
                    "status": "active",
                    "tier": "gold",
                },
                settings={
                    "notifications": 1,
                    "auto_renew": 1,
                },
            ),
            DeeplyNestedTypes(
                id=2,
                name="Test User",
                tags=["new", "trial"],
                scores=[70, 75],
                addresses=[
                    Address("789 Elm St", "Seattle", "98101"),
                ],
                contacts=[
                    ContactInfo("user2@email.com", "555-0201"),
                ],
                interaction_matrix=[
                    [5, 10],
                ],
                category_hierarchy=[
                    ["Books"],
                ],
                order_history=[
                    [
                        OrderItem(301, "Book", 1, Decimal("19.99")),
                    ],
                ],
                nested_scores=[
                    [[20, 21], [22, 23]],
                ],
                metadata={
                    "status": "trial",
                    "tier": "silver",
                },
                settings={
                    "notifications": 1,
                    "auto_renew": 0,
                },
            ),
        ]

        # Set database name based on adapter type
        if adapter_type == "athena":
            self.database_name = "test_db"
        elif adapter_type == "bigquery":
            self.database_name = "test-project.test_dataset"
        elif adapter_type == "redshift":
            self.database_name = "test_db"
        elif adapter_type == "snowflake":
            self.database_name = "test_db.sqltesting"
        elif adapter_type == "trino":
            self.database_name = "memory"
        elif adapter_type == "duckdb":
            self.database_name = ""

    def test_select_all_deeply_nested_fields(self, adapter_type, use_physical_tables):
        """Test selecting all deeply nested fields for the specified adapter."""

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[DeeplyNestedTypesMockTable(self.test_data, self.database_name)],
            result_class=DeeplyNestedTypesResult,
        )
        def query_all_nested_fields():
            return TestCase(
                query="""
                    SELECT
                        id,
                        name,
                        tags,
                        scores,
                        addresses,
                        contacts,
                        interaction_matrix,
                        category_hierarchy,
                        order_history,
                        nested_scores,
                        metadata,
                        settings
                    FROM deeply_nested_types
                    WHERE id = 1
                """,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_all_nested_fields()
        assert len(results) == 1

        # Verify results based on adapter type
        if adapter_type == "athena":
            self._verify_athena_all_fields(results)
        elif adapter_type == "bigquery":
            self._verify_bigquery_all_fields(results)
        elif adapter_type == "redshift":
            self._verify_redshift_all_fields(results)
        elif adapter_type == "snowflake":
            self._verify_snowflake_all_fields(results)
        elif adapter_type == "trino":
            self._verify_trino_all_fields(results)
        elif adapter_type == "duckdb":
            self._verify_duckdb_all_fields(results)

    def test_access_nested_elements(self, adapter_type, use_physical_tables):
        """Test accessing specific elements from deeply nested structures."""

        # Build query based on adapter capabilities
        if adapter_type == "trino":
            query = """
                SELECT
                    id,
                    name,
                    CARDINALITY(addresses) as address_count,
                    addresses[1].street as first_address_street,
                    addresses[1].city as first_address_city,
                    contacts[2].email as second_contact_email,
                    interaction_matrix[1][1] as first_interaction_value,
                    category_hierarchy[1][1] as first_category,
                    CARDINALITY(order_history[1]) as first_order_item_count,
                    order_history[1][1].product_name as first_order_item_name,
                    nested_scores[1][1][1] as nested_score_value,
                    metadata['status'] as status_metadata,
                    settings['notifications'] as notification_setting
                FROM deeply_nested_types
                WHERE id = 1
            """
        elif adapter_type == "snowflake":
            # Snowflake uses 0-based indexing and different syntax
            query = """
                SELECT
                    id,
                    name,
                    ARRAY_SIZE(addresses) as address_count,
                    addresses[0]:street::string as first_address_street,
                    addresses[0]:city::string as first_address_city,
                    contacts[1]:email::string as second_contact_email,
                    interaction_matrix[0][0]::int as first_interaction_value,
                    category_hierarchy[0][0]::string as first_category,
                    ARRAY_SIZE(order_history[0]) as first_order_item_count,
                    order_history[0][0]:product_name::string as first_order_item_name,
                    nested_scores[0][0][0]::int as nested_score_value,
                    metadata['status']::string as status_metadata,
                    settings['notifications']::int as notification_setting
                FROM deeply_nested_types
                WHERE id = 1
            """
        elif adapter_type == "bigquery":
            # BigQuery uses different array access syntax
            query = """
                SELECT
                    id,
                    name,
                    ARRAY_LENGTH(addresses) as address_count,
                    addresses[OFFSET(0)].street as first_address_street,
                    addresses[OFFSET(0)].city as first_address_city,
                    contacts[OFFSET(1)].email as second_contact_email,
                    interaction_matrix[OFFSET(0)][OFFSET(0)] as first_interaction_value,
                    category_hierarchy[OFFSET(0)][OFFSET(0)] as first_category,
                    ARRAY_LENGTH(order_history[OFFSET(0)]) as first_order_item_count,
                    order_history[OFFSET(0)][OFFSET(0)].product_name as first_order_item_name,
                    nested_scores[OFFSET(0)][OFFSET(0)][OFFSET(0)] as nested_score_value,
                    metadata['status'] as status_metadata,
                    settings['notifications'] as notification_setting
                FROM deeply_nested_types
                WHERE id = 1
            """
        elif adapter_type == "athena":
            # Athena/Presto syntax similar to Trino
            query = """
                SELECT
                    id,
                    name,
                    CARDINALITY(addresses) as address_count,
                    addresses[1].street as first_address_street,
                    addresses[1].city as first_address_city,
                    contacts[2].email as second_contact_email,
                    interaction_matrix[1][1] as first_interaction_value,
                    category_hierarchy[1][1] as first_category,
                    CARDINALITY(order_history[1]) as first_order_item_count,
                    order_history[1][1].product_name as first_order_item_name,
                    nested_scores[1][1][1] as nested_score_value,
                    element_at(metadata, 'status') as status_metadata,
                    element_at(settings, 'notifications') as notification_setting
                FROM deeply_nested_types
                WHERE id = 1
            """
        elif adapter_type == "duckdb":
            # DuckDB uses 1-based indexing and len() function
            query = """
                SELECT
                    id,
                    name,
                    len(addresses) as address_count,
                    addresses[1].street as first_address_street,
                    addresses[1].city as first_address_city,
                    contacts[2].email as second_contact_email,
                    interaction_matrix[1][1] as first_interaction_value,
                    category_hierarchy[1][1] as first_category,
                    len(order_history[1]) as first_order_item_count,
                    (order_history[1][1]).product_name as first_order_item_name,
                    nested_scores[1][1][1] as nested_score_value,
                    metadata['status'] as status_metadata,
                    settings['notifications'] as notification_setting
                FROM deeply_nested_types
                WHERE id = 1
            """
        elif adapter_type == "redshift":
            # Redshift uses SUPER type with 0-based indexing and dot/bracket notation
            # Use GET_ARRAY_LENGTH for SUPER type arrays
            query = """
                SELECT
                    id,
                    name,
                    GET_ARRAY_LENGTH(addresses) as address_count,
                    addresses[0].street::varchar as first_address_street,
                    addresses[0].city::varchar as first_address_city,
                    contacts[1].email::varchar as second_contact_email,
                    interaction_matrix[0][0]::int as first_interaction_value,
                    category_hierarchy[0][0]::varchar as first_category,
                    GET_ARRAY_LENGTH(order_history[0]) as first_order_item_count,
                    order_history[0][0].product_name::varchar as first_order_item_name,
                    nested_scores[0][0][0]::int as nested_score_value,
                    metadata.status::varchar as status_metadata,
                    settings.notifications::int as notification_setting
                FROM deeply_nested_types
                WHERE id = 1
            """
        else:
            pytest.skip(f"Nested element access not implemented for {adapter_type}")
            return

        @sql_test(
            adapter_type=adapter_type,
            mock_tables=[DeeplyNestedTypesMockTable(self.test_data, self.database_name)],
            result_class=NestedAccessResult,
        )
        def query_nested_access():
            return TestCase(
                query=query,
                default_namespace=self.database_name,
                use_physical_tables=use_physical_tables,
            )

        results = query_nested_access()
        assert len(results) == 1

        result = results[0]
        assert result.id == 1
        assert result.address_count == 2
        assert result.first_address_street == "123 Main St"
        assert result.first_address_city == "New York"
        assert result.second_contact_email == "user1@work.com"
        assert result.first_interaction_value == 10

        if adapter_type in ["trino", "athena"]:
            assert result.first_category == "Electronics"
        elif adapter_type == "bigquery":
            assert result.first_category == "Electronics"
        elif adapter_type == "snowflake":
            assert result.first_category == "Electronics"

        assert result.first_order_item_count == 2
        assert result.first_order_item_name == "Laptop"
        assert result.nested_score_value == 1
        assert result.status_metadata == "active"
        assert result.notification_setting == 1

    # Verification methods for each adapter
    def _verify_athena_all_fields(self, results):
        """Verify Athena-specific results for all fields."""
        result = results[0]
        assert result.id == 1
        assert result.name == "Athena User"
        assert result.tags == ["premium", "vip", "loyal"]
        assert result.scores == [95, 87, 92]

        # Verify addresses (list of structs)
        assert len(result.addresses) == 2
        assert result.addresses[0].street == "123 Main St"
        assert result.addresses[0].city == "New York"
        assert result.addresses[0].zipcode == "10001"
        assert result.addresses[1].street == "456 Oak Ave"

        # Verify contacts (list of structs)
        assert len(result.contacts) == 2
        assert result.contacts[0].email == "user1@email.com"
        assert result.contacts[0].phone == "555-0101"
        assert result.contacts[1].email == "user1@work.com"

        # Verify nested arrays
        assert len(result.interaction_matrix) == 2
        assert result.interaction_matrix[0] == [10, 25, 30]
        assert result.interaction_matrix[1] == [15, 20, 35]

        assert len(result.category_hierarchy) == 2
        assert result.category_hierarchy[0] == ["Electronics", "Laptops"]

        # Verify arrays of arrays of structs (order_history)
        assert len(result.order_history) == 2
        assert len(result.order_history[0]) == 2  # First order has 2 items
        assert result.order_history[0][0].product_name == "Laptop"
        assert result.order_history[0][0].price == Decimal("1299.99")
        assert result.order_history[0][0].quantity == 1
        assert result.order_history[0][1].product_name == "Mouse"

        # Verify 3D nested array
        assert len(result.nested_scores) == 2
        assert result.nested_scores[0] == [[1, 2, 3], [4, 5, 6]]
        assert result.nested_scores[1] == [[7, 8, 9], [10, 11, 12]]

        # Verify maps
        assert result.metadata["status"] == "active"
        assert result.metadata["tier"] == "gold"
        assert result.settings["notifications"] == 1
        assert result.settings["auto_renew"] == 1

    def _verify_bigquery_all_fields(self, results):
        """Verify BigQuery-specific results for all fields."""
        result = results[0]
        assert result.id == 1
        assert result.name == "Bigquery User"
        assert result.tags == ["premium", "vip", "loyal"]
        assert result.scores == [95, 87, 92]

        # Verify nested arrays
        assert len(result.interaction_matrix) == 2
        assert result.interaction_matrix[0] == [10, 25, 30]

        # Verify 3D nested array
        assert len(result.nested_scores) == 2
        assert result.nested_scores[0] == [[1, 2, 3], [4, 5, 6]]

        # Verify maps (BigQuery represents maps differently)
        assert result.metadata["status"] == "active"
        assert result.settings["notifications"] == 1

    def _verify_redshift_all_fields(self, results):
        """Verify Redshift-specific results for all fields."""
        result = results[0]
        assert result.id == 1
        assert result.name == "Redshift User"
        assert result.tags == ["premium", "vip", "loyal"]
        assert result.scores == [95, 87, 92]

        # Verify addresses (list of structs via SUPER/JSON)
        assert len(result.addresses) == 2
        assert result.addresses[0].street == "123 Main St"
        assert result.addresses[0].city == "New York"
        assert result.addresses[0].zipcode == "10001"
        assert result.addresses[1].street == "456 Oak Ave"

        # Verify contacts (list of structs via SUPER/JSON)
        assert len(result.contacts) == 2
        assert result.contacts[0].email == "user1@email.com"
        assert result.contacts[0].phone == "555-0101"
        assert result.contacts[1].email == "user1@work.com"

        # Verify nested arrays
        assert len(result.interaction_matrix) == 2
        assert result.interaction_matrix[0] == [10, 25, 30]
        assert result.interaction_matrix[1] == [15, 20, 35]

        assert len(result.category_hierarchy) == 2
        assert result.category_hierarchy[0] == ["Electronics", "Laptops"]

        # Verify arrays of arrays of structs (order_history via SUPER/JSON)
        assert len(result.order_history) == 2
        assert len(result.order_history[0]) == 2
        assert result.order_history[0][0].product_name == "Laptop"
        assert result.order_history[0][0].price == Decimal("1299.99")
        assert result.order_history[0][1].product_name == "Mouse"

        # Verify 3D nested array
        assert len(result.nested_scores) == 2
        assert result.nested_scores[0] == [[1, 2, 3], [4, 5, 6]]
        assert result.nested_scores[1] == [[7, 8, 9], [10, 11, 12]]

        # Verify maps
        assert result.metadata["status"] == "active"
        assert result.metadata["tier"] == "gold"
        assert result.settings["notifications"] == 1
        assert result.settings["auto_renew"] == 1

    def _verify_snowflake_all_fields(self, results):
        """Verify Snowflake-specific results for all fields."""
        result = results[0]
        assert result.id == 1
        assert result.name == "Snowflake User"
        assert result.tags == ["premium", "vip", "loyal"]
        assert result.scores == [95, 87, 92]

        # Verify nested arrays
        assert len(result.interaction_matrix) == 2
        assert result.interaction_matrix[0] == [10, 25, 30]

        # Verify 3D nested array
        assert len(result.nested_scores) == 2
        assert result.nested_scores[0] == [[1, 2, 3], [4, 5, 6]]

        # Verify maps
        assert result.metadata["status"] == "active"
        assert result.settings["notifications"] == 1

    def _verify_trino_all_fields(self, results):
        """Verify Trino-specific results for all fields."""
        result = results[0]
        assert result.id == 1
        assert result.name == "Trino User"
        assert result.tags == ["premium", "vip", "loyal"]
        assert result.scores == [95, 87, 92]

        # Verify addresses (list of structs)
        assert len(result.addresses) == 2
        assert result.addresses[0].street == "123 Main St"
        assert result.addresses[0].city == "New York"
        assert result.addresses[0].zipcode == "10001"
        assert result.addresses[1].street == "456 Oak Ave"
        assert result.addresses[1].city == "Boston"

        # Verify contacts (list of structs)
        assert len(result.contacts) == 2
        assert result.contacts[0].email == "user1@email.com"
        assert result.contacts[0].phone == "555-0101"
        assert result.contacts[1].email == "user1@work.com"
        assert result.contacts[1].phone == "555-0102"

        # Verify nested arrays
        assert len(result.interaction_matrix) == 2
        assert result.interaction_matrix[0] == [10, 25, 30]
        assert result.interaction_matrix[1] == [15, 20, 35]

        assert len(result.category_hierarchy) == 2
        assert result.category_hierarchy[0] == ["Electronics", "Laptops"]
        assert result.category_hierarchy[1] == ["Home", "Furniture"]

        # Verify arrays of arrays of structs (order_history)
        assert len(result.order_history) == 2
        assert len(result.order_history[0]) == 2  # First order has 2 items
        assert result.order_history[0][0].product_name == "Laptop"
        assert result.order_history[0][0].price == Decimal("1299.99")
        assert result.order_history[0][0].quantity == 1
        assert result.order_history[0][0].item_id == 101
        assert result.order_history[0][1].product_name == "Mouse"
        assert result.order_history[0][1].quantity == 2
        assert len(result.order_history[1]) == 1  # Second order has 1 item
        assert result.order_history[1][0].product_name == "Monitor"

        # Verify 3D nested array
        assert len(result.nested_scores) == 2
        assert result.nested_scores[0] == [[1, 2, 3], [4, 5, 6]]
        assert result.nested_scores[1] == [[7, 8, 9], [10, 11, 12]]

        # Verify maps
        assert result.metadata["status"] == "active"
        assert result.metadata["tier"] == "gold"
        assert result.settings["notifications"] == 1
        assert result.settings["auto_renew"] == 1

    def _verify_duckdb_all_fields(self, results):
        """Verify DuckDB-specific results for all fields."""
        result = results[0]
        assert result.id == 1
        assert result.name == "Duckdb User"
        assert result.tags == ["premium", "vip", "loyal"]
        assert result.scores == [95, 87, 92]

        # Verify addresses (list of structs)
        assert len(result.addresses) == 2
        assert result.addresses[0].street == "123 Main St"
        assert result.addresses[0].city == "New York"
        assert result.addresses[0].zipcode == "10001"
        assert result.addresses[1].street == "456 Oak Ave"
        assert result.addresses[1].city == "Boston"

        # Verify contacts (list of structs)
        assert len(result.contacts) == 2
        assert result.contacts[0].email == "user1@email.com"
        assert result.contacts[0].phone == "555-0101"
        assert result.contacts[1].email == "user1@work.com"
        assert result.contacts[1].phone == "555-0102"

        # Verify nested arrays
        assert len(result.interaction_matrix) == 2
        assert result.interaction_matrix[0] == [10, 25, 30]
        assert result.interaction_matrix[1] == [15, 20, 35]

        assert len(result.category_hierarchy) == 2
        assert result.category_hierarchy[0] == ["Electronics", "Laptops"]
        assert result.category_hierarchy[1] == ["Home", "Furniture"]

        # Verify arrays of arrays of structs (order_history)
        assert len(result.order_history) == 2
        assert len(result.order_history[0]) == 2  # First order has 2 items
        assert result.order_history[0][0].product_name == "Laptop"
        assert result.order_history[0][0].price == Decimal("1299.99")
        assert result.order_history[0][0].quantity == 1
        assert result.order_history[0][0].item_id == 101
        assert result.order_history[0][1].product_name == "Mouse"
        assert result.order_history[0][1].quantity == 2
        assert len(result.order_history[1]) == 1  # Second order has 1 item
        assert result.order_history[1][0].product_name == "Monitor"

        # Verify 3D nested array
        assert len(result.nested_scores) == 2
        assert result.nested_scores[0] == [[1, 2, 3], [4, 5, 6]]
        assert result.nested_scores[1] == [[7, 8, 9], [10, 11, 12]]

        # Verify maps
        assert result.metadata["status"] == "active"
        assert result.metadata["tier"] == "gold"
        assert result.settings["notifications"] == 1
        assert result.settings["auto_renew"] == 1


if __name__ == "__main__":
    pytest.main([__file__])
