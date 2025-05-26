"""Core SQL testing framework."""

from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    get_type_hints,
)

import pandas as pd
import sqlglot
from sqlglot import exp

from .adapters.base import DatabaseAdapter
from .exceptions import (
    MockTableNotFoundError,
    QuerySizeLimitExceeded,
    SQLParseError,
    TypeConversionError,
)
from .mock_table import BaseMockTable


# Type for adapter types
AdapterType = Literal["bigquery", "athena", "redshift", "trino", "snowflake"]

T = TypeVar("T")


@dataclass
class SQLTestCase(Generic[T]):
    """Represents a SQL test case."""

    __test__ = False  # Tell pytest this is not a test class

    query: str
    execution_database: str
    mock_tables: Optional[List[BaseMockTable]] = None
    result_class: Optional[Type[T]] = None
    use_physical_tables: bool = False
    description: Optional[str] = None
    adapter_type: Optional[AdapterType] = None


class SQLTestFramework:
    """Main framework for executing SQL tests."""

    def __init__(self, adapter: DatabaseAdapter) -> None:
        self.adapter = adapter
        self.type_converter = self.adapter.get_type_converter()
        self.temp_tables: List[str] = []

    def run_test(self, test_case: SQLTestCase[T]) -> List[T]:
        """
        Execute a test case and return deserialized results.

        Args:
            test_case: The test case to execute

        Returns:
            List of result objects of type test_case.result_class
        """
        try:
            # Validate required fields
            if test_case.mock_tables is None:
                raise ValueError(
                    "mock_tables must be provided either in SQLTestCase or sql_test decorator"
                )

            if test_case.result_class is None:
                raise ValueError(
                    "result_class must be provided either in SQLTestCase or sql_test decorator"
                )

            # Parse SQL to find table references
            referenced_tables = self._parse_sql_tables(test_case.query)

            # Resolve unqualified table names
            resolved_tables = self._resolve_table_names(
                referenced_tables, test_case.execution_database
            )

            # Validate all required mock tables are provided
            self._validate_mock_tables(resolved_tables, test_case.mock_tables)

            # Create table name mapping
            table_mapping = self._create_table_mapping(resolved_tables, test_case.mock_tables)

            if test_case.use_physical_tables:
                # Create physical temporary tables
                final_query = self._execute_with_physical_tables(
                    test_case.query, table_mapping, test_case.mock_tables
                )
            else:
                # Generate query with CTEs
                final_query = self._generate_cte_query(
                    test_case.query, table_mapping, test_case.mock_tables
                )

                # Check size limit for adapters that need it
                size_limit = self.adapter.get_query_size_limit()
                if size_limit and len(final_query.encode("utf-8")) > size_limit:
                    raise QuerySizeLimitExceeded(
                        len(final_query.encode("utf-8")),
                        size_limit,
                        self.adapter.__class__.__name__,
                    )

            # Execute query
            result_df = self.adapter.execute_query(final_query)

            # Convert results to typed objects
            return self._deserialize_results(result_df, test_case.result_class)

        finally:
            # Cleanup any temporary tables
            if self.temp_tables:
                self.adapter.cleanup_temp_tables(self.temp_tables)
                self.temp_tables = []

    def _parse_sql_tables(self, query: str) -> List[str]:
        """Parse SQL query to extract table references."""
        try:
            dialect = self.adapter.get_sqlglot_dialect()
            parsed = sqlglot.parse_one(query, dialect=dialect)

            # Get all CTE (WITH clause) aliases to filter them out
            cte_aliases = set()
            for cte in parsed.find_all(exp.CTE):
                if hasattr(cte, "alias"):
                    cte_aliases.add(str(cte.alias))

            # Find all real tables (excluding the CTEs)
            tables = []
            for table in parsed.find_all(exp.Table):
                # Skip tables that are actually CTE references
                if str(table.name) in cte_aliases:
                    continue

                # Get the fully qualified name including catalog/schema if present
                if table.db and table.catalog:
                    qualified_name = f"{table.catalog}.{table.db}.{table.name}"
                elif table.db:
                    qualified_name = f"{table.db}.{table.name}"
                else:
                    qualified_name = str(table.name)

                tables.append(qualified_name)

            return list(set(tables))  # Remove duplicates

        except Exception as e:
            raise SQLParseError(query, str(e))  # noqa:  B904

    def _resolve_table_names(
        self, referenced_tables: List[str], execution_database: str
    ) -> Dict[str, str]:
        """
        Resolve unqualified table names using execution database context.

        Returns:
            Dict mapping original table name to fully qualified name
        """
        resolved = {}
        for table_name in referenced_tables:
            if "." in table_name:
                # Already qualified
                resolved[table_name] = table_name
            else:
                # Add database prefix
                qualified_name = f"{execution_database}.{table_name}"
                resolved[table_name] = qualified_name

        return resolved

    def _validate_mock_tables(
        self, resolved_tables: Dict[str, str], mock_tables: List[BaseMockTable]
    ) -> None:
        """Validate that all required mock tables are provided."""
        provided_tables = {mock.get_qualified_name() for mock in mock_tables}
        required_tables = set(resolved_tables.values())

        missing_tables = required_tables - provided_tables

        if missing_tables:
            raise MockTableNotFoundError(
                list(missing_tables)[0],  # Show first missing table
                list(provided_tables),
            )

    def _create_table_mapping(
        self, resolved_tables: Dict[str, str], mock_tables: List[BaseMockTable]
    ) -> Dict[str, BaseMockTable]:
        """Create mapping from qualified table names to mock table objects."""
        mock_table_map = {mock.get_qualified_name(): mock for mock in mock_tables}

        # Map original table references to mock tables
        table_mapping = {}
        for original_name, qualified_name in resolved_tables.items():
            table_mapping[original_name] = mock_table_map[qualified_name]

        return table_mapping

    def _generate_cte_query(
        self,
        query: str,
        table_mapping: Dict[str, BaseMockTable],
        mock_tables: List[BaseMockTable],
    ) -> str:
        """Generate query with CTE injections for mock data."""
        # Generate CTEs for each mock table
        ctes = []
        replacement_mapping = {}

        for original_name, mock_table in table_mapping.items():
            cte_alias = mock_table.get_cte_alias()
            cte_sql = self._generate_cte(mock_table, cte_alias)
            ctes.append(cte_sql)
            replacement_mapping[original_name] = cte_alias

        # Replace table names in original query
        modified_query = self._replace_table_names_in_query(query, replacement_mapping)

        # Combine CTEs with original query
        if ctes:
            # Check if modified query already starts with WITH
            modified_query_stripped = modified_query.strip()
            if modified_query_stripped.upper().startswith("WITH"):
                # Query already has WITH clause, so append our CTEs with comma
                cte_block = ",\n".join(ctes)
                final_query = f"WITH {cte_block},\n{modified_query_stripped[4:].strip()}"
            else:
                # Query doesn't have WITH clause, add it
                cte_block = "WITH " + ",\n".join(ctes)
                final_query = f"{cte_block}\n{modified_query}"
        else:
            final_query = modified_query

        return final_query

    def _generate_cte(self, mock_table: BaseMockTable, alias: str) -> str:
        """Generate CTE SQL for a mock table."""
        df = mock_table.to_dataframe()
        column_types = mock_table.get_column_types()
        if df.empty:
            # Generate empty CTE
            columns = list(column_types.keys())
            return f"{alias} AS (SELECT {', '.join(f'NULL as {col}' for col in columns)} WHERE 1=0)"  # noqa: E501

        # Get dialect to determine the correct CTE format
        dialect = self.adapter.get_sqlglot_dialect()

        if dialect == "bigquery":
            # BigQuery-specific format using UNNEST + STRUCT
            struct_rows = []
            columns = list(df.columns)

            # Process each row
            for idx, (_, row) in enumerate(df.iterrows()):
                # For the first row, include column names
                if idx == 0:
                    first_row_values = []
                    for col_name, value in row.items():
                        col_type = column_types.get(col_name, str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        first_row_values.append(f"{formatted_value} as {col_name}")
                    struct_rows.append(f"({', '.join(first_row_values)})")
                else:
                    # For subsequent rows, only include values
                    row_values = []
                    for col_name, value in row.items():
                        col_type = column_types.get(col_name, str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        row_values.append(formatted_value)
                    struct_rows.append(f"({', '.join(row_values)})")

            # Combine the rows into the UNNEST format
            joined_rows = ",\n      ".join(struct_rows)
            return f"{alias} AS (\n  SELECT\n    *\n  FROM UNNEST([\n    STRUCT\n      {joined_rows}\n]))"  # noqa: E501
        elif dialect == "redshift":
            # Redshift-specific format using UNION ALL (VALUES not supported in CTEs)
            columns = list(df.columns)
            select_statements = []

            for idx, (_, row) in enumerate(df.iterrows()):
                if idx == 0:
                    # First SELECT with column aliases
                    select_expressions = []
                    for col_name, value in row.items():
                        col_type = column_types.get(col_name, str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        select_expressions.append(f"{formatted_value} AS {col_name}")
                    select_statements.append(f"SELECT {', '.join(select_expressions)}")
                else:
                    # Subsequent SELECTs without aliases
                    row_values = []
                    for col_name, value in row.items():
                        col_type = column_types.get(col_name, str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        row_values.append(formatted_value)
                    select_statements.append(f"SELECT {', '.join(row_values)}")

            union_query = "\n  UNION ALL\n  ".join(select_statements)
            return f"{alias} AS (\n  {union_query}\n)"
        else:
            # Standard SQL format using VALUES clause
            values_rows = []
            for _, row in df.iterrows():
                row_values = []
                for col_name, value in row.items():
                    col_type = column_types.get(col_name, str)
                    formatted_value = self.adapter.format_value_for_cte(value, col_type)
                    row_values.append(formatted_value)
                values_rows.append(f"({', '.join(row_values)})")

            column_list = ", ".join(df.columns)
            values_clause = ", ".join(values_rows)

            return f"{alias} AS (SELECT * FROM (VALUES {values_clause}) AS t({column_list}))"

    def _replace_table_names_in_query(self, query: str, replacement_mapping: Dict[str, str]) -> str:
        """Replace table names in query using sqlglot AST transformation."""
        try:
            dialect = self.adapter.get_sqlglot_dialect()

            # Parse the query to an AST
            parsed = sqlglot.parse_one(query, dialect=dialect)

            # Create a transformer to replace table names
            def transform_tables(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Table):
                    # Get the original table name
                    if node.db and node.catalog:
                        original_name = f"{node.catalog}.{node.db}.{node.name}"
                    elif node.db:
                        original_name = f"{node.db}.{node.name}"
                    else:
                        original_name = str(node.name)

                    # Check if this table should be replaced
                    if original_name in replacement_mapping:
                        # Create a new Table node with the replacement name
                        new_table = exp.Table(
                            this=exp.Identifier(this=replacement_mapping[original_name])
                        )

                        # Preserve the table alias if it exists
                        if hasattr(node, "alias") and node.alias:
                            new_table.set("alias", node.alias)

                        return new_table

                return node

            # Apply the transformation to the AST
            transformed = parsed.transform(transform_tables)

            # Generate the SQL from the transformed AST
            result_sql: str = transformed.sql(dialect=dialect)
            return result_sql

        except Exception as e:
            # Re-raise the exception as SQLParseError to maintain compatibility
            # with the existing error handling expectations
            raise SQLParseError(query, str(e))  # noqa:  B904

    def _execute_with_physical_tables(
        self,
        query: str,
        table_mapping: Dict[str, BaseMockTable],
        mock_tables: List[BaseMockTable],
    ) -> str:
        """Execute query using physical temporary tables."""
        # Create physical tables
        replacement_mapping = {}

        for original_name, mock_table in table_mapping.items():
            temp_table_name = self.adapter.create_temp_table(mock_table)
            self.temp_tables.append(temp_table_name)
            replacement_mapping[original_name] = temp_table_name

        # Replace table names and return modified query
        return self._replace_table_names_in_query(query, replacement_mapping)

    def _deserialize_results(self, result_df: pd.DataFrame, result_class: Type[T]) -> List[T]:
        """Deserialize query results to typed objects."""
        if result_df.empty:
            return []

        # Get type hints from the result class
        type_hints = get_type_hints(result_class)

        results: List[T] = []
        for _, row in result_df.iterrows():
            # Convert row to dictionary with proper types
            converted_row: Dict[str, Any] = {}
            for col_name, value in row.items():
                if col_name in type_hints:
                    target_type = type_hints[col_name]
                    try:
                        converted_value = self.type_converter.convert(value, target_type)
                        converted_row[col_name] = converted_value
                    except Exception:
                        raise TypeConversionError(value, target_type, col_name)  # noqa:  B904
                else:
                    converted_row[col_name] = value

            # Create instance of result class
            try:
                result_obj = result_class(**converted_row)
                results.append(result_obj)
            except Exception as e:
                raise TypeError(  # noqa:  B904
                    f"Failed to create {result_class.__name__} instance: {e}"
                )

        return results
