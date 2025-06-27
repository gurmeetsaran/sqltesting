"""Core SQL testing framework."""

import os
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
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


if TYPE_CHECKING:
    import pandas as pd

# Heavy imports moved to function level for better performance
from ._adapters.base import DatabaseAdapter
from ._exceptions import (
    MockTableNotFoundError,
    QuerySizeLimitExceeded,
    SQLParseError,
    TypeConversionError,
)
from ._mock_table import BaseMockTable
from ._sql_logger import SQLLogger


# Type for adapter types
AdapterType = Literal["bigquery", "athena", "redshift", "trino", "snowflake"]

T = TypeVar("T")

# Global storage for SQL execution data (used by pytest plugin)
sql_test_execution_data: Dict[str, Dict[str, Any]] = {}


@dataclass
class SQLTestCase(Generic[T]):
    """Represents a SQL test case."""

    __test__ = False  # Tell pytest this is not a test class

    query: str
    default_namespace: Optional[str] = None
    mock_tables: Optional[List[BaseMockTable]] = None
    result_class: Optional[Type[T]] = None
    use_physical_tables: bool = False
    description: Optional[str] = None
    adapter_type: Optional[AdapterType] = None
    log_sql: Optional[bool] = None
    # Backward compatibility
    execution_database: Optional[str] = None

    def __post_init__(self) -> None:
        """Handle backward compatibility for execution_database parameter."""
        if self.execution_database is not None and self.default_namespace is not None:
            # Both provided - warn and prefer default_namespace
            import warnings

            warnings.warn(
                "Both 'default_namespace' and 'execution_database' provided. "
                "Using 'default_namespace'. Please migrate to 'default_namespace' only.",
                DeprecationWarning,
                stacklevel=2,
            )
        elif self.execution_database is not None and self.default_namespace is None:
            # Only execution_database provided - use it with deprecation warning
            import warnings

            warnings.warn(
                "'execution_database' parameter is deprecated. Use 'default_namespace' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.default_namespace = self.execution_database
        elif self.default_namespace is None and self.execution_database is None:
            # Neither provided - this is an error
            raise ValueError(
                "Must provide either 'default_namespace' (preferred) or 'execution_database' "
                "(deprecated) parameter"
            )


class SQLTestFramework:
    """Main framework for executing SQL tests."""

    def __init__(self, adapter: DatabaseAdapter, sql_logger: Optional[SQLLogger] = None) -> None:
        self.adapter = adapter
        self.type_converter = self.adapter.get_type_converter()
        self.temp_tables: List[str] = []
        self.sql_logger = sql_logger or SQLLogger()

    def run_test(
        self, test_case: SQLTestCase[T], test_context: Optional[Dict[str, Any]] = None
    ) -> List[T]:
        """
        Execute a test case and return deserialized results.

        Args:
            test_case: The test case to execute
            test_context: Optional context dictionary with test metadata

        Returns:
            List of result objects of type test_case.result_class
        """
        import time

        # Track execution time
        start_time = time.time()
        final_query = ""
        error_message = None
        row_count = None
        temp_table_queries: List[str] = []  # Track temp table creation queries

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
            # default_namespace is guaranteed to be set by __post_init__
            assert test_case.default_namespace is not None
            resolved_tables = self._resolve_table_names(
                referenced_tables, test_case.default_namespace
            )

            # Validate all required mock tables are provided
            self._validate_mock_tables(resolved_tables, test_case.mock_tables)

            # Create table name mapping
            table_mapping = self._create_table_mapping(resolved_tables, test_case.mock_tables)

            if test_case.use_physical_tables:
                # Create physical temporary tables
                final_query = self._execute_with_physical_tables(
                    test_case.query, table_mapping, test_case.mock_tables, temp_table_queries
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

            # Track row count
            row_count = len(result_df) if result_df is not None else 0

            # Convert results to typed objects
            results = self._deserialize_results(result_df, test_case.result_class)

            # Log SQL if enabled (success case)
            execution_time = time.time() - start_time

            # Store execution data for potential logging on test failure
            if test_context:
                test_id = test_context.get("test_id")
                if test_id:
                    sql_test_execution_data[test_id] = {
                        "sql": final_query,
                        "test_name": test_context.get("test_name", "unknown_test"),
                        "test_class": test_context.get("test_class"),
                        "test_file": test_context.get("test_file"),
                        "metadata": {
                            "query": test_case.query,
                            "default_namespace": test_case.default_namespace,
                            "mock_tables": test_case.mock_tables,
                            "adapter_type": self.adapter.__class__.__name__.replace(
                                "Adapter", ""
                            ).lower(),
                            "use_physical_tables": test_case.use_physical_tables,
                            "execution_time": execution_time,
                            "row_count": row_count,
                            "error": None,
                            "temp_table_queries": temp_table_queries,
                        },
                        "sql_logger": self.sql_logger,
                        "log_sql": test_case.log_sql,
                    }

            if self.sql_logger.should_log(test_case.log_sql):
                # Get test context info
                test_name = (
                    test_context.get("test_name", "unknown_test")
                    if test_context
                    else "unknown_test"
                )
                test_class = test_context.get("test_class") if test_context else None
                test_file = test_context.get("test_file") if test_context else None

                metadata = {
                    "query": test_case.query,
                    "default_namespace": test_case.default_namespace,
                    "mock_tables": test_case.mock_tables,
                    "adapter_type": self.adapter.get_sqlglot_dialect(),
                    "adapter_name": self.adapter.__class__.__name__.replace("Adapter", "").lower(),
                    "use_physical_tables": test_case.use_physical_tables,
                    "execution_time": execution_time,
                    "row_count": row_count,
                    "error": None,
                    "temp_table_queries": temp_table_queries,
                }

                # Log SQL immediately
                log_path = self.sql_logger.log_sql(
                    sql=final_query,
                    test_name=test_name,
                    test_class=test_class,
                    test_file=test_file,
                    failed=False,
                    metadata=metadata,
                )

                # Print log location if environment variable is set
                if os.environ.get("SQL_TEST_LOG_ALL", "").lower() in ("true", "1", "yes"):
                    import sys

                    print(f"\nSQL logged to: file://{log_path}", file=sys.stderr)  # noqa: T201
                    sys.stderr.flush()

            return results

        except Exception as e:
            # Store exception information for potential logging by pytest hook
            execution_time = time.time() - start_time

            # Capture full error details including traceback
            import traceback

            error_message = str(e)
            error_traceback = traceback.format_exc()

            # Store execution data for pytest hook to potentially log
            if test_context and test_case.log_sql is not False:
                test_id = test_context.get("test_id")
                if test_id:
                    # Update the execution data with error information
                    if test_id in sql_test_execution_data:
                        sql_test_execution_data[test_id]["metadata"]["error"] = error_message
                        sql_test_execution_data[test_id]["metadata"]["error_traceback"] = (
                            error_traceback
                        )
                        sql_test_execution_data[test_id]["metadata"]["execution_time"] = (
                            execution_time
                        )
                        sql_test_execution_data[test_id]["metadata"]["row_count"] = row_count
                    else:
                        # If we haven't stored data yet (error happened early), store it now
                        sql_test_execution_data[test_id] = {
                            "sql": final_query if "final_query" in locals() else test_case.query,
                            "test_name": test_context.get("test_name", "unknown_test"),
                            "test_class": test_context.get("test_class"),
                            "test_file": test_context.get("test_file"),
                            "metadata": {
                                "query": test_case.query,
                                "default_namespace": test_case.default_namespace,
                                "mock_tables": test_case.mock_tables,
                                "adapter_type": self.adapter.get_sqlglot_dialect(),
                                "adapter_name": self.adapter.__class__.__name__.replace(
                                    "Adapter", ""
                                ).lower(),
                                "use_physical_tables": test_case.use_physical_tables,
                                "execution_time": execution_time,
                                "row_count": row_count,
                                "error": error_message,
                                "error_traceback": error_traceback,
                                "temp_table_queries": temp_table_queries,
                            },
                            "sql_logger": self.sql_logger,
                            "log_sql": test_case.log_sql,
                        }

            raise

        finally:
            # Cleanup any temporary tables
            if self.temp_tables:
                self.adapter.cleanup_temp_tables(self.temp_tables)
                self.temp_tables = []

    def _parse_sql_tables(self, query: str) -> List[str]:
        """Parse SQL query to extract table references."""
        try:
            import sqlglot
            from sqlglot import exp

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
        self, referenced_tables: List[str], default_namespace: str
    ) -> Dict[str, str]:
        """
        Resolve unqualified table names using default namespace context.

        Returns:
            Dict mapping original table name to fully qualified name
        """
        resolved = {}
        for table_name in referenced_tables:
            if "." in table_name:
                # Already qualified
                resolved[table_name] = table_name
            else:
                # Add namespace prefix
                qualified_name = f"{default_namespace}.{table_name}"
                resolved[table_name] = qualified_name

        return resolved

    def _validate_mock_tables(
        self, resolved_tables: Dict[str, str], mock_tables: List[BaseMockTable]
    ) -> None:
        """Validate that all required mock tables are provided."""
        provided_tables = {mock.get_qualified_name() for mock in mock_tables}
        required_tables = set(resolved_tables.values())

        # Perform case-insensitive validation for all SQL databases
        provided_tables_upper = {table.upper() for table in provided_tables}
        missing_tables = set()

        for required_table in required_tables:
            if required_table.upper() not in provided_tables_upper:
                missing_tables.add(required_table)

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

        # Map original table references to mock tables using case-insensitive matching
        table_mapping = {}

        for original_name, qualified_name in resolved_tables.items():
            # Case-insensitive matching for all SQL databases
            matched_mock = None
            for mock_qualified_name, mock_table in mock_table_map.items():
                if qualified_name.upper() == mock_qualified_name.upper():
                    matched_mock = mock_table
                    break
            if matched_mock:
                table_mapping[original_name] = matched_mock
            else:
                # This shouldn't happen if validation passed, but fallback to exact match
                exact_match = mock_table_map.get(qualified_name)
                if exact_match:
                    table_mapping[original_name] = exact_match

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

        if dialect in ["bigquery", "snowflake"]:
            # BigQuery and Snowflake-specific format using UNION ALL
            # (Snowflake VALUES clauses don't support complex expressions like ARRAY_CONSTRUCT)
            columns = list(df.columns)
            select_statements = []

            for idx, (_, row) in enumerate(df.iterrows()):
                if idx == 0:
                    # First SELECT with column aliases
                    select_expressions = []
                    for col_name, value in row.items():
                        col_type = column_types.get(str(col_name), str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        select_expressions.append(f"{formatted_value} AS {col_name}")
                    select_statements.append(f"SELECT {', '.join(select_expressions)}")
                else:
                    # Subsequent SELECTs without aliases
                    row_values = []
                    for col_name, value in row.items():
                        col_type = column_types.get(str(col_name), str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        row_values.append(formatted_value)
                    select_statements.append(f"SELECT {', '.join(row_values)}")

            union_query = "\n  UNION ALL\n  ".join(select_statements)
            return f"{alias} AS (\n  {union_query}\n)"
        elif dialect == "redshift":
            # Redshift-specific format using UNION ALL (VALUES not supported in CTEs)
            columns = list(df.columns)
            select_statements = []

            for idx, (_, row) in enumerate(df.iterrows()):
                if idx == 0:
                    # First SELECT with column aliases
                    select_expressions = []
                    for col_name, value in row.items():
                        col_type = column_types.get(str(col_name), str)
                        formatted_value = self.adapter.format_value_for_cte(value, col_type)
                        select_expressions.append(f"{formatted_value} AS {col_name}")
                    select_statements.append(f"SELECT {', '.join(select_expressions)}")
                else:
                    # Subsequent SELECTs without aliases
                    row_values = []
                    for col_name, value in row.items():
                        col_type = column_types.get(str(col_name), str)
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
                    col_type = column_types.get(str(col_name), str)
                    formatted_value = self.adapter.format_value_for_cte(value, col_type)
                    row_values.append(formatted_value)
                values_rows.append(f"({', '.join(row_values)})")

            column_list = ", ".join(df.columns)
            values_clause = ", ".join(values_rows)

            return f"{alias} AS (SELECT * FROM (VALUES {values_clause}) AS t({column_list}))"

    def _replace_table_names_in_query(self, query: str, replacement_mapping: Dict[str, str]) -> str:
        """Replace table names in query using sqlglot AST transformation."""
        try:
            import sqlglot
            from sqlglot import exp

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
                    # Perform case-insensitive matching for all SQL databases
                    replacement_name = None
                    for mapping_key, mapping_value in replacement_mapping.items():
                        if original_name.upper() == mapping_key.upper():
                            replacement_name = mapping_value
                            break

                    if replacement_name:
                        # Parse the replacement name to handle schema qualification
                        parts = replacement_name.split(".")
                        # Only quote identifiers for Snowflake physical tables (which contain TEMP_)
                        should_quote = "TEMP_" in replacement_name and dialect == "snowflake"

                        if len(parts) == 3:
                            # catalog.schema.table format
                            new_table = exp.Table(
                                this=exp.Identifier(this=parts[2], quoted=should_quote),
                                db=exp.Identifier(this=parts[1], quoted=should_quote),
                                catalog=exp.Identifier(this=parts[0], quoted=should_quote),
                            )
                        elif len(parts) == 2:
                            # schema.table format
                            new_table = exp.Table(
                                this=exp.Identifier(this=parts[1], quoted=should_quote),
                                db=exp.Identifier(this=parts[0], quoted=should_quote),
                            )
                        else:
                            # Just table name
                            new_table = exp.Table(
                                this=exp.Identifier(this=replacement_name, quoted=should_quote)
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
        temp_table_queries: List[str],
    ) -> str:
        """Execute query using physical temporary tables."""
        # Create physical tables
        replacement_mapping = {}

        for original_name, mock_table in table_mapping.items():
            try:
                # Check if adapter has method to get temp table SQL
                if hasattr(self.adapter, "create_temp_table_with_sql"):
                    temp_table_name, create_sql = self.adapter.create_temp_table_with_sql(
                        mock_table
                    )
                    temp_table_queries.append(create_sql)
                else:
                    temp_table_name = self.adapter.create_temp_table(mock_table)
                    # Try to generate approximate SQL for logging
                    temp_table_queries.append(
                        f"-- CREATE TEMP TABLE {temp_table_name} (SQL not captured)"
                    )

                self.temp_tables.append(temp_table_name)
                replacement_mapping[original_name] = temp_table_name
            except Exception:
                # If table creation fails, still try to capture the SQL for debugging
                if hasattr(self.adapter, "create_temp_table_with_sql") and hasattr(
                    mock_table, "get_table_name"
                ):
                    try:
                        temp_table_name, ctas_sql = self.adapter.create_temp_table_with_sql(
                            mock_table
                        )
                        temp_table_queries.append(ctas_sql)
                        replacement_mapping[original_name] = temp_table_name
                    except Exception:
                        # If even SQL generation fails, add a placeholder
                        temp_table_queries.append(
                            f"-- CREATE TEMP TABLE for {original_name} (SQL generation failed)"
                        )
                        replacement_mapping[original_name] = f"temp_{original_name}_failed"
                # Re-raise the original exception
                raise

        # Replace table names and return modified query
        return self._replace_table_names_in_query(query, replacement_mapping)

    def _deserialize_results(self, result_df: "pd.DataFrame", result_class: Type[T]) -> List[T]:
        """Deserialize query results to typed objects."""
        import numpy as np

        if result_df.empty:
            return []

        # STEP 1: Convert database-returned NaN values to Python None
        #
        # WHY THIS IS NEEDED:
        # - SQL databases return NULL values which pandas converts to NaN
        # - Different database adapters may return NaN for null numeric/float columns
        # - NaN values break object serialization (dataclass/Pydantic instantiation)
        # - Python None is the correct representation for nullable/optional fields
        #
        # RELATIONSHIP TO mock_table.py NaN HANDLING:
        # - mock_table.py: Handles NaN created during DataFrame dtype conversion (input side)
        # - core.py (here): Handles NaN returned from actual database queries (output side)
        # - Both are needed because NaN can appear at different pipeline stages
        result_df = result_df.replace([np.nan], [None])
        # Get type hints from the result class
        type_hints = get_type_hints(result_class)

        results: List[T] = []
        for _, row in result_df.iterrows():
            # Convert row to dictionary with proper types
            converted_row: Dict[str, Any] = {}
            for col_name, value in row.items():
                col_name_str = str(col_name)
                if col_name_str in type_hints:
                    target_type = type_hints[col_name_str]
                    try:
                        converted_value = self.type_converter.convert(value, target_type)
                        converted_row[col_name_str] = converted_value
                    except Exception:
                        raise TypeConversionError(value, target_type, col_name_str)  # noqa:  B904
                else:
                    converted_row[col_name_str] = value

            # Create instance of result class
            try:
                result_obj = result_class(**converted_row)
                results.append(result_obj)
            except Exception as e:
                raise TypeError(  # noqa:  B904
                    f"Failed to create {result_class.__name__} instance: {e}"
                )

        return results
