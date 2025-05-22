"""Pytest plugin for SQL testing."""

import configparser
import functools
import os
import sys
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, cast

import pytest
from _pytest.nodes import Item

from .core import AdapterType, SQLTestFramework, TestCase
from .mock_table import BaseMockTable


T = TypeVar("T")


class SQLTestDecorator:
    """Manages SQL test decoration and execution."""

    def __init__(self) -> None:
        self._framework: Optional[SQLTestFramework] = None
        self._config: Optional[Dict[str, str]] = None
        self._project_root: Optional[str] = None
        self._config_parser: Optional[configparser.ConfigParser] = None

    def get_framework(
        self, adapter_type: Optional[AdapterType] = None
    ) -> SQLTestFramework:
        """
        Get or create SQL test framework from configuration.

        Args:
            adapter_type: Optional adapter type to use. If provided, this will use
                          configuration from [sql_testing.{adapter_type}] section.
        """
        if adapter_type is not None or self._framework is None:
            self._framework = self._create_framework_from_config(adapter_type)
        return self._framework

    def _create_framework_from_config(
        self, adapter_type: Optional[AdapterType] = None
    ) -> SQLTestFramework:
        """
        Create framework instance from configuration file.

        Args:
            adapter_type: Optional adapter type to use. If provided, this will use
                         configuration from [sql_testing.{adapter_type}] section.
        """
        config = self._load_config()

        # Use the provided adapter_type or get it from config
        if adapter_type is None:
            adapter_type = cast(AdapterType, config.get("adapter", "bigquery"))

        # Load adapter-specific configuration
        adapter_config = self._load_adapter_config(adapter_type)

        if adapter_type == "bigquery":
            from .adapters.bigquery import BigQueryAdapter

            project_id = adapter_config.get("project_id")
            dataset_id = adapter_config.get("dataset_id")
            credentials_path = adapter_config.get("credentials_path")

            # Handle relative paths for credentials by converting to absolute
            if credentials_path and not os.path.isabs(credentials_path):
                project_root = self._get_project_root()
                credentials_path = os.path.join(project_root, credentials_path)

            if not project_id or not dataset_id:
                raise ValueError(
                    "BigQuery adapter requires 'project_id' and 'dataset_id' "
                    "in configuration"
                )

            adapter = BigQueryAdapter(
                project_id=project_id,
                dataset_id=dataset_id,
                credentials_path=credentials_path,
            )
        else:
            raise ValueError(f"Unsupported adapter type: {adapter_type}")

        return SQLTestFramework(adapter)

    def _get_project_root(self) -> str:
        """Get the project root directory."""
        if self._project_root is not None:
            return self._project_root

        # First, check if SQL_TESTING_PROJECT_ROOT environment variable is set
        project_root = os.environ.get("SQL_TESTING_PROJECT_ROOT")
        if project_root and os.path.isdir(project_root):
            self._project_root = project_root
            return project_root

        # Second, check if pyproject.toml exists in any parent directory
        current_dir = os.getcwd()
        # Until we reach the filesystem root
        while current_dir != os.path.dirname(current_dir):
            # Look for strong project root indicators
            if (
                os.path.exists(os.path.join(current_dir, "pyproject.toml"))
                or os.path.exists(os.path.join(current_dir, "setup.py"))
                or os.path.exists(os.path.join(current_dir, ".git"))
            ):
                self._project_root = current_dir
                return current_dir

            # Look for .sql_testing_root marker file (could be created manually)
            if os.path.exists(os.path.join(current_dir, ".sql_testing_root")):
                self._project_root = current_dir
                return current_dir

            # Move up one directory
            current_dir = os.path.dirname(current_dir)

        # If no project root marker found, use current directory
        self._project_root = os.getcwd()
        return self._project_root

    def _get_config_parser(self) -> configparser.ConfigParser:
        """
        Get or create the configuration parser.

        Returns a cached ConfigParser instance with the configuration loaded from
        pytest.ini, setup.cfg, or tox.ini.
        """
        if self._config_parser is not None:
            return self._config_parser

        # Make sure we're in the project root or switch to it
        project_root = self._get_project_root()
        original_dir = os.getcwd()

        # Change to project root if needed
        if original_dir != project_root:
            os.chdir(project_root)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)

        config_parser = configparser.ConfigParser()

        # Search for config files in the project root
        config_files = ["pytest.ini", "setup.cfg", "tox.ini"]

        for config_file in config_files:
            if os.path.exists(config_file):
                config_parser.read(config_file)
                break

        # If we changed directories, change back to original
        if original_dir != project_root:
            os.chdir(original_dir)

        # Cache the config parser
        self._config_parser = config_parser
        return config_parser

    def _load_config(self) -> Dict[str, str]:
        """
        Load main configuration from pytest.ini or setup.cfg.

        Returns:
            Dictionary with configuration values from the [sql_testing] section.
        """
        if self._config is not None:
            return self._config

        config_parser = self._get_config_parser()

        # Extract sql_testing configuration
        if "sql_testing" in config_parser:
            self._config = dict(config_parser["sql_testing"])
            return self._config
        else:
            # Try to create default config or exit with error
            msg = (
                "No [sql_testing] section found in pytest.ini, setup.cfg, or tox.ini. "
                "Please configure the SQL testing library or set the "
                "SQL_TESTING_PROJECT_ROOT environment variable."
            )
            raise ValueError(msg)

    def _load_adapter_config(
        self, adapter_type: Optional[AdapterType] = None
    ) -> Dict[str, str]:
        """
        Load adapter-specific configuration.

        Args:
            adapter_type: Optional adapter type to use. If not provided, it will be
                         retrieved from the main sql_testing configuration.

        Returns:
            Dictionary with configuration values from the adapter-specific section.
        """
        config = self._load_config()

        # If adapter_type is not provided, get it from the config
        if adapter_type is None:
            adapter_type = cast(AdapterType, config.get("adapter", "bigquery"))

        config_parser = self._get_config_parser()

        # Get adapter-specific section
        section_name = f"sql_testing.{adapter_type}"

        if section_name in config_parser:
            return dict(config_parser[section_name])
        else:
            # Fall back to the main sql_testing section for backward compatibility
            return config


# Global instance
_sql_test_decorator = SQLTestDecorator()


def sql_test(
    mock_tables: Optional[List[BaseMockTable]] = None,
    result_class: Optional[Type[T]] = None,
    use_physical_tables: Optional[bool] = None,
    adapter_type: Optional[AdapterType] = None,
) -> Callable[[Callable[[], TestCase[T]]], Callable[[], List[T]]]:
    """
    Decorator to mark a function as a SQL test.

    The decorator parameters will override any values specified in the TestCase returned
    by the decorated function. If a parameter is not provided to the decorator, the
    TestCase's value will be used.

    Args:
        mock_tables: Optional list of mock table objects to inject.
                     If provided, overrides mock_tables in TestCase.
        result_class: Optional Pydantic model class for deserializing results.
                      If provided, overrides result_class in TestCase.
        use_physical_tables: Optional flag to use physical tables instead of CTEs.
                            If provided, overrides use_physical_tables in TestCase.
        adapter_type: Optional adapter type to use for this test
                     (e.g., 'bigquery', 'athena').
                     If provided, overrides adapter_type in TestCase and uses config
                     from [sql_testing.{adapter_type}] section.
    """

    def decorator(func: Callable[[], TestCase[T]]) -> Callable[[], List[T]]:
        # Check for multiple sql_test decorators
        if hasattr(func, "_sql_test_decorated"):
            raise ValueError(
                f"Function {func.__name__} has multiple @sql_test decorators. "
                "Only one @sql_test decorator is allowed per function."
            )

        @functools.wraps(func)
        def wrapper() -> List[T]:
            # Execute the test function to get the TestCase
            test_case = func()

            # Validate that function returns a TestCase
            if not isinstance(test_case, TestCase):
                raise TypeError(
                    f"Function {func.__name__} must return a TestCase instance, "
                    f"got {type(test_case)}"
                )

            # Apply decorator values only if provided
            # If decorator value is not None, override the TestCase value
            if mock_tables is not None:
                test_case.mock_tables = mock_tables

            if result_class is not None:
                test_case.result_class = result_class

            if use_physical_tables is not None:
                test_case.use_physical_tables = use_physical_tables

            if adapter_type is not None:
                test_case.adapter_type = adapter_type

            # Get framework and execute test
            framework = _sql_test_decorator.get_framework(test_case.adapter_type)
            results: List[T] = framework.run_test(test_case)

            return results

        # Mark function as SQL test
        wrapper._sql_test_decorated = True  # type: ignore
        wrapper._original_func = func  # type: ignore

        return wrapper

    return decorator


def pytest_collection_modifyitems(config: pytest.Config, items: List[Item]) -> None:
    """Pytest hook to discover and modify SQL test items."""
    sql_test_items = []

    for item in items:
        # Check if this is a SQL test
        if hasattr(getattr(item, "function", None), "_sql_test_decorated"):
            # Mark as SQL test for potential special handling
            item.add_marker(pytest.mark.sql_test)
            sql_test_items.append(item)

    # Could add special handling for SQL tests here
    # e.g., grouping, ordering, etc.


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "sql_test: mark test as a SQL test")


def pytest_runtest_call(item: Item) -> None:
    """Custom test execution for SQL tests."""
    item_function = getattr(item, "function", None)
    if item_function and hasattr(item_function, "_sql_test_decorated"):
        # Execute SQL test
        try:
            function = cast(Callable[[], List[Any]], item_function)
            results = function()
            # Store results for potential inspection
            item._sql_test_results = results
        except Exception as e:
            # Re-raise with better context
            raise AssertionError(f"SQL test failed: {e}") from e
    else:
        # Use default pytest execution
        item.runtest()
