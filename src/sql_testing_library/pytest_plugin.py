"""Pytest plugin for SQL testing."""

import configparser
import functools
import os
import sys
from typing import Callable, List, Type, TypeVar

import pytest

from .core import SQLTestFramework, TestCase
from .mock_table import BaseMockTable


T = TypeVar("T")


class SQLTestDecorator:
    """Manages SQL test decoration and execution."""

    def __init__(self):
        self._framework = None
        self._config = None
        self._project_root = None

    def get_framework(self) -> SQLTestFramework:
        """Get or create SQL test framework from configuration."""
        if self._framework is None:
            self._framework = self._create_framework_from_config()
        return self._framework

    def _create_framework_from_config(self) -> SQLTestFramework:
        """Create framework instance from configuration file."""
        config = self._load_config()

        adapter_type = config.get("adapter", "bigquery")

        if adapter_type == "bigquery":
            from .adapters.bigquery import BigQueryAdapter

            project_id = config.get("project_id")
            dataset_id = config.get("dataset_id")
            credentials_path = config.get("credentials_path")

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

    def _get_project_root(self):
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

    def _load_config(self) -> dict:
        """Load configuration from pytest.ini or setup.cfg."""
        if self._config is not None:
            return self._config

        # Make sure we're in the project root or switch to it
        project_root = self._get_project_root()
        original_dir = os.getcwd()

        # Change to project root if needed and add to sys.path
        if original_dir != project_root:
            os.chdir(project_root)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)

        config = configparser.ConfigParser()

        # Search for config files in the project root
        config_files = ["pytest.ini", "setup.cfg", "tox.ini"]

        for config_file in config_files:
            if os.path.exists(config_file):
                config.read(config_file)
                break

        # If we changed directories, change back to original
        # For PyCharm compatibility, we don't want to disturb its working directory
        if original_dir != project_root:
            os.chdir(original_dir)

        # Extract sql_testing configuration
        if "sql_testing" in config:
            self._config = dict(config["sql_testing"])
        else:
            # Try to create default config or exit with error
            msg = (
                "No [sql_testing] section found in pytest.ini, setup.cfg, or tox.ini. "
                "Please configure the SQL testing library or set the "
                "SQL_TESTING_PROJECT_ROOT environment variable."
            )
            raise ValueError(msg)

        return self._config


# Global instance
_sql_test_decorator = SQLTestDecorator()


def sql_test(
    mock_tables: List[BaseMockTable] = None,
    result_class: Type[T] = None,
    use_physical_tables: bool = None,
):
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
    """

    def decorator(func: Callable[[], TestCase]):
        # Check for multiple sql_test decorators
        if hasattr(func, "_sql_test_decorated"):
            raise ValueError(
                f"Function {func.__name__} has multiple @sql_test decorators. "
                "Only one @sql_test decorator is allowed per function."
            )

        @functools.wraps(func)
        def wrapper():
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

            # Get framework and execute test
            framework = _sql_test_decorator.get_framework()
            results = framework.run_test(test_case)

            return results

        # Mark function as SQL test
        wrapper._sql_test_decorated = True
        wrapper._original_func = func

        return wrapper

    return decorator


def pytest_collection_modifyitems(config, items):
    """Pytest hook to discover and modify SQL test items."""
    sql_test_items = []

    for item in items:
        # Check if this is a SQL test
        if hasattr(item.function, "_sql_test_decorated"):
            # Mark as SQL test for potential special handling
            item.add_marker(pytest.mark.sql_test)
            sql_test_items.append(item)

    # Could add special handling for SQL tests here
    # e.g., grouping, ordering, etc.


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "sql_test: mark test as a SQL test")


def pytest_runtest_call(item):
    """Custom test execution for SQL tests."""
    if hasattr(item.function, "_sql_test_decorated"):
        # Execute SQL test
        try:
            results = item.function()
            # Store results for potential inspection
            item._sql_test_results = results
        except Exception as e:
            # Re-raise with better context
            raise AssertionError(f"SQL test failed: {e}") from e
    else:
        # Use default pytest execution
        item.runtest()
