"""Pytest plugin for SQL testing."""

import configparser
import functools
import os
import sys
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, cast

import pytest
from _pytest.nodes import Item

from ._adapters.base import DatabaseAdapter
from ._core import AdapterType, SQLTestCase, SQLTestFramework, sql_test_execution_data
from ._mock_table import BaseMockTable


T = TypeVar("T")


class SQLTestDecorator:
    """Manages SQL test decoration and execution."""

    def __init__(self) -> None:
        self._config: Optional[Dict[str, str]] = None
        self._project_root: Optional[str] = None
        self._config_parser: Optional[configparser.ConfigParser] = None

    def get_framework(self, adapter_type: Optional[AdapterType] = None) -> SQLTestFramework:
        """
        Create a fresh SQL test framework from configuration.

        Args:
            adapter_type: Optional adapter type to use. If provided, this will use
                          configuration from [sql_testing.{adapter_type}] section.
        """
        # Always create a fresh framework - no caching to avoid test isolation issues
        return self._create_framework_from_config(adapter_type)

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

        # Variable to hold the appropriate adapter instance
        database_adapter: DatabaseAdapter

        if adapter_type == "bigquery":
            from ._adapters.bigquery import BigQueryAdapter

            project_id = adapter_config.get("project_id")
            dataset_id = adapter_config.get("dataset_id")
            credentials_path = adapter_config.get("credentials_path")

            # Handle relative paths for credentials by converting to absolute
            if credentials_path and not os.path.isabs(credentials_path):
                project_root = self._get_project_root()
                credentials_path = os.path.join(project_root, credentials_path)

            if not project_id or not dataset_id:
                raise ValueError(
                    "BigQuery adapter requires 'project_id' and 'dataset_id' in configuration"
                )

            database_adapter = BigQueryAdapter(
                project_id=project_id,
                dataset_id=dataset_id,
                credentials_path=credentials_path,
            )
        elif adapter_type == "athena":
            from ._adapters.athena import AthenaAdapter

            database = adapter_config.get("database")
            s3_output_location = adapter_config.get("s3_output_location")
            region = adapter_config.get("region", "us-west-2")
            aws_access_key_id = adapter_config.get("aws_access_key_id")
            aws_secret_access_key = adapter_config.get("aws_secret_access_key")

            if not database or not s3_output_location:
                raise ValueError(
                    "Athena adapter requires 'database' and 's3_output_location' in configuration"
                )

            database_adapter = AthenaAdapter(
                database=database,
                s3_output_location=s3_output_location,
                region=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        elif adapter_type == "redshift":
            from ._adapters.redshift import RedshiftAdapter

            host = adapter_config.get("host")
            database = adapter_config.get("database")
            user = adapter_config.get("user")
            password = adapter_config.get("password")
            port = int(adapter_config.get("port", "5439"))

            if not all([host, database, user, password]):
                raise ValueError(
                    "Redshift adapter requires 'host', 'database', 'user', "
                    "and 'password' in configuration"
                )

            # All required values are now guaranteed to exist
            assert host is not None
            assert database is not None
            assert user is not None
            assert password is not None

            database_adapter = RedshiftAdapter(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
            )
        elif adapter_type == "trino":
            from ._adapters.trino import TrinoAdapter

            host = adapter_config.get("host")
            port = int(adapter_config.get("port", "8080"))
            user = adapter_config.get("user")
            catalog = adapter_config.get("catalog", "memory")
            schema = adapter_config.get("schema", "default")
            http_scheme = adapter_config.get("http_scheme", "http")

            # Auth dictionary for various authentication methods
            auth = None
            auth_type = adapter_config.get("auth_type")
            if auth_type == "basic":
                password = adapter_config.get("password")
                if password:
                    auth = {"type": "basic", "user": user, "password": password}
            elif auth_type == "jwt":
                token = adapter_config.get("token")
                if token:
                    auth = {"type": "jwt", "token": token}

            if not host:
                raise ValueError("Trino adapter requires 'host' in configuration")

            # Host is now guaranteed to exist
            assert host is not None

            database_adapter = TrinoAdapter(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
                http_scheme=http_scheme,
                auth=auth,
            )
        elif adapter_type == "snowflake":
            from ._adapters.snowflake import SnowflakeAdapter

            account = adapter_config.get("account")
            user = adapter_config.get("user")
            password = adapter_config.get("password")
            database = adapter_config.get("database")
            schema = adapter_config.get("schema", "PUBLIC")
            warehouse = adapter_config.get("warehouse")
            role = adapter_config.get("role")
            private_key_path = adapter_config.get("private_key_path")
            private_key_passphrase = adapter_config.get("private_key_passphrase")

            # Check required fields based on authentication method
            has_private_key = private_key_path or os.environ.get("SNOWFLAKE_PRIVATE_KEY")

            if has_private_key:
                # For key-pair auth, we just need account and user
                if not all([account, user]):
                    raise ValueError(
                        "Snowflake adapter with key-pair authentication requires "
                        "'account' and 'user' in configuration"
                    )
            else:
                # For password-based auth, we need password
                if not all([account, user, password]):
                    raise ValueError(
                        "Snowflake adapter requires 'account', 'user', and 'password' "
                        "in configuration (or use key-pair authentication for CI/CD)"
                    )

            # Database and warehouse are recommended but not always required
            if not database:
                database = ""  # Let adapter handle empty database

            # Ensure non-None values for required parameters
            assert account is not None
            assert user is not None

            database_adapter = SnowflakeAdapter(
                account=account,
                user=user,
                password=password,
                database=database,
                schema=schema,
                warehouse=warehouse,
                role=role,
                private_key_path=private_key_path,
                private_key_passphrase=private_key_passphrase,
            )
        else:
            raise ValueError(f"Unsupported adapter type: {adapter_type}")

        return SQLTestFramework(database_adapter)

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

    def _load_adapter_config(self, adapter_type: Optional[AdapterType] = None) -> Dict[str, str]:
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

# Global SQL execution context for logging
_sql_execution_context: Dict[str, Any] = {}


def sql_test(
    mock_tables: Optional[List[BaseMockTable]] = None,
    result_class: Optional[Type[T]] = None,
    use_physical_tables: Optional[bool] = None,
    adapter_type: Optional[AdapterType] = None,
    log_sql: Optional[bool] = None,
) -> Callable[[Callable[[], SQLTestCase[T]]], Callable[[], List[T]]]:
    """
    Decorator to mark a function as a SQL test.

    The decorator parameters will override any values specified in the SQLTestCase returned
    by the decorated function. If a parameter is not provided to the decorator, the
    SQLTestCase's value will be used.

    Args:
        mock_tables: Optional list of mock table objects to inject.
                     If provided, overrides mock_tables in SQLTestCase.
        result_class: Optional Pydantic model class for deserializing results.
                      If provided, overrides result_class in SQLTestCase.
        use_physical_tables: Optional flag to use physical tables instead of CTEs.
                            If provided, overrides use_physical_tables in SQLTestCase.
        adapter_type: Optional adapter type to use for this test
                     (e.g., 'bigquery', 'athena').
                     If provided, overrides adapter_type in SQLTestCase and uses config
                     from [sql_testing.{adapter_type}] section.
        log_sql: Optional flag to log the generated SQL to a file.
                 If provided, overrides log_sql in SQLTestCase.
    """

    def decorator(func: Callable[[], SQLTestCase[T]]) -> Callable[[], List[T]]:
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

            # Validate that function returns a SQLTestCase
            if not isinstance(test_case, SQLTestCase):
                raise TypeError(
                    f"Function {func.__name__} must return a SQLTestCase instance, "
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

            if log_sql is not None:
                test_case.log_sql = log_sql

            # Get framework and execute test
            framework = _sql_test_decorator.get_framework(test_case.adapter_type)

            # Create test context for logging
            test_context = {}

            # Try to get test metadata from the current pytest context
            import inspect

            frame = inspect.currentframe()
            while frame:
                frame_locals = frame.f_locals
                if "item" in frame_locals and hasattr(frame_locals["item"], "name"):
                    item = frame_locals["item"]
                    test_context["test_name"] = item.name
                    test_context["test_class"] = item.cls.__name__ if item.cls else None
                    test_context["test_file"] = (
                        str(item.fspath) if hasattr(item, "fspath") else None
                    )
                    # Create a unique test ID
                    test_context["test_id"] = str(id(item))
                    break
                frame = frame.f_back

            # If we couldn't get test context from stack, try to get it from function name
            if not test_context:
                test_context["test_name"] = func.__name__
                test_context["test_file"] = inspect.getfile(func) if func is not None else None
                # Create a unique test ID
                test_context["test_id"] = f"{func.__name__}_{id(func)}"

            results: List[T] = framework.run_test(test_case, test_context)

            return results

        # Mark function as SQL test
        wrapper._sql_test_decorated = True  # type: ignore
        wrapper._original_func = func  # type: ignore
        wrapper._decorator_params = {  # type: ignore
            "mock_tables": mock_tables,
            "result_class": result_class,
            "use_physical_tables": use_physical_tables,
            "adapter_type": adapter_type,
            "log_sql": log_sql,
        }

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
    """Register custom markers and configure xdist worker detection."""
    config.addinivalue_line("markers", "sql_test: mark test as a SQL test")

    # Set up pytest-xdist worker ID if running in parallel
    if hasattr(config, "workerinput"):
        # We're running with pytest-xdist
        worker_id = config.workerinput["workerid"]  # type: ignore[attr-defined]
        os.environ["PYTEST_XDIST_WORKER"] = worker_id


def pytest_runtest_call(item: Item) -> None:
    """Custom test execution for SQL tests."""
    item_function = getattr(item, "function", None)
    if item_function and hasattr(item_function, "_sql_test_decorated"):
        # Execute SQL test
        try:
            function = cast(Callable[[], List[Any]], item_function)
            results = function()
            # Store results for potential inspection
            # We use setattr to avoid mypy errors about unknown attributes
            # Attach results to the pytest item for potential retrieval later
            setattr(item, "_sql_test_results", results)  # noqa: B010
        except Exception as e:
            # Re-raise with better context
            raise AssertionError(f"SQL test failed: {e}") from e
    else:
        # Use default pytest execution
        item.runtest()


def pytest_runtest_makereport(item: Item, call: Any) -> None:
    """Hook to log SQL when tests fail (including assertion failures)."""
    # We want to log after the test call phase
    if call.when == "call":
        test_id = str(id(item))

        if call.excinfo is not None:
            # Test failed - check if we have SQL execution data for this test
            if test_id in sql_test_execution_data:
                data = sql_test_execution_data[test_id]
                sql_logger = data["sql_logger"]
                log_sql = data.get("log_sql")

                # Only log if log_sql is not False
                if log_sql is not False:
                    # Capture the assertion error details
                    import traceback

                    metadata = data["metadata"].copy()
                    # Update error info with the actual pytest error
                    # (might be different from stored error)
                    metadata["error"] = str(call.excinfo.value)
                    metadata["error_traceback"] = "".join(
                        traceback.format_exception(
                            call.excinfo.type, call.excinfo.value, call.excinfo.tb
                        )
                    )

                    # Log the SQL
                    log_path = sql_logger.log_sql(
                        sql=data["sql"],
                        test_name=data["test_name"],
                        test_class=data["test_class"],
                        test_file=data["test_file"],
                        failed=True,
                        metadata=metadata,
                    )

                    # Print log location
                    import sys

                    print(f"\nSQL logged to: file://{log_path}", file=sys.stderr)  # noqa: T201
                    sys.stderr.flush()

        # Clean up the stored data after the test (whether it passed or failed)
        if test_id in sql_test_execution_data:
            del sql_test_execution_data[test_id]
