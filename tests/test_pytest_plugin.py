"""Tests for the pytest_plugin configuration loading."""

import configparser
import os
import tempfile
import unittest
from unittest import mock

import pytest

from sql_testing_library._core import SQLTestCase
from sql_testing_library._mock_table import BaseMockTable
from sql_testing_library._pytest_plugin import SQLTestDecorator, sql_test


class TestPytestPluginConfig(unittest.TestCase):
    def test_load_config_basic(self):
        """Test loading basic configuration from pytest.ini."""
        # Create a mock ConfigParser
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {
            "adapter": "bigquery",
        }
        mock_config["sql_testing.bigquery"] = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_path": "/path/to/credentials.json",
        }

        # Create SQLTestDecorator instance with mocked config parser
        decorator = SQLTestDecorator()
        decorator._config_parser = mock_config

        # Test loading the main config
        config = decorator._load_config()
        assert config["adapter"] == "bigquery"

        # Test loading adapter-specific config
        adapter_config = decorator._load_adapter_config()
        assert adapter_config["project_id"] == "test-project"
        assert adapter_config["dataset_id"] == "test_dataset"
        assert adapter_config["credentials_path"] == "/path/to/credentials.json"

    def test_load_config_basic_with_athena(self):
        """Test loading basic configuration from pytest.ini."""
        # Create a mock ConfigParser
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {
            "adapter": "athena",
        }
        mock_config["sql_testing.bigquery"] = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_path": "/path/to/credentials.json",
        }

        mock_config["sql_testing.athena"] = {
            "foo": "bar",
        }

        # Create SQLTestDecorator instance with mocked config parser
        decorator = SQLTestDecorator()
        decorator._config_parser = mock_config

        # Test loading the main config
        config = decorator._load_config()
        assert config["adapter"] == "athena"

        # Test loading adapter-specific config
        adapter_config = decorator._load_adapter_config()
        assert adapter_config["foo"] == "bar"

    def test_load_config_with_adapter_type(self):
        """Test loading configuration for a specific adapter type."""
        # Create a mock ConfigParser
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {
            "adapter": "bigquery",
        }
        mock_config["sql_testing.bigquery"] = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_path": "/path/to/credentials.json",
        }
        mock_config["sql_testing.athena"] = {
            "database": "test_db",
            "s3_output_location": "s3://test-bucket/",
            "region": "us-west-2",
        }

        # Create SQLTestDecorator instance with mocked config parser
        decorator = SQLTestDecorator()
        decorator._config_parser = mock_config

        # Test loading configuration for Athena
        athena_config = decorator._load_adapter_config("athena")
        assert athena_config["database"] == "test_db"
        assert athena_config["s3_output_location"] == "s3://test-bucket/"
        assert athena_config["region"] == "us-west-2"

        # Test loading configuration for BigQuery
        bigquery_config = decorator._load_adapter_config("bigquery")
        assert bigquery_config["project_id"] == "test-project"
        assert bigquery_config["dataset_id"] == "test_dataset"

    def test_adapter_type_fallback(self):
        """Test fallback to main config when adapter-specific section doesn't exist."""
        # Create a mock ConfigParser
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {
            "adapter": "bigquery",
            "project_id": "default-project",
            "dataset_id": "default_dataset",
        }
        # No sql_testing.redshift section

        # Create SQLTestDecorator instance with mocked config parser
        decorator = SQLTestDecorator()
        decorator._config_parser = mock_config
        decorator._config = dict(mock_config["sql_testing"])

        # Test loading configuration for Redshift (which doesn't exist)
        # Should fall back to main config
        redshift_config = decorator._load_adapter_config("redshift")
        assert redshift_config["project_id"] == "default-project"
        assert redshift_config["dataset_id"] == "default_dataset"

    @pytest.mark.skipif(
        os.environ.get("CI") == "true", reason="Test requires creating temporary files"
    )
    def test_config_parser_caching(self):
        """Test that the config parser is cached correctly."""
        # Create a temporary pytest.ini file
        with tempfile.TemporaryDirectory() as temp_dir:
            pytest_ini_path = os.path.join(temp_dir, "pytest.ini")
            with open(pytest_ini_path, "w") as f:
                f.write(
                    """
[sql_testing]
adapter = bigquery

[sql_testing.bigquery]
project_id = test-project
dataset_id = test_dataset
credentials_path = /path/to/credentials.json
                """
                )

            # Create SQLTestDecorator instance
            decorator = SQLTestDecorator()

            # Mock _get_project_root to return our temp directory
            with mock.patch.object(decorator, "_get_project_root", return_value=temp_dir):
                # First call should parse the file
                config_parser1 = decorator._get_config_parser()
                assert "sql_testing" in config_parser1
                assert "sql_testing.bigquery" in config_parser1

                # Change the file (this shouldn't affect the cached parser)
                with open(pytest_ini_path, "w") as f:
                    f.write(
                        """
[sql_testing]
adapter = athena
                    """
                    )

                # Second call should return the cached parser
                config_parser2 = decorator._get_config_parser()

                # Should still have the original sections
                assert "sql_testing.bigquery" in config_parser2
                assert config_parser2["sql_testing"]["adapter"] == "bigquery"

                # Verify it's the same object
                assert config_parser1 is config_parser2


class TestSQLTestDecorator(unittest.TestCase):
    """Tests for the SQLTestDecorator class."""

    def setUp(self):
        """Set up test decorator."""
        self.decorator = SQLTestDecorator()

    def test_project_root_detection_env_var(self):
        """Test project root detection using environment variable."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.dict(os.environ, {"SQL_TESTING_PROJECT_ROOT": temp_dir}):
                # Clear cached project root
                self.decorator._project_root = None

                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_detection_pyproject_toml(self):
        """Test project root detection using pyproject.toml."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create pyproject.toml file
            pyproject_path = os.path.join(temp_dir, "pyproject.toml")
            with open(pyproject_path, "w") as f:
                f.write("[tool.poetry]\nname = 'test'\n")

            # Clear cached project root
            self.decorator._project_root = None

            with mock.patch("os.getcwd", return_value=temp_dir):
                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_detection_git(self):
        """Test project root detection using .git directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create .git directory
            git_dir = os.path.join(temp_dir, ".git")
            os.makedirs(git_dir)

            # Clear cached project root
            self.decorator._project_root = None

            with mock.patch("os.getcwd", return_value=temp_dir):
                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_detection_setup_py(self):
        """Test project root detection using setup.py."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create setup.py file
            setup_path = os.path.join(temp_dir, "setup.py")
            with open(setup_path, "w") as f:
                f.write("from setuptools import setup\nsetup(name='test')\n")

            # Clear cached project root
            self.decorator._project_root = None

            with mock.patch("os.getcwd", return_value=temp_dir):
                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_detection_sql_testing_root_marker(self):
        """Test project root detection using .sql_testing_root marker."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create .sql_testing_root marker file
            marker_path = os.path.join(temp_dir, ".sql_testing_root")
            with open(marker_path, "w") as f:
                f.write("")

            # Clear cached project root
            self.decorator._project_root = None

            with mock.patch("os.getcwd", return_value=temp_dir):
                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_detection_fallback(self):
        """Test project root detection fallback to current directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Clear cached project root
            self.decorator._project_root = None

            with mock.patch("os.getcwd", return_value=temp_dir):
                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_caching(self):
        """Test that project root is cached."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set initial project root
            self.decorator._project_root = temp_dir

            # Should return cached value without calling detection logic
            root = self.decorator._get_project_root()
            self.assertEqual(root, temp_dir)

    def test_load_config_missing_section(self):
        """Test loading config when [sql_testing] section is missing."""
        mock_config = configparser.ConfigParser()
        # No sql_testing section

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._load_config()

        self.assertIn("No [sql_testing] section found", str(context.exception))

    def test_create_framework_bigquery_missing_config(self):
        """Test BigQuery framework creation with missing required config."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "bigquery"}
        mock_config["sql_testing.bigquery"] = {
            # Missing project_id and dataset_id
            "credentials_path": "/path/to/creds.json"
        }

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._create_framework_from_config("bigquery")

        self.assertIn("BigQuery adapter requires", str(context.exception))

    def test_create_framework_athena_missing_config(self):
        """Test Athena framework creation with missing required config."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "athena"}
        mock_config["sql_testing.athena"] = {
            # Missing database and s3_output_location
            "region": "us-west-2"
        }

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._create_framework_from_config("athena")

        self.assertIn("Athena adapter requires", str(context.exception))

    def test_create_framework_redshift_missing_config(self):
        """Test Redshift framework creation with missing required config."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "redshift"}
        mock_config["sql_testing.redshift"] = {
            "host": "localhost",
            # Missing database, user, password
        }

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._create_framework_from_config("redshift")

        self.assertIn("Redshift adapter requires", str(context.exception))

    def test_create_framework_snowflake_missing_config(self):
        """Test Snowflake framework creation with missing required config."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "snowflake"}
        mock_config["sql_testing.snowflake"] = {
            "account": "test-account",
            "user": "test-user",
            # Missing password, database, warehouse
        }

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._create_framework_from_config("snowflake")

        self.assertIn("Snowflake adapter requires", str(context.exception))

    def test_create_framework_trino_missing_config(self):
        """Test Trino framework creation with missing required config."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "trino"}
        mock_config["sql_testing.trino"] = {
            # Missing host
            "port": "8080"
        }

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._create_framework_from_config("trino")

        self.assertIn("Trino adapter requires", str(context.exception))

    def test_create_framework_unsupported_adapter(self):
        """Test framework creation with unsupported adapter type."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "unsupported"}

        self.decorator._config_parser = mock_config

        with self.assertRaises(ValueError) as context:
            self.decorator._create_framework_from_config("unsupported")

        self.assertIn("Unsupported adapter type: unsupported", str(context.exception))

    def test_create_framework_bigquery_relative_credentials_path(self):
        """Test BigQuery framework creation with relative credentials path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_config = configparser.ConfigParser()
            mock_config["sql_testing"] = {"adapter": "bigquery"}
            mock_config["sql_testing.bigquery"] = {
                "project_id": "test-project",
                "dataset_id": "test-dataset",
                "credentials_path": "relative/path/creds.json",
            }

            self.decorator._config_parser = mock_config
            self.decorator._project_root = temp_dir

            # Mock the BigQueryAdapter to avoid actual creation
            with mock.patch(
                "sql_testing_library._adapters.bigquery.BigQueryAdapter"
            ) as mock_adapter:
                self.decorator._create_framework_from_config("bigquery")

                # Verify that absolute path was constructed
                expected_path = os.path.join(temp_dir, "relative/path/creds.json")
                mock_adapter.assert_called_once_with(
                    project_id="test-project",
                    dataset_id="test-dataset",
                    credentials_path=expected_path,
                )

    def test_create_framework_trino_with_auth(self):
        """Test Trino framework creation with authentication."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "trino"}
        mock_config["sql_testing.trino"] = {
            "host": "localhost",
            "port": "8080",
            "user": "test-user",
            "auth_type": "basic",
            "password": "test-password",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.trino.TrinoAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("trino")

            expected_auth = {
                "type": "basic",
                "user": "test-user",
                "password": "test-password",
            }
            mock_adapter.assert_called_once_with(
                host="localhost",
                port=8080,
                user="test-user",
                catalog="memory",
                schema="default",
                http_scheme="http",
                auth=expected_auth,
            )

    def test_create_framework_trino_with_jwt_auth(self):
        """Test Trino framework creation with JWT authentication."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "trino"}
        mock_config["sql_testing.trino"] = {
            "host": "localhost",
            "auth_type": "jwt",
            "token": "jwt-token-here",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.trino.TrinoAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("trino")

            expected_auth = {"type": "jwt", "token": "jwt-token-here"}
            mock_adapter.assert_called_once_with(
                host="localhost",
                port=8080,
                user=None,
                catalog="memory",
                schema="default",
                http_scheme="http",
                auth=expected_auth,
            )


class TestSQLTestDecoratorFunction(unittest.TestCase):
    """Tests for the sql_test decorator function."""

    def test_sql_test_decorator_basic(self):
        """Test basic sql_test decorator functionality."""

        @sql_test()
        def test_function():
            return SQLTestCase(query="SELECT 1 as result", default_namespace="test")

        # Check that function is marked as decorated
        self.assertTrue(hasattr(test_function, "_sql_test_decorated"))
        self.assertTrue(test_function._sql_test_decorated)
        self.assertTrue(hasattr(test_function, "_original_func"))

    def test_sql_test_decorator_with_params(self):
        """Test sql_test decorator with parameters."""

        class TestMockTable(BaseMockTable):
            def get_table_name(self):
                return "test_table"

            def get_database_name(self):
                return "test_db"

        mock_table = TestMockTable(data=[])

        @sql_test(mock_tables=[mock_table], use_physical_tables=True, adapter_type="bigquery")
        def test_function():
            return SQLTestCase(query="SELECT * FROM test_table", default_namespace="test")

        # Check that function is marked as decorated
        self.assertTrue(hasattr(test_function, "_sql_test_decorated"))

    def test_sql_test_decorator_multiple_decorators_error(self):
        """Test that multiple sql_test decorators raise an error."""

        def test_function():
            return SQLTestCase(query="SELECT 1", default_namespace="test")

        # Apply first decorator
        decorated_once = sql_test()(test_function)

        # Applying second decorator should raise error
        with self.assertRaises(ValueError) as context:
            sql_test()(decorated_once)

        self.assertIn("multiple @sql_test decorators", str(context.exception))

    def test_sql_test_decorator_invalid_return_type(self):
        """Test sql_test decorator with invalid return type."""

        @sql_test()
        def test_function():
            return "not a SQLTestCase"

        with self.assertRaises(TypeError) as context:
            test_function()

        self.assertIn("must return a SQLTestCase instance", str(context.exception))

    def test_sql_test_decorator_parameter_override(self):
        """Test that decorator parameters override SQLTestCase values."""

        class TestMockTable1(BaseMockTable):
            def get_table_name(self):
                return "table1"

            def get_database_name(self):
                return "test_db"

        class TestMockTable2(BaseMockTable):
            def get_table_name(self):
                return "table2"

            def get_database_name(self):
                return "test_db"

        mock_table1 = TestMockTable1(data=[])
        mock_table2 = TestMockTable2(data=[])

        @sql_test(mock_tables=[mock_table2], use_physical_tables=True, adapter_type="athena")
        def test_function():
            # Return SQLTestCase with different values
            return SQLTestCase(
                query="SELECT 1",
                default_namespace="test",
                mock_tables=[mock_table1],
                use_physical_tables=False,
                adapter_type="bigquery",
            )

        # Mock the framework to verify parameter override
        with mock.patch(
            "sql_testing_library._pytest_plugin._sql_test_decorator.get_framework"
        ) as mock_get_framework:
            mock_framework = mock.Mock()
            mock_framework.run_test.return_value = []
            mock_get_framework.return_value = mock_framework

            test_function()

            # Verify that get_framework was called with decorator's adapter_type
            mock_get_framework.assert_called_once_with("athena")

            # Verify that run_test was called with modified test case
            args, kwargs = mock_framework.run_test.call_args
            test_case = args[0]

            self.assertEqual(test_case.mock_tables, [mock_table2])
            self.assertTrue(test_case.use_physical_tables)
            self.assertEqual(test_case.adapter_type, "athena")


class TestPytestHooks(unittest.TestCase):
    """Tests for pytest hooks."""

    def test_pytest_collection_modifyitems(self):
        """Test pytest_collection_modifyitems hook."""
        from sql_testing_library._pytest_plugin import pytest_collection_modifyitems

        # Create mock items
        mock_config = mock.Mock()

        # Mock SQL test item
        sql_test_item = mock.Mock()
        sql_test_function = mock.Mock()
        sql_test_function._sql_test_decorated = True
        sql_test_item.function = sql_test_function

        # Mock regular test item
        regular_item = mock.Mock()
        regular_item.function = mock.Mock()

        items = [sql_test_item, regular_item]

        # Call the hook
        pytest_collection_modifyitems(mock_config, items)

        # Verify SQL test was marked
        sql_test_item.add_marker.assert_called_once()

    def test_pytest_configure(self):
        """Test pytest_configure hook."""
        from sql_testing_library._pytest_plugin import pytest_configure

        mock_config = mock.Mock(spec=["addinivalue_line"])
        # By using spec, hasattr(config, 'workerinput') will return False

        # Call the hook
        pytest_configure(mock_config)

        # Verify marker was added
        mock_config.addinivalue_line.assert_called_once_with(
            "markers", "sql_test: mark test as a SQL test"
        )

    def test_pytest_configure_with_xdist(self):
        """Test pytest_configure hook with xdist worker."""
        import os

        from sql_testing_library._pytest_plugin import pytest_configure

        mock_config = mock.Mock()
        # Simulate xdist worker with workerinput
        mock_config.workerinput = {"workerid": "gw0"}

        # Store original env var if it exists
        original_env = os.environ.get("PYTEST_XDIST_WORKER")

        try:
            # Call the hook
            pytest_configure(mock_config)

            # Verify marker was added
            mock_config.addinivalue_line.assert_called_once_with(
                "markers", "sql_test: mark test as a SQL test"
            )

            # Verify worker ID was set in environment
            self.assertEqual(os.environ.get("PYTEST_XDIST_WORKER"), "gw0")
        finally:
            # Restore original environment
            if original_env is None and "PYTEST_XDIST_WORKER" in os.environ:
                del os.environ["PYTEST_XDIST_WORKER"]
            elif original_env is not None:
                os.environ["PYTEST_XDIST_WORKER"] = original_env

    def test_pytest_runtest_call_sql_test(self):
        """Test pytest_runtest_call hook with SQL test."""
        from sql_testing_library._pytest_plugin import pytest_runtest_call

        # Mock SQL test item
        mock_item = mock.Mock()
        mock_function = mock.Mock()
        mock_function._sql_test_decorated = True
        mock_function.return_value = [{"result": 1}]
        mock_item.function = mock_function

        # Call the hook
        pytest_runtest_call(mock_item)

        # Verify function was executed and results stored
        mock_function.assert_called_once()
        self.assertEqual(mock_item._sql_test_results, [{"result": 1}])

    def test_pytest_runtest_call_regular_test(self):
        """Test pytest_runtest_call hook with regular test."""
        from sql_testing_library._pytest_plugin import pytest_runtest_call

        # Mock regular test item without _sql_test_decorated attribute
        mock_item = mock.Mock()
        mock_item.function = None  # No function attribute for non-SQL tests

        # Call the hook
        pytest_runtest_call(mock_item)

        # Verify default runtest was called
        mock_item.runtest.assert_called_once()

    def test_pytest_runtest_call_sql_test_error(self):
        """Test pytest_runtest_call hook with SQL test error."""
        from sql_testing_library._pytest_plugin import pytest_runtest_call

        # Mock SQL test item that raises an error
        mock_item = mock.Mock()
        mock_function = mock.Mock()
        mock_function._sql_test_decorated = True
        mock_function.side_effect = ValueError("Test error")
        mock_item.function = mock_function

        # Call the hook and expect AssertionError
        with self.assertRaises(AssertionError) as context:
            pytest_runtest_call(mock_item)

        self.assertIn("SQL test failed: Test error", str(context.exception))


class TestSQLTestDecoratorAdditionalCoverage(unittest.TestCase):
    """Additional tests to improve coverage."""

    def setUp(self):
        """Set up test decorator."""
        self.decorator = SQLTestDecorator()

    def test_get_framework_no_adapter_type_uses_default(self):
        """Test get_framework with no adapter_type uses default from config."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "bigquery"}
        mock_config["sql_testing.bigquery"] = {
            "project_id": "test-project",
            "dataset_id": "test-dataset",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.bigquery.BigQueryAdapter"):
            framework = self.decorator.get_framework()
            self.assertIsNotNone(framework)

    def test_get_framework_no_adapter_type_with_explicit_adapter_type(self):
        """Test get_framework with explicit adapter_type parameter."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "bigquery"}
        mock_config["sql_testing.athena"] = {
            "database": "test_db",
            "s3_output_location": "s3://test-bucket/",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.athena.AthenaAdapter"):
            framework = self.decorator.get_framework("athena")
            self.assertIsNotNone(framework)

    def test_create_framework_athena_success(self):
        """Test successful Athena framework creation."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "athena"}
        mock_config["sql_testing.athena"] = {
            "database": "test_db",
            "s3_output_location": "s3://test-bucket/",
            "region": "us-west-2",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.athena.AthenaAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("athena")

            mock_adapter.assert_called_once_with(
                database="test_db",
                s3_output_location="s3://test-bucket/",
                region="us-west-2",
                workgroup=None,
                aws_access_key_id="test_key",
                aws_secret_access_key="test_secret",
            )

    def test_create_framework_redshift_success(self):
        """Test successful Redshift framework creation."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "redshift"}
        mock_config["sql_testing.redshift"] = {
            "host": "test-host",
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
            "port": "5439",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.redshift.RedshiftAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("redshift")

            mock_adapter.assert_called_once_with(
                host="test-host",
                database="test_db",
                user="test_user",
                password="test_password",
                port=5439,
            )

    def test_create_framework_snowflake_success(self):
        """Test successful Snowflake framework creation."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "snowflake"}
        mock_config["sql_testing.snowflake"] = {
            "account": "test-account",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
            "warehouse": "test_warehouse",
            "schema": "test_schema",
            "role": "test_role",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.snowflake.SnowflakeAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("snowflake")

            mock_adapter.assert_called_once_with(
                account="test-account",
                user="test_user",
                password="test_password",
                database="test_db",
                schema="test_schema",
                warehouse="test_warehouse",
                role="test_role",
                private_key_path=None,
                private_key_passphrase=None,
            )

    def test_create_framework_trino_success_no_auth(self):
        """Test successful Trino framework creation without auth."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "trino"}
        mock_config["sql_testing.trino"] = {
            "host": "localhost",
            "port": "8080",
            "user": "test_user",
            "catalog": "hive",
            "schema": "test_schema",
            "http_scheme": "https",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.trino.TrinoAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("trino")

            mock_adapter.assert_called_once_with(
                host="localhost",
                port=8080,
                user="test_user",
                catalog="hive",
                schema="test_schema",
                http_scheme="https",
                auth=None,
            )

    def test_create_framework_bigquery_success_absolute_path(self):
        """Test BigQuery framework creation with absolute credentials path."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "bigquery"}
        mock_config["sql_testing.bigquery"] = {
            "project_id": "test-project",
            "dataset_id": "test-dataset",
            "credentials_path": "/absolute/path/creds.json",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.bigquery.BigQueryAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("bigquery")

            mock_adapter.assert_called_once_with(
                project_id="test-project",
                dataset_id="test-dataset",
                credentials_path="/absolute/path/creds.json",
            )

    def test_create_framework_bigquery_success_no_credentials(self):
        """Test BigQuery framework creation without credentials path."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "bigquery"}
        mock_config["sql_testing.bigquery"] = {
            "project_id": "test-project",
            "dataset_id": "test-dataset",
        }

        self.decorator._config_parser = mock_config

        with mock.patch("sql_testing_library._adapters.bigquery.BigQueryAdapter") as mock_adapter:
            self.decorator._create_framework_from_config("bigquery")

            mock_adapter.assert_called_once_with(
                project_id="test-project",
                dataset_id="test-dataset",
                credentials_path=None,
            )

    def test_get_config_parser_with_project_root_change(self):
        """Test config parser when project root changes directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create pytest.ini file
            pytest_ini_path = os.path.join(temp_dir, "pytest.ini")
            with open(pytest_ini_path, "w") as f:
                f.write(
                    """
[sql_testing]
adapter = bigquery
                """
                )

            original_cwd = os.getcwd()
            try:
                # Clear cached values
                self.decorator._config_parser = None
                self.decorator._project_root = None

                # Mock _get_project_root to return our temp directory
                with mock.patch.object(self.decorator, "_get_project_root", return_value=temp_dir):
                    # Mock os.chdir and os.getcwd to simulate directory changes
                    with mock.patch("os.chdir") as mock_chdir:
                        with mock.patch("os.getcwd", return_value=original_cwd):
                            # Mock os.path.exists to return True for pytest.ini in temp_dir
                            def mock_exists(path):
                                if path == "pytest.ini":
                                    return True
                                return os.path.exists(path)

                            with mock.patch("os.path.exists", side_effect=mock_exists):
                                # Mock ConfigParser.read to actually read our file
                                original_read = configparser.ConfigParser.read

                                def mock_read(self, filenames, encoding=None):
                                    if isinstance(filenames, str) and filenames == "pytest.ini":
                                        return original_read(self, [pytest_ini_path], encoding)
                                    return original_read(self, filenames, encoding)

                                with mock.patch.object(
                                    configparser.ConfigParser, "read", mock_read
                                ):
                                    config_parser = self.decorator._get_config_parser()
                                    self.assertIn("sql_testing", config_parser)

                                    # Verify chdir was called to go to project root and back
                                    self.assertTrue(mock_chdir.called)
            finally:
                # Ensure we're in the original directory
                os.chdir(original_cwd)

    def test_project_root_detection_traversal(self):
        """Test project root detection traverses up directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create nested directory structure
            nested_dir = os.path.join(temp_dir, "level1", "level2")
            os.makedirs(nested_dir)

            # Create pyproject.toml in root
            pyproject_path = os.path.join(temp_dir, "pyproject.toml")
            with open(pyproject_path, "w") as f:
                f.write("[tool.poetry]\nname = 'test'\n")

            # Clear cached project root
            self.decorator._project_root = None

            # Start from nested directory
            with mock.patch("os.getcwd", return_value=nested_dir):
                root = self.decorator._get_project_root()
                self.assertEqual(root, temp_dir)

    def test_project_root_detection_invalid_env_var(self):
        """Test project root detection with invalid environment variable."""
        with mock.patch.dict(os.environ, {"SQL_TESTING_PROJECT_ROOT": "/nonexistent/path"}):
            # Clear cached project root
            self.decorator._project_root = None

            with tempfile.TemporaryDirectory() as temp_dir:
                with mock.patch("os.getcwd", return_value=temp_dir):
                    root = self.decorator._get_project_root()
                    # Should fall back to current directory
                    self.assertEqual(root, temp_dir)

    def test_config_caching(self):
        """Test that config is cached after first load."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "bigquery"}

        self.decorator._config_parser = mock_config

        # First call should load and cache
        config1 = self.decorator._load_config()

        # Second call should return cached version
        config2 = self.decorator._load_config()

        self.assertEqual(config1, config2)
        self.assertIs(config1, config2)

    def test_load_adapter_config_no_adapter_type_provided(self):
        """Test loading adapter config when no adapter_type is provided."""
        mock_config = configparser.ConfigParser()
        mock_config["sql_testing"] = {"adapter": "athena"}
        mock_config["sql_testing.athena"] = {"database": "test_db"}

        self.decorator._config_parser = mock_config
        self.decorator._config = dict(mock_config["sql_testing"])

        # Call without providing adapter_type
        adapter_config = self.decorator._load_adapter_config()

        self.assertEqual(adapter_config["database"], "test_db")
