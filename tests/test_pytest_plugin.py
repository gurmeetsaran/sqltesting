"""Tests for the pytest_plugin configuration loading."""

import configparser
import os
import tempfile
import unittest
from unittest import mock

import pytest

from sql_testing_library.pytest_plugin import SQLTestDecorator


class TestPytestPluginConfig(unittest.TestCase):
    """Tests for the configuration loading in pytest_plugin."""

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
                f.write("""
[sql_testing]
adapter = bigquery

[sql_testing.bigquery]
project_id = test-project
dataset_id = test_dataset
credentials_path = /path/to/credentials.json
                """)

            # Create SQLTestDecorator instance
            decorator = SQLTestDecorator()

            # Mock _get_project_root to return our temp directory
            with mock.patch.object(
                decorator, "_get_project_root", return_value=temp_dir
            ):
                # First call should parse the file
                config_parser1 = decorator._get_config_parser()
                assert "sql_testing" in config_parser1
                assert "sql_testing.bigquery" in config_parser1

                # Change the file (this shouldn't affect the cached parser)
                with open(pytest_ini_path, "w") as f:
                    f.write("""
[sql_testing]
adapter = athena
                    """)

                # Second call should return the cached parser
                config_parser2 = decorator._get_config_parser()

                # Should still have the original sections
                assert "sql_testing.bigquery" in config_parser2
                assert config_parser2["sql_testing"]["adapter"] == "bigquery"

                # Verify it's the same object
                assert config_parser1 is config_parser2
