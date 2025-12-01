"""Tests for backward compatibility in _core.py."""

import warnings

import pytest

from sql_testing_library._core import SQLTestCase


class TestSQLTestCaseBackwardCompatibility:
    """Test backward compatibility warnings in SQLTestCase."""

    def test_execution_database_deprecated_warning(self):
        """Test deprecation warning when using execution_database."""
        with pytest.warns(DeprecationWarning, match="execution_database.*deprecated"):
            test_case = SQLTestCase(
                query="SELECT 1",
                execution_database="my_db",  # Deprecated parameter
            )

        # Should have copied value to default_namespace
        assert test_case.default_namespace == "my_db"

    def test_both_parameters_provided_warning(self):
        """Test warning when both default_namespace and execution_database provided."""
        with pytest.warns(DeprecationWarning, match="Both.*default_namespace.*execution_database"):
            test_case = SQLTestCase(
                query="SELECT 1",
                default_namespace="new_db",
                execution_database="old_db",  # Should be ignored
            )

        # Should prefer default_namespace
        assert test_case.default_namespace == "new_db"

    def test_neither_parameter_raises_error(self):
        """Test error when neither parameter is provided."""
        with pytest.raises(ValueError, match="Must provide either"):
            SQLTestCase(query="SELECT 1")

    def test_default_namespace_only_no_warning(self):
        """Test no warning when only default_namespace is used (preferred)."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Turn warnings into errors
            test_case = SQLTestCase(
                query="SELECT 1",
                default_namespace="my_db",
            )

        assert test_case.default_namespace == "my_db"

    def test_execution_database_none_default_namespace_set(self):
        """Test execution_database=None with default_namespace set works."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            test_case = SQLTestCase(
                query="SELECT 1",
                default_namespace="my_db",
                execution_database=None,  # Explicitly None
            )

        assert test_case.default_namespace == "my_db"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
