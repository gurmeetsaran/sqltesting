"""Comprehensive tests for SQLLogger functionality using the real implementation."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from sql_testing_library._sql_logger import SQLLogger


class TestSQLLoggerReal:
    """Test cases for real SQLLogger implementation."""

    def setup_method(self):
        """Reset SQLLogger class state before each test."""
        SQLLogger.reset_run_directory()

    def teardown_method(self):
        """Clean up after each test."""
        SQLLogger.reset_run_directory()

    def test_get_worker_id_with_pytest_xdist(self):
        """Test _get_worker_id with pytest-xdist environment variable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Test with PYTEST_XDIST_WORKER
            # Clear both possible env vars first, then set the one we want to test
            with patch.dict(
                os.environ,
                {
                    "PYTEST_XDIST_WORKER": "gw0",
                    "PYTEST_CURRENT_TEST_WORKER": "",
                },
                clear=False,
            ):
                # Remove PYTEST_CURRENT_TEST_WORKER if it exists
                os.environ.pop("PYTEST_CURRENT_TEST_WORKER", None)
                worker_id = logger._get_worker_id()
                assert worker_id == "gw0"

            # Test with PYTEST_CURRENT_TEST_WORKER (fallback)
            # Clear PYTEST_XDIST_WORKER so fallback is used
            with patch.dict(
                os.environ,
                {"PYTEST_CURRENT_TEST_WORKER": "worker1"},
                clear=False,
            ):
                # Remove PYTEST_XDIST_WORKER to test the fallback
                os.environ.pop("PYTEST_XDIST_WORKER", None)
                worker_id = logger._get_worker_id()
                assert worker_id == "worker1"

            # Test without any worker env vars
            with patch.dict(os.environ, {}, clear=True):
                worker_id = logger._get_worker_id()
                assert worker_id is None

    def test_ensure_run_directory_serial_execution(self):
        """Test run directory creation in serial execution."""
        SQLLogger.reset_run_directory()  # Ensure clean state

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Mock _get_worker_id to return None (serial execution)
            with patch.object(logger, "_get_worker_id", return_value=None):
                run_dir = logger._ensure_run_directory()

                # Verify run directory created
                assert run_dir.exists()
                assert run_dir.parent == Path(tmpdir)
                assert SQLLogger._run_id is not None
                assert SQLLogger._run_id.startswith("runid_")
                assert "gw" not in SQLLogger._run_id  # No worker ID

    def test_ensure_run_directory_parallel_execution(self):
        """Test run directory creation in parallel execution."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Mock _get_worker_id to return a worker ID
            with patch.object(logger, "_get_worker_id", return_value="gw0"):
                run_dir = logger._ensure_run_directory()

                # Verify run directory created with worker ID
                assert run_dir.exists()
                assert SQLLogger._run_id is not None
                assert "gw0" in SQLLogger._run_id

    def test_ensure_run_directory_shared_across_instances(self):
        """Test that run directory is shared across multiple logger instances."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger1 = SQLLogger(log_dir=tmpdir)
            run_dir1 = logger1._ensure_run_directory()
            run_id1 = SQLLogger._run_id

            # Create second logger
            logger2 = SQLLogger(log_dir=tmpdir)
            run_dir2 = logger2._ensure_run_directory()
            run_id2 = SQLLogger._run_id

            # Should be the same
            assert run_dir1 == run_dir2
            assert run_id1 == run_id2

    def test_format_sql_success(self):
        """Test SQL formatting with valid SQL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Test simple query
            sql = "SELECT * FROM users WHERE id = 1"
            formatted = logger.format_sql(sql)
            assert "SELECT" in formatted
            assert "FROM" in formatted
            assert "users" in formatted

    def test_format_sql_with_dialect(self):
        """Test SQL formatting with specific dialect."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Test with BigQuery dialect
            sql = "SELECT * FROM `project.dataset.table`"
            formatted = logger.format_sql(sql, dialect="bigquery")
            assert "SELECT" in formatted

    def test_format_sql_invalid_sql(self):
        """Test SQL formatting with invalid SQL returns original."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Invalid SQL should return original
            invalid_sql = "THIS IS NOT SQL AT ALL!!!"
            formatted = logger.format_sql(invalid_sql)
            assert formatted == invalid_sql

    def test_create_metadata_header_basic(self):
        """Test metadata header creation with basic info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()  # Ensure run_id is set

            header = logger.create_metadata_header(test_name="test_example", query="SELECT 1")

            # Verify header contains basic information
            assert "SQL Test Case Log" in header
            assert "test_example" in header
            assert "SELECT 1" in header
            assert "Original Query:" in header
            assert "Transformed Query:" in header

    def test_create_metadata_header_with_test_class(self):
        """Test metadata header with test class."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                test_class="TestMyClass",
                query="SELECT 1",
            )

            assert "Test Class: TestMyClass" in header

    def test_create_metadata_header_with_test_file(self):
        """Test metadata header with test file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                test_file="/path/to/test_file.py",
                query="SELECT 1",
            )

            assert "Test File: /path/to/test_file.py" in header

    def test_create_metadata_header_with_adapter_type(self):
        """Test metadata header with adapter type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                adapter_type="bigquery",
                query="SELECT 1",
            )

            assert "Adapter: bigquery" in header

    def test_create_metadata_header_with_adapter_name(self):
        """Test metadata header with adapter name different from type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                adapter_type="postgres",
                adapter_name="PostgreSQL 14",
                query="SELECT 1",
            )

            assert "Database: PostgreSQL 14" in header

    def test_create_metadata_header_with_namespace(self):
        """Test metadata header with default namespace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                default_namespace="my_schema",
                query="SELECT 1",
            )

            assert "Default Namespace: my_schema" in header

    def test_create_metadata_header_with_execution_time(self):
        """Test metadata header with execution time."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                execution_time=1.234,
                query="SELECT 1",
            )

            assert "Execution Time: 1.234 seconds" in header

    def test_create_metadata_header_with_row_count(self):
        """Test metadata header with row count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example", row_count=42, query="SELECT 1"
            )

            assert "Result Rows: 42" in header

    def test_create_metadata_header_with_error(self):
        """Test metadata header with error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                error="Something went wrong",
                query="SELECT 1",
            )

            assert "Status: FAILED" in header
            assert "Something went wrong" in header

    def test_create_metadata_header_with_error_traceback(self):
        """Test metadata header with error and traceback."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            traceback = "Traceback (most recent call last):\n  File 'test.py', line 10"
            header = logger.create_metadata_header(
                test_name="test_example",
                error="Error occurred",
                error_traceback=traceback,
                query="SELECT 1",
            )

            assert "Full Error Details:" in header
            assert "Traceback" in header

    def test_create_metadata_header_success_status(self):
        """Test metadata header shows success when no error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(test_name="test_example", query="SELECT 1")

            assert "Status: SUCCESS" in header

    def test_create_metadata_header_with_mock_tables(self):
        """Test metadata header with mock tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            # Create mock table objects
            mock_table1 = MagicMock()
            mock_table1.get_table_name.return_value = "users"
            mock_table1.data = [{"id": 1}, {"id": 2}]
            mock_table1.get_column_types.return_value = {"id": "INTEGER", "name": "VARCHAR"}

            mock_table2 = MagicMock()
            mock_table2.get_table_name.return_value = "orders"
            mock_table2.data = [{"order_id": 1}]
            mock_table2.get_column_types.return_value = {"order_id": "INTEGER"}

            header = logger.create_metadata_header(
                test_name="test_example",
                mock_tables=[mock_table1, mock_table2],
                query="SELECT * FROM users",
            )

            assert "Mock Tables:" in header
            assert "Table: users" in header
            assert "Rows: 2" in header
            assert "Columns: id, name" in header
            assert "Table: orders" in header
            assert "Rows: 1" in header

    def test_create_metadata_header_with_physical_tables(self):
        """Test metadata header with physical tables flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            header = logger.create_metadata_header(
                test_name="test_example",
                use_physical_tables=True,
                query="SELECT 1",
            )

            assert "Use Physical Tables: True" in header

    def test_create_metadata_header_with_temp_table_queries(self):
        """Test metadata header with temp table queries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            temp_queries = [
                "CREATE TEMP TABLE t1 AS SELECT 1",
                "CREATE TEMP TABLE t2 AS SELECT 2",
            ]

            header = logger.create_metadata_header(
                test_name="test_example",
                use_physical_tables=True,
                temp_table_queries=temp_queries,
                adapter_type="postgres",
                query="SELECT * FROM t1 JOIN t2",
            )

            assert "Temporary Table Creation Queries:" in header
            assert "Query 1:" in header
            assert "Query 2:" in header
            assert "CREATE" in header

    def test_log_sql_basic(self):
        """Test basic SQL logging."""
        SQLLogger.reset_run_directory()  # Ensure clean state

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()  # Initialize run directory

            sql = "SELECT * FROM users"
            filepath = logger.log_sql(
                sql=sql,
                test_name="test_example",
            )

            # Verify file was created
            assert Path(filepath).exists()
            assert filepath.endswith(".sql")

            # Verify content
            content = Path(filepath).read_text()
            assert "SELECT" in content
            assert "test_example" in content

            # Verify tracked in logged files
            assert filepath in logger.get_logged_files()

    def test_log_sql_with_test_class_and_file(self):
        """Test SQL logging with test class and file."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            filepath = logger.log_sql(
                sql="SELECT 1",
                test_name="test_example",
                test_class="TestMyClass",
                test_file="/path/to/test_file.py",
            )

            # Verify filename contains test file name and class
            assert "test_file" in filepath
            assert "TestMyClass" in filepath

    def test_log_sql_with_failed_flag(self):
        """Test SQL logging with failed flag."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            filepath = logger.log_sql(
                sql="SELECT 1",
                test_name="test_example",
                failed=True,
            )

            # Verify FAILED in filename
            assert "FAILED" in filepath

    def test_log_sql_with_metadata(self):
        """Test SQL logging with metadata."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            metadata: Dict[str, Any] = {
                "adapter_type": "bigquery",
                "execution_time": 2.5,
                "row_count": 100,
                "default_namespace": "my_dataset",
            }

            filepath = logger.log_sql(
                sql="SELECT * FROM users",
                test_name="test_example",
                metadata=metadata,
            )

            # Verify metadata in file
            content = Path(filepath).read_text()
            assert "Adapter: bigquery" in content
            assert "Execution Time: 2.500 seconds" in content
            assert "Result Rows: 100" in content
            assert "Default Namespace: my_dataset" in content

    def test_get_logged_files(self):
        """Test getting logged files."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            # Log multiple files
            file1 = logger.log_sql(sql="SELECT 1", test_name="test1")
            file2 = logger.log_sql(sql="SELECT 2", test_name="test2")

            logged = logger.get_logged_files()
            assert file1 in logged
            assert file2 in logged
            assert len(logged) == 2

    def test_clear_logged_files(self):
        """Test clearing logged files list."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            # Log a file
            logger.log_sql(sql="SELECT 1", test_name="test1")
            assert len(logger.get_logged_files()) == 1

            # Clear
            logger.clear_logged_files()
            assert len(logger.get_logged_files()) == 0

    def test_get_run_directory(self):
        """Test getting run directory."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            run_dir = SQLLogger.get_run_directory()
            assert run_dir is not None
            assert run_dir.exists()
            assert run_dir.parent == Path(tmpdir)

    def test_get_run_id(self):
        """Test getting run ID."""
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            run_id = SQLLogger.get_run_id()
            assert run_id is not None
            assert run_id.startswith("runid_")

    def test_reset_run_directory(self):
        """Test resetting run directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)
            logger._ensure_run_directory()

            # Verify set
            assert SQLLogger._run_directory is not None
            assert SQLLogger._run_id is not None

            # Reset
            SQLLogger.reset_run_directory()

            # Verify cleared
            assert SQLLogger._run_directory is None
            assert SQLLogger._run_id is None

    def test_generate_filename_with_worker_id(self):
        """Test filename generation includes worker ID in parallel mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Mock worker ID
            with patch.object(logger, "_get_worker_id", return_value="gw2"):
                filename = logger.generate_filename(
                    test_name="test_parallel",
                    test_class="TestClass",
                    test_file="/path/to/test_file.py",
                )

                # Should contain worker ID
                assert "wgw2" in filename

    def test_generate_filename_unique_suffix(self):
        """Test that generated filenames have unique suffixes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = SQLLogger(log_dir=tmpdir)

            # Generate multiple filenames quickly
            filenames = [logger.generate_filename(test_name="test_example") for _ in range(5)]

            # All should be unique
            assert len(filenames) == len(set(filenames))

    def test_threading_safety_ensure_run_directory(self):
        """Test that run directory creation is thread-safe."""
        import threading

        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dirs = []
            run_ids = []

            def create_logger():
                logger = SQLLogger(log_dir=tmpdir)
                run_dir = logger._ensure_run_directory()
                run_dirs.append(run_dir)
                run_ids.append(SQLLogger._run_id)

            # Create multiple threads
            threads = [threading.Thread(target=create_logger) for _ in range(10)]

            # Start all threads
            for t in threads:
                t.start()

            # Wait for all to complete
            for t in threads:
                t.join()

            # All should have the same run directory and ID
            assert len(set(str(d) for d in run_dirs)) == 1
            assert len(set(run_ids)) == 1


def test_sql_logger_default_initialization():
    """Test SQLLogger can be initialized with defaults."""
    # This test ensures the logger can find project root
    logger = SQLLogger()
    assert logger.log_dir is not None
    assert logger.log_dir.exists()


def test_sql_logger_env_variable_for_log_dir():
    """Test SQL_TEST_LOG_DIR environment variable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        custom_dir = Path(tmpdir) / "custom_sql_logs"

        with patch.dict(os.environ, {"SQL_TEST_LOG_DIR": str(custom_dir)}):
            logger = SQLLogger()
            assert logger.log_dir == custom_dir
            assert logger.log_dir.exists()
