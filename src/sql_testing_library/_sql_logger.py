"""SQL logging functionality for test cases."""

import os
import re
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlglot import parse_one

from ._mock_table import BaseMockTable


class SQLLogger:
    """Handles SQL logging for test cases."""

    # Class variable to store the run directory for the current test session
    _run_directory: Optional[Path] = None
    _run_id: Optional[str] = None
    _lock = threading.Lock()  # Thread lock for concurrent access

    def __init__(self, log_dir: Optional[str] = None) -> None:
        """Initialize SQL logger.

        Args:
            log_dir: Directory to store SQL log files. If None, uses .sql_logs in project root.
        """
        if log_dir is None:
            # Check environment variable first
            env_log_dir = os.environ.get("SQL_TEST_LOG_DIR")
            if env_log_dir:
                self.log_dir = Path(env_log_dir)
            else:
                # Try to find the project root by looking for specific project files
                current_path = Path.cwd()

                # Look for definitive project root markers (in order of preference)
                # These are files that typically only exist at project root
                root_markers = ["pyproject.toml", "setup.py", "setup.cfg", "tox.ini"]

                # Search up the directory tree for project root
                project_root = None
                search_path = current_path

                while search_path != search_path.parent:
                    # Check for root markers
                    if any((search_path / marker).exists() for marker in root_markers):
                        project_root = search_path
                        break

                    # Also check for .git directory (but not .git file which could be a submodule)
                    if (search_path / ".git").is_dir():
                        project_root = search_path
                        break

                    search_path = search_path.parent

                # If we found a project root, use it; otherwise fall back to current directory
                if project_root:
                    self.log_dir = project_root / ".sql_logs"
                else:
                    # Fall back to current directory if project root not found
                    self.log_dir = Path(".sql_logs")
        else:
            self.log_dir = Path(log_dir)

        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._logged_files: List[str] = []

    def _get_worker_id(self) -> Optional[str]:
        """Get pytest-xdist worker ID if running in parallel.

        Returns:
            Worker ID (e.g., 'gw0', 'gw1') or None if running serially
        """
        # Check for pytest-xdist worker ID in environment
        worker_id = os.environ.get("PYTEST_XDIST_WORKER")
        if worker_id:
            return worker_id

        # Alternative: Check for pytest-xdist worker input (for older versions)
        # This would be set in conftest.py if needed
        return os.environ.get("PYTEST_CURRENT_TEST_WORKER")

    def _ensure_run_directory(self) -> Path:
        """Ensure run directory exists, creating it if necessary.

        Returns:
            Path to the run directory
        """
        with SQLLogger._lock:
            # Create run directory if not already created for this session
            if SQLLogger._run_directory is None:
                # Generate run ID with timestamp
                timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

                # Check if running in parallel mode
                worker_id = self._get_worker_id()
                if worker_id:
                    # Include worker ID in run directory name for parallel execution
                    SQLLogger._run_id = f"runid_{timestamp}_{worker_id}"
                else:
                    # Serial execution - use standard run ID
                    SQLLogger._run_id = f"runid_{timestamp}"

                SQLLogger._run_directory = self.log_dir / SQLLogger._run_id
                SQLLogger._run_directory.mkdir(parents=True, exist_ok=True)
        return SQLLogger._run_directory

    def should_log(self, log_sql: Optional[bool] = None) -> bool:
        """Determine if SQL should be logged based on environment and parameters.

        Args:
            log_sql: Explicit parameter from test case

        Returns:
            True if SQL should be logged
        """
        # If explicitly set in test case, use that
        if log_sql is not None:
            return log_sql

        # Check environment variable
        return os.environ.get("SQL_TEST_LOG_ALL", "").lower() in ("true", "1", "yes")

    def generate_filename(
        self,
        test_name: str,
        test_class: Optional[str] = None,
        test_file: Optional[str] = None,
        failed: bool = False,
    ) -> str:
        """Generate a unique filename for the SQL log.

        Args:
            test_name: Name of the test function
            test_class: Name of the test class (if any)
            test_file: Path to the test file
            failed: Whether the test failed

        Returns:
            Generated filename
        """
        # Clean test name for filesystem (including square brackets)
        clean_name = re.sub(r'[<>:"/\\|?*\[\]]', "_", test_name)

        # Build filename components
        components = []

        # Add test file name (without path and extension)
        if test_file:
            file_base = Path(test_file).stem
            components.append(file_base)

        # Add class name if present
        if test_class:
            clean_class = re.sub(r'[<>:"/\\|?*\[\]]', "_", test_class)
            components.append(clean_class)

        # Add test name
        components.append(clean_name)

        # Add status indicator
        if failed:
            components.append("FAILED")

        # Add worker ID if running in parallel
        worker_id = self._get_worker_id()
        if worker_id:
            components.append(f"w{worker_id}")

        # Add timestamp for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Milliseconds
        components.append(timestamp)

        # Add a short UUID suffix to absolutely guarantee uniqueness
        # This handles edge cases where tests might start at exactly the same millisecond
        unique_suffix = str(uuid.uuid4())[:8]
        components.append(unique_suffix)

        # Join with double underscore for clarity
        filename = "__".join(components) + ".sql"

        return filename

    def format_sql(self, sql: str, dialect: Optional[str] = None) -> str:
        """Format SQL query for better readability.

        Args:
            sql: SQL query to format
            dialect: SQL dialect (e.g., 'bigquery', 'athena')

        Returns:
            Formatted SQL
        """
        try:
            # Parse and format using sqlglot
            parsed = parse_one(sql, dialect=dialect)
            # IMPORTANT: Pass dialect to sql() to preserve dialect-specific syntax
            formatted = parsed.sql(pretty=True, pad=2, dialect=dialect)
            return formatted
        except Exception:
            # If formatting fails, return original
            return sql

    def create_metadata_header(
        self,
        test_name: str,
        test_class: Optional[str] = None,
        test_file: Optional[str] = None,
        query: str = "",
        default_namespace: Optional[str] = None,
        mock_tables: Optional[List[BaseMockTable]] = None,
        adapter_type: Optional[str] = None,
        use_physical_tables: bool = False,
        execution_time: Optional[float] = None,
        row_count: Optional[int] = None,
        error: Optional[str] = None,
        error_traceback: Optional[str] = None,
        temp_table_queries: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> str:
        """Create a metadata header for the SQL file.

        Returns:
            Formatted metadata header as SQL comments
        """
        lines = [
            "-- SQL Test Case Log",
            "-- " + "=" * 78,
            f"-- Generated: {datetime.now().isoformat()}",
            f"-- Run ID: {SQLLogger._run_id}",
            f"-- Test Name: {test_name}",
        ]

        if test_class:
            lines.append(f"-- Test Class: {test_class}")

        if test_file:
            lines.append(f"-- Test File: {test_file}")

        if adapter_type:
            lines.append(f"-- Adapter: {adapter_type}")

        # Show adapter name if different from sqlglot dialect
        adapter_name = kwargs.get("adapter_name")
        if adapter_name and adapter_name != adapter_type:
            lines.append(f"-- Database: {adapter_name}")

        if default_namespace:
            lines.append(f"-- Default Namespace: {default_namespace}")

        lines.append(f"-- Use Physical Tables: {use_physical_tables}")

        if execution_time is not None:
            lines.append(f"-- Execution Time: {execution_time:.3f} seconds")

        if row_count is not None:
            lines.append(f"-- Result Rows: {row_count}")

        if error:
            lines.extend(
                [
                    "-- Status: FAILED",
                    "-- Error:",
                ]
            )
            for line in error.strip().split("\n"):
                lines.append(f"-- {line}")

            # Add full error traceback if available
            if error_traceback:
                lines.extend(
                    [
                        "",
                        "-- Full Error Details:",
                        "-- " + "-" * 78,
                    ]
                )
                # Add each line of the traceback as a SQL comment
                for line in error_traceback.strip().split("\n"):
                    lines.append(f"-- {line}")
        else:
            lines.append("-- Status: SUCCESS")

        # Add mock tables information
        if mock_tables:
            lines.extend(
                [
                    "",
                    "-- Mock Tables:",
                    "-- " + "-" * 78,
                ]
            )
            for table in mock_tables:
                lines.append(f"-- Table: {table.get_table_name()}")
                # Get row count from data
                if hasattr(table, "data") and table.data:
                    lines.append(f"--   Rows: {len(table.data)}")
                # Get column names from first row or column types
                if hasattr(table, "get_column_types"):
                    columns = list(table.get_column_types().keys())
                    if columns:
                        lines.append(f"--   Columns: {', '.join(columns)}")

        # Add original query
        lines.extend(
            [
                "",
                "-- Original Query:",
                "-- " + "-" * 78,
            ]
        )
        # Comment out each line of the original query
        for line in query.split("\n"):
            lines.append(f"-- {line}")

        # Add temp table queries if physical tables were used
        if use_physical_tables and temp_table_queries:
            lines.extend(
                [
                    "",
                    "-- Temporary Table Creation Queries:",
                    "-- " + "-" * 78,
                    "",
                ]
            )
            for i, temp_query in enumerate(temp_table_queries, 1):
                lines.append(f"-- Query {i}:")
                lines.append("")
                # Format the temp table SQL
                formatted_temp_sql = self.format_sql(temp_query, dialect=adapter_type)
                lines.append(formatted_temp_sql)
                lines.append("")

        lines.extend(
            [
                "",
                "-- Transformed Query:",
                "-- " + "=" * 78,
                "",
            ]
        )

        return "\n".join(lines)

    def log_sql(
        self,
        sql: str,
        test_name: str,
        test_class: Optional[str] = None,
        test_file: Optional[str] = None,
        failed: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Log SQL to a file and return the file path.

        Args:
            sql: The transformed SQL query to log
            test_name: Name of the test
            test_class: Test class name
            test_file: Test file path
            failed: Whether the test failed
            metadata: Additional metadata to include

        Returns:
            Path to the created SQL file
        """
        # Generate filename
        filename = self.generate_filename(test_name, test_class, test_file, failed)

        # Ensure run directory exists (lazy creation)
        run_directory = self._ensure_run_directory()
        filepath = run_directory / filename

        # Prepare metadata
        if metadata is None:
            metadata = {}

        # Create header
        header = self.create_metadata_header(
            test_name=test_name, test_class=test_class, test_file=test_file, **metadata
        )

        # Format SQL
        dialect = metadata.get("adapter_type")
        formatted_sql = self.format_sql(sql, dialect)

        # Write to file
        content = header + formatted_sql
        filepath.write_text(content, encoding="utf-8")

        # Track logged file
        self._logged_files.append(str(filepath))

        # Return absolute path for clickable URLs
        return str(filepath.absolute())

    def get_logged_files(self) -> List[str]:
        """Get list of files logged in this session."""
        return self._logged_files.copy()

    def clear_logged_files(self) -> None:
        """Clear the list of logged files."""
        self._logged_files = []

    @classmethod
    def get_run_directory(cls) -> Optional[Path]:
        """Get the current run directory."""
        return cls._run_directory

    @classmethod
    def get_run_id(cls) -> Optional[str]:
        """Get the current run ID."""
        return cls._run_id

    @classmethod
    def reset_run_directory(cls) -> None:
        """Reset the run directory (useful for testing)."""
        cls._run_directory = None
        cls._run_id = None
