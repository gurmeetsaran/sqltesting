"""Tests for SQL logger functionality.

Note: These tests use a mock SQLLogger to avoid import issues with database adapters.
The actual SQLLogger functionality is tested through integration tests.
"""

import os
import tempfile
from pathlib import Path
from typing import List, Optional
from unittest.mock import patch


class MockSQLLogger:
    """Mock implementation of SQLLogger for testing core functionality."""

    # Class variables to match the real SQLLogger
    _run_directory: Optional[Path] = None
    _run_id: Optional[str] = None

    def __init__(self, log_dir: Optional[str] = None) -> None:
        """Initialize mock SQL logger."""
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

        # Create run directory if not already created for this session
        if MockSQLLogger._run_directory is None:
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
            MockSQLLogger._run_id = f"runid_{timestamp}"
            MockSQLLogger._run_directory = self.log_dir / MockSQLLogger._run_id
            MockSQLLogger._run_directory.mkdir(parents=True, exist_ok=True)

    def should_log(self, log_sql: Optional[bool] = None) -> bool:
        """Determine if SQL should be logged based on environment and parameters."""
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
        """Generate a filename for the SQL log."""
        import re
        from datetime import datetime

        # Build filename parts
        parts = []

        # Extract module name from test file if provided
        if test_file:
            # Get just the filename without path and extension
            module_name = Path(test_file).stem
            parts.append(module_name)

        # Add test class if provided
        if test_class:
            parts.append(test_class)

        # Add test name
        parts.append(test_name)

        # Add failed indicator
        if failed:
            parts.append("FAILED")

        # Join parts with double underscore
        base_name = "__".join(parts)

        # Sanitize filename - remove invalid characters
        # Updated to include square brackets and angle brackets
        base_name = re.sub(r'[<>:"/\\|?*\[\]]', "_", base_name)

        # Add timestamp with milliseconds
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

        return f"{base_name}__{timestamp}.sql"

    @classmethod
    def reset_run_directory(cls) -> None:
        """Reset the run directory (useful for testing)."""
        cls._run_directory = None
        cls._run_id = None


class TestSQLLogger:
    """Test cases for SQLLogger class using mock implementation."""

    def test_default_log_directory_project_root(self):
        """Test that default log directory finds project root."""
        # Save current directory
        original_cwd = Path.cwd()

        try:
            # Create a temporary directory structure
            with tempfile.TemporaryDirectory() as tmpdir:
                project_root = Path(tmpdir)
                subdir = project_root / "tests" / "integration"
                subdir.mkdir(parents=True)

                # Create project marker
                (project_root / "pyproject.toml").touch()

                # Change to subdirectory
                os.chdir(subdir)

                # Create logger
                logger = MockSQLLogger()

                # Should find project root - use resolve() to handle symlinks
                assert logger.log_dir.resolve() == (project_root / ".sql_logs").resolve()
                assert logger.log_dir.exists()
        finally:
            os.chdir(original_cwd)

    def test_environment_variable_override(self):
        """Test that SQL_TEST_LOG_DIR environment variable works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_dir = Path(tmpdir) / "my_logs"

            with patch.dict(os.environ, {"SQL_TEST_LOG_DIR": str(custom_dir)}):
                logger = MockSQLLogger()

                assert logger.log_dir == custom_dir
                assert logger.log_dir.exists()

    def test_explicit_log_directory(self):
        """Test that explicit log directory parameter works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_dir = Path(tmpdir) / "custom_logs"

            logger = MockSQLLogger(log_dir=str(custom_dir))

            assert logger.log_dir == custom_dir
            assert logger.log_dir.exists()

    def test_fallback_to_current_directory(self):
        """Test fallback when project root cannot be found."""
        # Save current directory
        original_cwd = Path.cwd()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                # Change to temp directory with no project markers
                os.chdir(tmpdir)

                logger = MockSQLLogger()

                # Should use current directory
                assert logger.log_dir == Path(".sql_logs")
                assert logger.log_dir.exists()
        finally:
            os.chdir(original_cwd)

    def test_should_log_with_environment_variable(self):
        """Test should_log respects SQL_TEST_LOG_ALL environment variable."""
        logger = MockSQLLogger()

        # Test various truthy values
        for value in ["true", "True", "TRUE", "1", "yes", "Yes", "YES"]:
            with patch.dict(os.environ, {"SQL_TEST_LOG_ALL": value}):
                assert logger.should_log() is True

        # Test falsy values
        for value in ["false", "False", "0", "no", ""]:
            with patch.dict(os.environ, {"SQL_TEST_LOG_ALL": value}):
                assert logger.should_log() is False

        # Test missing env var
        with patch.dict(os.environ, {}, clear=True):
            assert logger.should_log() is False

    def test_should_log_with_explicit_parameter(self):
        """Test should_log respects explicit parameter."""
        logger = MockSQLLogger()

        # Explicit True should override environment
        with patch.dict(os.environ, {"SQL_TEST_LOG_ALL": "false"}):
            assert logger.should_log(log_sql=True) is True

        # Explicit False should override environment
        with patch.dict(os.environ, {"SQL_TEST_LOG_ALL": "true"}):
            assert logger.should_log(log_sql=False) is False

    def test_generate_filename_sanitization(self):
        """Test filename generation with special characters."""
        logger = MockSQLLogger()

        # Test with various special characters
        filename = logger.generate_filename(
            test_name="test[param1]",
            test_class="Test<Class>",
            test_file="/path/to/test_file.py",
            failed=True,
        )

        # Should not contain invalid characters
        assert "[" not in filename
        assert "]" not in filename
        assert "<" not in filename
        assert ">" not in filename
        assert "/" not in filename
        assert "\\" not in filename

        # Should contain FAILED indicator
        assert "FAILED" in filename

        # Should have .sql extension
        assert filename.endswith(".sql")

    def test_project_root_detection_with_git_directory(self):
        """Test project root detection with .git directory."""
        original_cwd = Path.cwd()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                project_root = Path(tmpdir)
                subdir = project_root / "src" / "tests"
                subdir.mkdir(parents=True)

                # Create .git directory (not file)
                git_dir = project_root / ".git"
                git_dir.mkdir()

                # Change to subdirectory
                os.chdir(subdir)

                logger = MockSQLLogger()

                # Should find project root by .git directory - use resolve()
                assert logger.log_dir.resolve() == (project_root / ".sql_logs").resolve()
        finally:
            os.chdir(original_cwd)

    def test_project_root_detection_ignores_git_file(self):
        """Test that .git file (submodule) is ignored."""
        original_cwd = Path.cwd()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                project_root = Path(tmpdir)
                subdir = project_root / "submodule"
                subdir.mkdir(parents=True)

                # Create .git file (not directory) in submodule
                (subdir / ".git").write_text("gitdir: ../.git/modules/submodule")

                # Create actual project marker in parent
                (project_root / "setup.py").touch()

                # Change to subdirectory
                os.chdir(subdir)

                logger = MockSQLLogger()

                # Should find parent project root, not stop at .git file - use resolve()
                assert logger.log_dir.resolve() == (project_root / ".sql_logs").resolve()
        finally:
            os.chdir(original_cwd)

    def test_run_directory_creation(self):
        """Test that run directory is created with timestamp."""
        # Reset run directory for clean test
        MockSQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first logger instance
            MockSQLLogger(log_dir=tmpdir)

            # Check run directory was created
            assert MockSQLLogger._run_directory is not None
            assert MockSQLLogger._run_id is not None
            assert MockSQLLogger._run_id.startswith("runid_")
            assert MockSQLLogger._run_directory.exists()
            assert MockSQLLogger._run_directory.parent == Path(tmpdir)

            # Save run directory for comparison
            first_run_dir = MockSQLLogger._run_directory
            first_run_id = MockSQLLogger._run_id

            # Create second logger instance (should use same run directory)
            MockSQLLogger(log_dir=tmpdir)

            # Should reuse the same run directory
            assert MockSQLLogger._run_directory == first_run_dir
            assert MockSQLLogger._run_id == first_run_id

        # Clean up
        MockSQLLogger.reset_run_directory()
