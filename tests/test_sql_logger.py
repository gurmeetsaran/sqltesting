"""Tests for the production SQL logger without connecting to database adapters."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from sql_testing_library._sql_logger import SQLLogger


@pytest.fixture(autouse=True)
def isolate_logger_state(monkeypatch, tmp_path):
    """Keep environment settings, log files, and shared run state local to each test."""
    monkeypatch.delenv("SQL_TEST_LOG_DIR", raising=False)
    monkeypatch.delenv("SQL_TEST_LOG_ALL", raising=False)
    monkeypatch.setattr(SQLLogger, "_run_directory", None)
    monkeypatch.setattr(SQLLogger, "_run_id", None)
    monkeypatch.chdir(tmp_path)


class TestSQLLogger:
    """Test cases against the production SQLLogger implementation."""

    def test_default_log_directory_project_root(self):
        """Test that default log directory finds project root."""
        # Save current directory
        original_cwd = Path.cwd()

        # Create a temporary directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                project_root = Path(tmpdir)
                subdir = project_root / "tests" / "integration"
                subdir.mkdir(parents=True)

                # Create project marker
                (project_root / "pyproject.toml").touch()

                # Change to subdirectory
                os.chdir(subdir)

                # Create logger
                logger = SQLLogger()

                # Should find project root - use resolve() to handle symlinks
                assert logger.log_dir.resolve() == (project_root / ".sql_logs").resolve()
                assert logger.log_dir.exists()
            finally:
                # Always change back before tempdir cleanup
                os.chdir(original_cwd)

    def test_environment_variable_override(self):
        """Test that SQL_TEST_LOG_DIR environment variable works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_dir = Path(tmpdir) / "my_logs"

            with patch.dict(os.environ, {"SQL_TEST_LOG_DIR": str(custom_dir)}):
                logger = SQLLogger()

                assert logger.log_dir == custom_dir
                assert logger.log_dir.exists()

    def test_explicit_log_directory(self, monkeypatch):
        """Test that explicit log directory parameter works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_dir = Path(tmpdir) / "custom_logs"
            env_dir = Path(tmpdir) / "env_logs"
            monkeypatch.setenv("SQL_TEST_LOG_DIR", str(env_dir))

            logger = SQLLogger(log_dir=str(custom_dir))

            assert logger.log_dir == custom_dir
            assert logger.log_dir.exists()
            assert not env_dir.exists()

    def test_fallback_to_current_directory(self):
        """Test fallback when project root cannot be found."""
        # Save current directory
        original_cwd = Path.cwd()

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                # Change to temp directory with no project markers
                os.chdir(tmpdir)

                logger = SQLLogger()

                # Should use current directory
                assert logger.log_dir == Path(".sql_logs")
                assert logger.log_dir.exists()
            finally:
                # Always change back before tempdir cleanup
                os.chdir(original_cwd)

    def test_should_log_with_environment_variable(self):
        """Test should_log respects SQL_TEST_LOG_ALL environment variable."""
        logger = SQLLogger()

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
        logger = SQLLogger()

        # Explicit True should override environment
        with patch.dict(os.environ, {"SQL_TEST_LOG_ALL": "false"}):
            assert logger.should_log(log_sql=True) is True

        # Explicit False should override environment
        with patch.dict(os.environ, {"SQL_TEST_LOG_ALL": "true"}):
            assert logger.should_log(log_sql=False) is False

    def test_generate_filename_sanitization(self):
        """Test filename generation with special characters."""
        logger = SQLLogger()

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

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                project_root = Path(tmpdir)
                subdir = project_root / "src" / "tests"
                subdir.mkdir(parents=True)

                # Create .git directory (not file)
                git_dir = project_root / ".git"
                git_dir.mkdir()

                # Change to subdirectory
                os.chdir(subdir)

                logger = SQLLogger()

                # Should find project root by .git directory - use resolve()
                assert logger.log_dir.resolve() == (project_root / ".sql_logs").resolve()
            finally:
                # Always change back before tempdir cleanup
                os.chdir(original_cwd)

    def test_project_root_detection_ignores_git_file(self):
        """Test that .git file (submodule) is ignored."""
        original_cwd = Path.cwd()

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                project_root = Path(tmpdir)
                subdir = project_root / "submodule"
                subdir.mkdir(parents=True)

                # Create .git file (not directory) in submodule
                (subdir / ".git").write_text("gitdir: ../.git/modules/submodule")

                # Create actual project marker in parent
                (project_root / "setup.py").touch()

                # Change to subdirectory
                os.chdir(subdir)

                logger = SQLLogger()

                # Should find parent project root, not stop at .git file - use resolve()
                assert logger.log_dir.resolve() == (project_root / ".sql_logs").resolve()
            finally:
                # Always change back before tempdir cleanup
                os.chdir(original_cwd)

    def test_run_directory_creation(self):
        """Test that run directory is created with timestamp."""
        # Reset run directory for clean test
        SQLLogger.reset_run_directory()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first logger instance
            logger = SQLLogger(log_dir=tmpdir)
            assert SQLLogger._run_directory is None
            assert SQLLogger._run_id is None
            assert list(Path(tmpdir).iterdir()) == []
            logger._ensure_run_directory()

            # Check run directory was created
            assert SQLLogger._run_directory is not None
            assert SQLLogger._run_id is not None
            assert SQLLogger._run_id.startswith("runid_")
            assert SQLLogger._run_directory.exists()
            assert SQLLogger._run_directory.parent == Path(tmpdir)

            # Save run directory for comparison
            first_run_dir = SQLLogger._run_directory
            first_run_id = SQLLogger._run_id

            # Create second logger instance (should use same run directory)
            SQLLogger(log_dir=tmpdir)

            # Should reuse the same run directory
            assert SQLLogger._run_directory == first_run_dir
            assert SQLLogger._run_id == first_run_id

        # Clean up
        SQLLogger.reset_run_directory()
