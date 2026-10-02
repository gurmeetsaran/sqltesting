from pathlib import Path

import pytest

from sql_testing_library._sql_logger import SQLLogger


def test_loggers_keep_separate_roots_in_one_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(SQLLogger, "_run_directory", None)
    monkeypatch.setattr(SQLLogger, "_run_id", None)
    first = SQLLogger(str(tmp_path / "first"))
    second = SQLLogger(str(tmp_path / "second"))

    first_file = Path(first.log_sql("SELECT 1", "first"))
    second_file = Path(second.log_sql("SELECT 2", "second"))
    first_run = first_file.parent
    second_run = second_file.parent

    assert first_run.parent == first.log_dir
    assert second_run.parent == second.log_dir
    assert first_run.name == second_run.name == SQLLogger.get_run_id()
    assert first_run.is_dir() and second_run.is_dir()
    assert "SELECT" in first_file.read_text(encoding="utf-8")
    assert "SELECT" in second_file.read_text(encoding="utf-8")
    assert Path(first.log_sql("SELECT 3", "first_again")).parent == first_run


def test_environment_log_root_changes_are_honored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(SQLLogger, "_run_directory", None)
    monkeypatch.setattr(SQLLogger, "_run_id", None)
    monkeypatch.setenv("SQL_TEST_LOG_DIR", str(tmp_path / "first"))
    first = Path(SQLLogger().log_sql("SELECT 1", "first"))
    monkeypatch.setenv("SQL_TEST_LOG_DIR", str(tmp_path / "second"))
    second = Path(SQLLogger().log_sql("SELECT 2", "second"))

    assert first.parent.parent == tmp_path / "first"
    assert second.parent.parent == tmp_path / "second"
    assert first.parent.name == second.parent.name
