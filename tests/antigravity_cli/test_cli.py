"""Tests for AntiGravity Typer CLI entrypoints."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from typer.testing import CliRunner

from tests.antigravity_ingestion.test_parser import (
    encode_bytes,
    encode_string,
    encode_model_usage,
)
from coding_agent_usage_monitors.antigravity_token_usage.cli import TYPER_APP


def test_cli_help_options() -> None:
    """Verify root CLI and ingest --help commands output help messages."""
    runner = CliRunner()
    res1 = runner.invoke(TYPER_APP, ["--help"])
    assert res1.exit_code == 0
    assert "AntiGravity token usage tooling." in res1.stdout

    res2 = runner.invoke(TYPER_APP, ["ingest", "--help"])
    assert res2.exit_code == 0
    assert "Ingest AntiGravity SQLite conversation databases into DuckDB." in res2.stdout


def test_ingest_command_success(tmp_path: Path) -> None:
    """CLI ingest should discover and parse valid AntiGravity database and exit 0."""
    db_dir = tmp_path / "conversations"
    db_dir.mkdir()
    db_file = db_dir / "session_good.db"

    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")
    usage = encode_model_usage(model_id=312, input_tokens=100, visible_output_tokens=50, response_id="r1")
    chat = encode_bytes(4, usage) + encode_string(19, "gemini 2.5 flash")
    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [encode_bytes(1, chat)])
    conn.commit()
    conn.close()

    database_path = tmp_path / "usage.duckdb"
    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "ingest",
            "--data-dir",
            str(db_dir),
            "--database-path",
            str(database_path),
        ],
    )

    assert result.exit_code == 0
    assert "databases_discovered=1" in result.stdout
    assert "databases_parsed=1" in result.stdout
    assert "databases_failed=0" in result.stdout
    assert "canonical_events_written=1" in result.stdout


def test_ingest_command_failure_exits_nonzero(tmp_path: Path) -> None:
    """CLI ingest should return exit code 1 when any database fails."""
    db_dir = tmp_path / "conversations"
    db_dir.mkdir()
    bad_db = db_dir / "session_bad.db"

    conn = sqlite3.connect(str(bad_db))
    conn.execute("CREATE TABLE wrong_table (id INT)")
    conn.commit()
    conn.close()

    database_path = tmp_path / "usage.duckdb"
    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "ingest",
            "--data-dir",
            str(db_dir),
            "--database-path",
            str(database_path),
        ],
    )

    assert result.exit_code == 1
    assert "databases_failed=1" in result.stdout
    assert "failed_database=" in result.stdout
