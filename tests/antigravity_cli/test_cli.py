"""Tests for AntiGravity Typer CLI entrypoints."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from tests.antigravity_ingestion.test_parser import (
    encode_bytes,
    encode_string,
    encode_model_usage,
)
from coding_agent_usage_monitors.antigravity_token_usage.cli import TYPER_APP


def test_cli_help_options() -> None:
    """Verify root, ingest, and stats help commands output help messages."""
    runner = CliRunner()
    res1 = runner.invoke(TYPER_APP, ["--help"])
    assert res1.exit_code == 0
    assert "AntiGravity token usage tooling." in res1.stdout

    res2 = runner.invoke(TYPER_APP, ["ingest", "--help"])
    assert res2.exit_code == 0
    assert "Ingest AntiGravity SQLite conversation databases into DuckDB." in res2.stdout

    res3 = runner.invoke(TYPER_APP, ["stats", "--help"])
    assert res3.exit_code == 0
    assert "Aggregate and print daily token usage" in res3.stdout


def test_ingest_command_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr("coding_agent_usage_monitors.antigravity_token_usage.stats.service.get_price_spec", dict)
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
    assert "Statistics (last 7 days):" in result.stdout


def test_stats_command_prints_usage_and_costs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stats should read canonical events and render normalized model costs."""
    database_path = tmp_path / "usage.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
CREATE TABLE antigravity_usage_events (
    event_key VARCHAR PRIMARY KEY,
    discovery_source VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMPTZ,
    model_code VARCHAR NOT NULL,
    provider_id BIGINT,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_tokens BIGINT NOT NULL,
    total_output_tokens BIGINT NOT NULL,
    cache_creation_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL
)
            """
        )
        connection.execute(
            """
INSERT INTO antigravity_usage_events VALUES
('event-1', 'default', 'session-1', '2026-09-21T00:00:00+00:00', 'gemini-3.8-flash-high', NULL,
 100, 10, 5, 15, 4, 20, 139)
            """
        )
    finally:
        connection.close()
    monkeypatch.setattr(
        "coding_agent_usage_monitors.antigravity_token_usage.stats.service.get_price_spec",
        lambda: {
            "google/gemini-3.8-flash": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
                "cache_creation_input_token_cost": 0.0002,
            }
        },
    )

    result = CliRunner().invoke(
        TYPER_APP,
        ["stats", "--database-path", str(database_path), "--timezone", "UTC"],
        terminal_width=220,
    )

    assert result.exit_code == 0
    assert "Daily Token Usage" in result.stdout
    assert "Overall Token Usage by Model" in result.stdout


def test_stats_command_rejects_invalid_timezone(tmp_path: Path) -> None:
    """Stats should report an invalid timezone as a usage error, not a traceback."""
    database_path = tmp_path / "usage.duckdb"
    duckdb.connect(str(database_path)).close()

    result = CliRunner().invoke(
        TYPER_APP,
        ["stats", "--database-path", str(database_path), "--timezone", "Not/AZone"],
    )

    assert result.exit_code != 0
    assert "Invalid timezone: Not/AZone" in result.output


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
