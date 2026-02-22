"""Tests for Typer CLI entrypoints."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import orjson
import pytest
from typer.testing import CliRunner

from codex_token_usage.cli import TYPER_APP


def test_ingest_command_ingests_session_file(tmp_path: Path) -> None:
    """CLI ingest should process token events and exit successfully."""
    sessions_root = tmp_path / "sessions"
    sessions_root.mkdir(parents=True)
    session_file = sessions_root / "session-1.jsonl"
    _write_jsonl(
        session_file,
        [
            _session_meta_event(),
            _turn_context_event("2026-02-15T00:00:01Z", "gpt-5", "00000000-0000-0000-0000-000000000010"),
            _token_event("2026-02-15T00:00:02Z", total=10, last=10),
        ],
    )

    database_path = tmp_path / "usage.duckdb"
    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "ingest",
            "--sessions-root",
            str(sessions_root),
            "--database-path",
            str(database_path),
        ],
    )

    assert result.exit_code == 0
    assert "files_scanned=1" in result.stdout
    assert "files_ingested=1" in result.stdout
    assert "token_rows_deduped=1" in result.stdout


def test_ingest_command_returns_nonzero_when_any_file_fails(tmp_path: Path) -> None:
    """CLI ingest should return exit code 1 when failed_files is non-empty."""
    sessions_root = tmp_path / "sessions"
    sessions_root.mkdir(parents=True)
    bad_file = sessions_root / "bad.jsonl"
    bad_file.write_text("{malformed json}\n", encoding="utf-8")

    database_path = tmp_path / "usage.duckdb"
    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "ingest",
            "--sessions-root",
            str(sessions_root),
            "--database-path",
            str(database_path),
        ],
    )

    assert result.exit_code == 1
    assert "parse_errors=1" in result.stdout
    assert "failed_file=" in result.stdout


def test_stats_command_prints_daily_and_overall_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI stats should print rich tables with aggregated token usage and cost values."""
    database_path = tmp_path / "usage.duckdb"
    _create_stats_database(
        database_path,
        [
            {
                "model_code": "gpt-5",
                "event_timestamp": "2026-02-15T01:00:00+00:00",
                "input_tokens": 100,
                "cached_input_tokens": 40,
                "output_tokens": 10,
                "reasoning_output_tokens": 5,
            },
            {
                "model_code": "gpt-5",
                "event_timestamp": "2026-02-15T03:00:00+00:00",
                "input_tokens": 50,
                "cached_input_tokens": 0,
                "output_tokens": 20,
                "reasoning_output_tokens": 0,
            },
            {
                "model_code": "o3",
                "event_timestamp": "2026-02-16T02:00:00+00:00",
                "input_tokens": 200,
                "cached_input_tokens": 100,
                "output_tokens": 50,
                "reasoning_output_tokens": 10,
            },
        ],
    )
    monkeypatch.setattr(
        "codex_token_usage.stats.service.get_price_spec",
        lambda: {
            "gpt-5": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
            },
            "o3": {
                "input_cost_per_token": 0.003,
                "output_cost_per_token": 0.004,
                "cache_read_input_token_cost": 0.0003,
            },
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "stats",
            "--database-path",
            str(database_path),
            "--timezone",
            "UTC",
        ],
    )

    assert result.exit_code == 0
    assert "Daily Token Usage" in result.stdout
    assert "Daily Aggregated Costs" in result.stdout
    assert "Overall Token Usage by Model" in result.stdout
    assert "gpt-5" in result.stdout
    assert "o3" in result.stdout
    assert "0.174000" in result.stdout
    assert "0.530000" in result.stdout
    assert "0.704000" in result.stdout


def test_ingest_command_prints_last_7_days_stats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI ingest should render summary and 7-day statistics output."""
    sessions_root = tmp_path / "sessions"
    sessions_root.mkdir(parents=True)
    session_file = sessions_root / "session-1.jsonl"
    recent_timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_jsonl(
        session_file,
        [
            _session_meta_event(),
            _turn_context_event(recent_timestamp, "gpt-5", "00000000-0000-0000-0000-000000000010"),
            _token_event(recent_timestamp, total=10, last=10),
        ],
    )

    database_path = tmp_path / "usage.duckdb"
    monkeypatch.setattr(
        "codex_token_usage.stats.service.get_price_spec",
        lambda: {
            "gpt-5": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
            }
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "ingest",
            "--database-path",
            str(database_path),
            "--sessions-root",
            str(sessions_root),
        ],
    )

    assert result.exit_code == 0
    assert "files_ingested=1" in result.stdout
    assert "Statistics (last 7 days):" in result.stdout
    assert "Daily Token Usage" in result.stdout
    assert "gpt-5" in result.stdout


def test_stats_command_handles_empty_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI stats should print a no-data message when the details table is empty."""
    database_path = tmp_path / "usage.duckdb"
    _create_stats_database(database_path, [])

    monkeypatch.setattr("codex_token_usage.stats.service.get_price_spec", lambda: {})

    runner = CliRunner()
    result = runner.invoke(TYPER_APP, ["stats", "--database-path", str(database_path)])

    assert result.exit_code == 0
    assert "No token usage events found in the database." in result.stdout


def test_stats_command_since_filters_older_days(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI stats should keep only rows on/after `--since`."""
    database_path = tmp_path / "usage.duckdb"
    _create_stats_database(
        database_path,
        [
            {
                "model_code": "gpt-5",
                "event_timestamp": "2026-02-15T01:00:00+00:00",
                "input_tokens": 100,
                "cached_input_tokens": 40,
                "output_tokens": 10,
                "reasoning_output_tokens": 5,
            },
            {
                "model_code": "o3",
                "event_timestamp": "2026-02-16T02:00:00+00:00",
                "input_tokens": 200,
                "cached_input_tokens": 100,
                "output_tokens": 50,
                "reasoning_output_tokens": 10,
            },
        ],
    )
    monkeypatch.setattr(
        "codex_token_usage.stats.service.get_price_spec",
        lambda: {
            "gpt-5": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
            },
            "o3": {
                "input_cost_per_token": 0.003,
                "output_cost_per_token": 0.004,
                "cache_read_input_token_cost": 0.0003,
            },
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "stats",
            "--database-path",
            str(database_path),
            "--timezone",
            "UTC",
            "--since",
            "2026-02-16",
        ],
    )

    assert result.exit_code == 0
    assert "2026-02-15" not in result.stdout
    assert "2026-02-16" in result.stdout
    assert "gpt-5" not in result.stdout
    assert "o3" in result.stdout
    assert "0.530000" in result.stdout


def test_stats_command_since_rejects_invalid_date(tmp_path: Path) -> None:
    """CLI stats should reject invalid `--since` values."""
    database_path = tmp_path / "usage.duckdb"
    _create_stats_database(database_path, [])

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "stats",
            "--database-path",
            str(database_path),
            "--since",
            "2026-02-99",
        ],
    )

    assert result.exit_code == 2
    assert "Invalid --since value" in result.output


def _create_stats_database(database_path: Path, rows: list[dict[str, object]]) -> None:
    """Create a stats test database with a token details table."""
    connection = duckdb.connect(str(database_path))
    try:
        _ = connection.execute(
            """
CREATE TABLE codex_session_details (
    model_code VARCHAR,
    event_timestamp TIMESTAMPTZ NOT NULL,
    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_output_tokens BIGINT NOT NULL
)
            """
        )
        if rows:
            _ = connection.executemany(
                """
INSERT INTO codex_session_details (
    model_code,
    event_timestamp,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    reasoning_output_tokens
)
VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    [
                        row["model_code"],
                        row["event_timestamp"],
                        row["input_tokens"],
                        row["cached_input_tokens"],
                        row["output_tokens"],
                        row["reasoning_output_tokens"],
                    ]
                    for row in rows
                ],
            )
    finally:
        connection.close()


def _session_meta_event() -> dict[str, object]:
    """Build a static session_meta event."""
    return {
        "timestamp": "2026-02-15T00:00:00Z",
        "type": "session_meta",
        "payload": {
            "id": "00000000-0000-0000-0000-000000000001",
            "cwd": "/workspace",
        },
    }


def _turn_context_event(timestamp: str, model: str, turn_id: str) -> dict[str, object]:
    """Build a turn_context event."""
    return {
        "timestamp": timestamp,
        "type": "turn_context",
        "payload": {
            "model": model,
            "turn_id": turn_id,
        },
    }


def _token_event(timestamp: str, total: int, last: int) -> dict[str, object]:
    """Build a token_count event with all required fields."""
    return {
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": total,
                    "cached_input_tokens": 0,
                    "output_tokens": 0,
                    "reasoning_output_tokens": 0,
                    "total_tokens": total,
                },
                "last_token_usage": {
                    "input_tokens": last,
                    "cached_input_tokens": 0,
                    "output_tokens": 0,
                    "reasoning_output_tokens": 0,
                    "total_tokens": last,
                },
            },
        },
    }


def _write_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    """Write JSONL events to disk."""
    with path.open("wb") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")
