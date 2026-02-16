"""Tests for Typer CLI entrypoints."""

from __future__ import annotations

from pathlib import Path

import orjson
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
