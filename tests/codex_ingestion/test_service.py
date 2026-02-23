"""Integration tests for ingestion service and DuckDB repository."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import orjson

from codex_token_usage.ingestion.repository import IngestionRepository
from codex_token_usage.ingestion.service import IngestionService


def test_ingestion_service_is_idempotent_and_resumes_from_checkpoint(tmp_path: Path) -> None:
    """Ingestion should skip unchanged files and append only new tail rows."""
    sessions_root = tmp_path / "sessions"
    sessions_root.mkdir(parents=True)

    session_file = sessions_root / "session-1.jsonl"
    _write_jsonl(
        session_file,
        [
            _session_meta_event(),
            _turn_context_event("2026-02-15T00:00:01Z", "gpt-5", "00000000-0000-0000-0000-000000000010"),
            _token_event("2026-02-15T00:00:02Z", total=10, last=10),
            _token_event("2026-02-15T00:00:03Z", total=20, last=10),
        ],
    )

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    service = IngestionService(repository=repository, sessions_root=sessions_root)

    first = service.ingest()
    assert first.files_scanned == 1
    assert first.files_ingested == 1
    assert first.files_skipped_unchanged == 0
    assert first.token_rows_raw == 2
    assert first.token_rows_deduped == 2
    assert first.parse_errors == 0

    second = service.ingest()
    assert second.files_scanned == 1
    assert second.files_ingested == 0
    assert second.files_skipped_unchanged == 1

    _append_jsonl(
        session_file,
        [
            _turn_context_event("2026-02-15T00:00:04Z", "o3", "00000000-0000-0000-0000-000000000011"),
            _token_event("2026-02-15T00:00:04Z", total=20, last=10),
            _token_event("2026-02-15T00:00:05Z", total=30, last=10),
        ],
    )
    os.utime(session_file, (session_file.stat().st_atime + 10, session_file.stat().st_mtime + 10))

    third = service.ingest()
    assert third.files_scanned == 1
    assert third.files_ingested == 1
    assert third.files_skipped_unchanged == 0
    assert third.token_rows_raw == 4
    assert third.token_rows_skipped_before_checkpoint == 1
    assert third.duplicate_rows_skipped == 1
    assert third.token_rows_deduped == 2
    assert third.parse_errors == 0

    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        detail_count = connection.execute("SELECT COUNT(*) FROM codex_session_details").fetchone()[0]
        assert detail_count == 3

        model_rows = connection.execute(
            """
            SELECT total_tokens_cumulative, model_code
            FROM codex_session_details
            ORDER BY total_tokens_cumulative
            """
        ).fetchall()
        assert model_rows == [(10, "gpt-5"), (20, "gpt-5"), (30, "o3")]

        file_count = connection.execute("SELECT COUNT(*) FROM codex_ingestion_files").fetchone()[0]
        assert file_count == 1
    finally:
        connection.close()
        repository.close()


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


def _append_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    """Append JSONL events to an existing file."""
    with path.open("ab") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")
