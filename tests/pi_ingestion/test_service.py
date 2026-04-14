"""Integration tests for Pi agent ingestion service and DuckDB repository."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import orjson

from coding_agent_usage_monitors.pi_token_usage.ingestion.service import IngestionService
from coding_agent_usage_monitors.pi_token_usage.ingestion.repository import IngestionRepository

SESSION_ID = "11111111-2222-3333-4444-555555555555"


def test_ingestion_is_idempotent_and_tails_from_checkpoint(tmp_path: Path) -> None:
    session_root = tmp_path / "sessions"
    project_dir = session_root / "--home-alice-work--"
    project_dir.mkdir(parents=True)
    session_file = project_dir / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    _write_jsonl(
        session_file,
        [
            _session_entry(cwd="/home/alice/work"),
            _assistant_event("2026-04-13T15:43:00Z", msg_id="m-1", input_t=100, output_t=20),
            _assistant_event("2026-04-13T15:44:00Z", msg_id="m-2", input_t=200, output_t=40),
        ],
    )

    db_path = tmp_path / "pi.duckdb"
    repo = IngestionRepository(db_path)
    service = IngestionService(repository=repo, session_root=session_root)

    first = service.ingest()
    assert first.files_scanned == 1
    assert first.files_ingested == 1
    assert first.usage_rows_raw == 2
    assert first.usage_rows_persisted == 2
    assert first.parse_errors == 0

    # Re-run unchanged -> skip.
    second = service.ingest()
    assert second.files_scanned == 1
    assert second.files_ingested == 0
    assert second.files_skipped_unchanged == 1

    # Append new row and bump mtime.
    _append_jsonl(
        session_file,
        [_assistant_event("2026-04-13T15:45:00Z", msg_id="m-3", input_t=300, output_t=60)],
    )
    os.utime(session_file, (session_file.stat().st_atime + 10, session_file.stat().st_mtime + 10))

    third = service.ingest()
    assert third.files_ingested == 1
    assert third.usage_rows_raw == 3
    assert third.usage_rows_skipped_before_checkpoint == 2
    assert third.usage_rows_persisted == 1

    connection = duckdb.connect(str(db_path))
    try:
        count = connection.execute("SELECT COUNT(*) FROM pi_usage_events").fetchone()[0]
        assert count == 3
        ordered = connection.execute(
            "SELECT message_id, input_tokens FROM pi_usage_events ORDER BY event_timestamp"
        ).fetchall()
        assert ordered == [("m-1", 100), ("m-2", 200), ("m-3", 300)]
        meta = connection.execute("SELECT session_id, cwd, session_version FROM pi_session_metadata").fetchone()
        assert meta == (SESSION_ID, "/home/alice/work", 3)
    finally:
        connection.close()
        repo.close()


def test_ingestion_continues_after_per_file_error(tmp_path: Path) -> None:
    session_root = tmp_path / "sessions"
    project_dir = session_root / "--home-alice-work--"
    project_dir.mkdir(parents=True)

    bad_file = project_dir / f"2026-04-13T00-00-00-000Z_{SESSION_ID}.jsonl"
    bad_file.write_text('{"type": "session"\n', encoding="utf-8")

    good_session_id = "22222222-2222-3333-4444-555555555555"
    good_file = project_dir / f"2026-04-13T01-00-00-000Z_{good_session_id}.jsonl"
    _write_jsonl(
        good_file,
        [
            _session_entry(cwd="/home/alice/work", session_id=good_session_id),
            _assistant_event("2026-04-13T01:01:00Z", msg_id="m-ok"),
        ],
    )

    db_path = tmp_path / "pi.duckdb"
    repo = IngestionRepository(db_path)
    service = IngestionService(repository=repo, session_root=session_root)

    counters = service.ingest()
    assert counters.files_scanned == 2
    assert counters.files_ingested == 1
    assert counters.parse_errors == 1
    assert len(counters.failed_files) == 1

    repo.close()


def test_ingestion_recovers_cwd_from_parent_directory(tmp_path: Path) -> None:
    session_root = tmp_path / "sessions"
    project_dir = session_root / "--home-alice-work--"
    project_dir.mkdir(parents=True)
    session_file = project_dir / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    _write_jsonl(
        session_file,
        [
            _session_entry(cwd=None),
            _assistant_event("2026-04-13T15:43:00Z", msg_id="m-1"),
        ],
    )

    db_path = tmp_path / "pi.duckdb"
    repo = IngestionRepository(db_path)
    service = IngestionService(repository=repo, session_root=session_root)

    counters = service.ingest()
    assert counters.sessions_cwd_recovered_from_path == 1

    connection = duckdb.connect(str(db_path))
    try:
        row = connection.execute("SELECT cwd FROM pi_session_metadata").fetchone()
        assert row == ("/home/alice/work",)
    finally:
        connection.close()
        repo.close()


# --- helpers ---


def _session_entry(*, cwd: str | None, session_id: str = SESSION_ID) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "session",
        "version": 3,
        "id": session_id,
        "timestamp": "2026-04-13T15:42:45.133Z",
    }
    if cwd is not None:
        entry["cwd"] = cwd
    return entry


def _assistant_event(
    timestamp: str,
    *,
    msg_id: str,
    input_t: int = 100,
    output_t: int = 20,
) -> dict[str, Any]:
    return {
        "type": "message",
        "id": msg_id,
        "parentId": None,
        "timestamp": timestamp,
        "message": {
            "role": "assistant",
            "api": "anthropic-messages",
            "provider": "opencode",
            "model": "minimax-m2.5-free",
            "stopReason": "stop",
            "responseId": f"resp-{msg_id}",
            "usage": {
                "input": input_t,
                "output": output_t,
                "cacheRead": 0,
                "cacheWrite": 0,
                "totalTokens": input_t + output_t,
                "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0},
            },
            "timestamp": 0,
        },
    }


def _write_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    with path.open("wb") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")


def _append_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    with path.open("ab") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")
