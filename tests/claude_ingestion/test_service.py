"""Integration tests for Claude Code ingestion service and DuckDB repository."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import orjson

from coding_agent_usage_monitors.claude_token_usage.ingestion.repository import IngestionRepository
from coding_agent_usage_monitors.claude_token_usage.ingestion.service import IngestionService


def test_ingestion_service_is_idempotent_and_resumes_from_checkpoint(tmp_path: Path) -> None:
    """Ingestion should skip unchanged files and append only new tail rows."""
    sessions_root = tmp_path / "projects" / "my-project"
    sessions_root.mkdir(parents=True)

    session_file = sessions_root / "session-1.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=100, output_t=50),
            _assistant_event("2026-03-15T00:00:02Z", model="claude-opus-4-6", input_t=200, output_t=100),
        ],
    )

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    service = IngestionService(repository=repository, session_roots=[tmp_path / "projects"])

    # First ingestion
    first = service.ingest()
    assert first.files_scanned == 1
    assert first.files_ingested == 1
    assert first.files_skipped_unchanged == 0
    assert first.usage_rows_raw == 2
    assert first.usage_rows_deduped == 2
    assert first.parse_errors == 0

    # Second ingestion: file unchanged -> skip
    second = service.ingest()
    assert second.files_scanned == 1
    assert second.files_ingested == 0
    assert second.files_skipped_unchanged == 1

    # Append new entries and touch file
    _append_jsonl(
        session_file,
        [
            _assistant_event("2026-03-15T00:00:03Z", model="claude-haiku-4-5-20251001", input_t=50, output_t=25),
        ],
    )
    os.utime(session_file, (session_file.stat().st_atime + 10, session_file.stat().st_mtime + 10))

    # Third ingestion: new rows only (checkpoint re-includes boundary row for safety)
    third = service.ingest()
    assert third.files_scanned == 1
    assert third.files_ingested == 1
    assert third.files_skipped_unchanged == 0
    assert third.usage_rows_raw == 3
    assert third.usage_rows_skipped_before_checkpoint == 1
    assert third.usage_rows_deduped == 2  # boundary row re-included, DB ON CONFLICT handles it
    assert third.parse_errors == 0

    # Verify DB state
    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        event_count = connection.execute("SELECT COUNT(*) FROM claude_usage_events").fetchone()[0]
        assert event_count == 3

        model_rows = connection.execute(
            """
            SELECT model_code, input_tokens, output_tokens
            FROM claude_usage_events
            ORDER BY event_timestamp
            """
        ).fetchall()
        assert model_rows == [
            ("claude-sonnet-4-6", 100, 50),
            ("claude-opus-4-6", 200, 100),
            ("claude-haiku-4-5-20251001", 50, 25),
        ]

        metadata = connection.execute("SELECT session_id, project_name FROM claude_session_metadata").fetchone()
        assert metadata[0] == "sess-001"
        assert metadata[1] == "my-project"

        file_count = connection.execute("SELECT COUNT(*) FROM claude_ingestion_files").fetchone()[0]
        assert file_count == 1
    finally:
        connection.close()
        repository.close()


def test_ingestion_handles_subagent_files(tmp_path: Path) -> None:
    """Ingestion should handle subagent JSONL files with sidechain info."""
    sessions_root = tmp_path / "projects" / "my-project" / "session-1" / "subagents"
    sessions_root.mkdir(parents=True)

    agent_file = sessions_root / "agent-abc.jsonl"
    _write_jsonl(
        agent_file,
        [
            _assistant_event(
                "2026-03-15T00:00:01Z",
                model="claude-haiku-4-5-20251001",
                input_t=50,
                output_t=25,
                is_sidechain=True,
                agent_id="agent-abc",
            ),
        ],
    )

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    service = IngestionService(repository=repository, session_roots=[tmp_path / "projects"])

    counters = service.ingest()
    assert counters.files_ingested == 1
    assert counters.usage_rows_deduped == 1

    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        row = connection.execute("SELECT is_sidechain, agent_id FROM claude_usage_events").fetchone()
        assert row[0] is True
        assert row[1] == "agent-abc"
    finally:
        connection.close()
        repository.close()


def test_ingestion_continues_after_per_file_error(tmp_path: Path) -> None:
    """Ingestion should continue processing other files after one file fails."""
    sessions_root = tmp_path / "projects" / "my-project"
    sessions_root.mkdir(parents=True)

    # Bad file: malformed JSON
    bad_file = sessions_root / "bad-session.jsonl"
    bad_file.write_text('{"type": "assistant"\n', encoding="utf-8")

    # Good file
    good_file = sessions_root / "good-session.jsonl"
    _write_jsonl(
        good_file,
        [
            _assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=100, output_t=50),
        ],
    )

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    service = IngestionService(repository=repository, session_roots=[tmp_path / "projects"])

    counters = service.ingest()
    assert counters.files_scanned == 2
    assert counters.files_ingested == 1
    assert counters.parse_errors == 1
    assert len(counters.failed_files) == 1

    repository.close()


# --- helpers ---


def _assistant_event(
    timestamp: str,
    *,
    model: str,
    input_t: int,
    output_t: int,
    message_id: str | None = None,
    request_id: str | None = None,
    is_sidechain: bool = False,
    agent_id: str | None = None,
    session_id: str = "sess-001",
) -> dict[str, object]:
    """Build an assistant entry with usage fields."""
    msg_id = message_id or f"msg-{timestamp}"
    req_id = request_id or f"req-{timestamp}"
    event: dict[str, object] = {
        "type": "assistant",
        "sessionId": session_id,
        "timestamp": timestamp,
        "requestId": req_id,
        "message": {
            "id": msg_id,
            "model": model,
            "role": "assistant",
            "usage": {
                "input_tokens": input_t,
                "output_tokens": output_t,
            },
        },
    }
    if is_sidechain:
        event["isSidechain"] = True
    if agent_id is not None:
        event["agentId"] = agent_id
    return event


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
