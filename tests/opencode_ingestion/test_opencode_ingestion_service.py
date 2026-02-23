"""Integration tests for OpenCode ingestion service."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import duckdb
import pytest

from opencode_token_usage.ingestion.errors import ParseError
from opencode_token_usage.ingestion.repository import IngestionRepository
from opencode_token_usage.ingestion.service import IngestionService
from opencode_token_usage.ingestion.source_reader import SourceReader


def test_service_ingests_incrementally_and_skips_when_unchanged(tmp_path: Path) -> None:
    """Service should ingest once, then fast-skip when source max time_updated is unchanged."""
    source_db = tmp_path / "opencode.db"
    _build_source_db(source_db, assistant_rows=[("m1", 1000), ("m2", 2000)])

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    reader = SourceReader(source_db)
    service = IngestionService(repository=repository, source_reader=reader, batch_size=1)

    first = service.ingest()
    assert first.messages_scanned == 2
    assert first.messages_ingested == 2
    assert first.skipped_no_source_changes is False

    second = service.ingest()
    assert second.messages_scanned == 0
    assert second.messages_ingested == 0
    # Fast-skip is disabled when timestamps match to catch same-ms updates
    assert second.skipped_no_source_changes is False

    _insert_assistant_message(source_db, message_id="m3", time_ms=3000)

    third = service.ingest()
    assert third.messages_scanned == 1
    assert third.messages_ingested == 1

    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        count = connection.execute("SELECT COUNT(*) FROM opencode_message_usage").fetchone()[0]
        assert count == 3
    finally:
        connection.close()
        reader.close()
        repository.close()


def test_service_full_refresh_reupserts_existing_messages(tmp_path: Path) -> None:
    """Full refresh should ignore checkpoint and upsert all assistant messages."""
    source_db = tmp_path / "opencode.db"
    _build_source_db(source_db, assistant_rows=[("m1", 1000)])

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    reader = SourceReader(source_db)
    service = IngestionService(repository=repository, source_reader=reader)

    first = service.ingest()
    assert first.messages_ingested == 1

    _update_assistant_tokens(source_db, message_id="m1", input_tokens=99, time_updated_ms=4000)

    second = service.ingest(full_refresh=True)
    assert second.messages_scanned == 1
    assert second.messages_ingested == 1

    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        row = connection.execute(
            "SELECT input_tokens, source_time_updated_ms FROM opencode_message_usage WHERE message_id = 'm1'"
        ).fetchone()
        assert row == (99, 4000)
    finally:
        connection.close()
        reader.close()
        repository.close()


def test_service_fails_fast_on_malformed_required_tokens(tmp_path: Path) -> None:
    """Malformed required token fields should raise ParseError and stop ingestion."""
    source_db = tmp_path / "opencode.db"
    _build_source_db(source_db, assistant_rows=[("m1", 1000)], valid=False)

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    reader = SourceReader(source_db)
    service = IngestionService(repository=repository, source_reader=reader)

    with pytest.raises(ParseError):
        _ = service.ingest()

    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        count = connection.execute("SELECT COUNT(*) FROM opencode_message_usage").fetchone()[0]
        assert count == 0
    finally:
        connection.close()
        reader.close()
        repository.close()


def _build_source_db(source_db: Path, assistant_rows: list[tuple[str, int]], valid: bool = True) -> None:
    connection = sqlite3.connect(str(source_db))
    try:
        _ = connection.execute("CREATE TABLE project (id TEXT PRIMARY KEY, worktree TEXT)")
        _ = connection.execute(
            "CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT, title TEXT, directory TEXT, version TEXT)"
        )
        _ = connection.execute(
            "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT)"
        )
        _ = connection.execute("INSERT INTO project (id, worktree) VALUES ('p1', '/tmp/project')")
        _ = connection.execute(
            "INSERT INTO session (id, project_id, title, directory, version) VALUES ('s1', 'p1', 'session title', '/tmp/project', '1.2.0')"
        )
        for message_id, time_ms in assistant_rows:
            _ = connection.execute(
                "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
                (message_id, "s1", time_ms, time_ms, json.dumps(_assistant_payload(valid=valid))),
            )
        connection.commit()
    finally:
        connection.close()


def _insert_assistant_message(source_db: Path, message_id: str, time_ms: int) -> None:
    connection = sqlite3.connect(str(source_db))
    try:
        _ = connection.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
            (message_id, "s1", time_ms, time_ms, json.dumps(_assistant_payload(valid=True))),
        )
        connection.commit()
    finally:
        connection.close()


def _update_assistant_tokens(source_db: Path, message_id: str, input_tokens: int, time_updated_ms: int) -> None:
    connection = sqlite3.connect(str(source_db))
    try:
        payload = _assistant_payload(valid=True)
        payload["tokens"]["input"] = input_tokens
        _ = connection.execute(
            "UPDATE message SET data = ?, time_updated = ? WHERE id = ?",
            (json.dumps(payload), time_updated_ms, message_id),
        )
        connection.commit()
    finally:
        connection.close()


def _assistant_payload(valid: bool) -> dict[str, object]:
    if not valid:
        return {
            "role": "assistant",
            "tokens": {
                "input": "bad",
                "output": 2,
                "reasoning": 3,
                "cache": {"read": 4, "write": 5},
            },
        }

    return {
        "role": "assistant",
        "tokens": {
            "input": 1,
            "output": 2,
            "reasoning": 3,
            "cache": {"read": 4, "write": 5},
            "total": 15,
        },
        "cost": 0.5,
        "providerID": "openai",
        "modelID": "gpt-5",
        "agent": "assistant",
        "mode": "default",
        "finish": "stop",
        "time": {"completed": 2000},
    }
