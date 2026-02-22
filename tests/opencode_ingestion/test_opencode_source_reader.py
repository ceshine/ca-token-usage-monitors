"""Tests for OpenCode source reader."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from opencode_token_usage.ingestion.errors import SourceSchemaError
from opencode_token_usage.ingestion.schemas import SourceCheckpoint
from opencode_token_usage.ingestion.source_reader import SourceReader


def test_source_reader_filters_rows_by_checkpoint_tuple(tmp_path: Path) -> None:
    """Reader should return assistant rows ordered and filtered by `(time_updated, id)` checkpoint."""
    source_db = tmp_path / "opencode.db"
    _build_source_db(source_db)

    reader = SourceReader(source_db)
    try:
        reader.ensure_schema()
        rows = reader.iter_assistant_rows(SourceCheckpoint(last_time_updated_ms=2000, last_message_id="m2"))
    finally:
        reader.close()

    assert [row.message_id for row in rows] == ["m3"]


def test_source_reader_latest_assistant_timestamp(tmp_path: Path) -> None:
    """Reader should return max assistant `time_updated` or None when no assistant rows exist."""
    source_db = tmp_path / "opencode.db"
    _build_source_db(source_db)

    reader = SourceReader(source_db)
    try:
        assert reader.get_latest_assistant_time_updated_ms() == 3000
    finally:
        reader.close()


def test_source_reader_raises_for_missing_required_tables(tmp_path: Path) -> None:
    """Schema validation should fail fast when required tables are missing."""
    source_db = tmp_path / "opencode.db"
    connection = sqlite3.connect(str(source_db))
    try:
        _ = connection.execute(
            "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT)"
        )
        connection.commit()
    finally:
        connection.close()

    reader = SourceReader(source_db)
    try:
        with pytest.raises(SourceSchemaError):
            reader.ensure_schema()
    finally:
        reader.close()


def _build_source_db(source_db: Path) -> None:
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

        _ = connection.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
            ("m1", "s1", 1000, 1000, json.dumps(_assistant_payload())),
        )
        _ = connection.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
            ("m2", "s1", 2000, 2000, json.dumps(_assistant_payload())),
        )
        _ = connection.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
            ("m3", "s1", 3000, 3000, json.dumps(_assistant_payload())),
        )
        _ = connection.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
            ("u1", "s1", 4000, 4000, json.dumps({"role": "user"})),
        )
        connection.commit()
    finally:
        connection.close()


def _assistant_payload() -> dict[str, object]:
    return {
        "role": "assistant",
        "tokens": {
            "input": 1,
            "output": 2,
            "reasoning": 3,
            "cache": {"read": 4, "write": 5},
            "total": 15,
        },
        "cost": 0.1,
        "providerID": "openai",
        "modelID": "gpt-5",
    }
