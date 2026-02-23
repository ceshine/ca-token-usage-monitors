"""Tests for session log parser."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import orjson
import pytest

from codex_token_usage.ingestion.errors import ModelAttributionError, ParseError
from codex_token_usage.ingestion.parser import parse_session_file, parse_session_identity
from codex_token_usage.ingestion.schemas import SessionCheckpoint


def test_parse_session_file_extracts_rows_with_checkpoint_filter(tmp_path: Path) -> None:
    """Parser should keep tail rows and count filtered rows accurately."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            {
                "timestamp": "2026-02-15T00:00:00Z",
                "type": "session_meta",
                "payload": {
                    "id": "00000000-0000-0000-0000-000000000001",
                    "cwd": "/workspace",
                },
            },
            {
                "timestamp": "2026-02-15T00:00:01Z",
                "type": "turn_context",
                "payload": {
                    "model": "gpt-5",
                    "turn_id": "00000000-0000-0000-0000-000000000010",
                },
            },
            _token_event("2026-02-15T00:00:02Z", total=5, last=5),
            _token_event("2026-02-15T00:00:03Z", total=10, last=5),
            {
                "timestamp": "2026-02-15T00:00:04Z",
                "type": "event_msg",
                "payload": {"type": "token_count", "info": None},
            },
            _token_event("2026-02-15T00:00:05Z", total=18, last=8),
        ],
    )

    identity = parse_session_identity(session_file)
    checkpoint = SessionCheckpoint(
        last_ts=datetime(2026, 2, 15, 0, 0, 3, tzinfo=UTC),
        last_total_tokens_cumulative=10,
    )

    parsed = parse_session_file(session_file, identity, checkpoint)

    assert parsed.token_rows_raw == 3
    assert parsed.token_rows_skipped_info_null == 1
    assert parsed.token_rows_skipped_before_checkpoint == 1
    assert [row.total_tokens_cumulative for row in parsed.token_rows] == [10, 18]
    assert parsed.metadata.session_id == UUID("00000000-0000-0000-0000-000000000001")


def test_parse_session_file_fails_when_model_context_is_missing(tmp_path: Path) -> None:
    """Parser must fail token rows that cannot be attributed to a model."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            {
                "timestamp": "2026-02-15T00:00:00Z",
                "type": "session_meta",
                "payload": {"id": "00000000-0000-0000-0000-000000000001"},
            },
            _token_event("2026-02-15T00:00:01Z", total=10, last=10),
        ],
    )

    identity = parse_session_identity(session_file)
    with pytest.raises(ModelAttributionError):
        parse_session_file(session_file, identity, checkpoint=None)


def test_parse_session_identity_fails_on_malformed_json(tmp_path: Path) -> None:
    """Malformed JSON lines should raise ParseError with line context."""
    session_file = tmp_path / "session.jsonl"
    session_file.write_text('{"timestamp": "2026-02-15T00:00:00Z"\n', encoding="utf-8")

    with pytest.raises(ParseError):
        parse_session_identity(session_file)


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
