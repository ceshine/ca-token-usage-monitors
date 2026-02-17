"""Tests for Gemini preprocessing metadata behavior."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import orjson
import pytest

from gemini_token_usage.preprocessing.convert import run_log_conversion
from gemini_token_usage.preprocessing.metadata import PROJECT_METADATA_RECORD_TYPE, ensure_project_metadata_line
from gemini_token_usage.preprocessing.simplify import run_log_simplification


def test_ensure_project_metadata_line_prepends_when_missing(tmp_path: Path) -> None:
    """A missing metadata line should be prepended to existing JSONL content."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    original_event = {
        "attributes": {
            "event.name": "gemini_cli.api_response",
            "event.timestamp": "2026-02-17T00:00:00Z",
            "model": "gemini-2.5-pro",
            "input_token_count": 1,
            "cached_content_token_count": 0,
            "output_token_count": 1,
            "thoughts_token_count": 0,
            "total_token_count": 2,
        }
    }
    with jsonl_file.open("wb") as handle:
        handle.write(orjson.dumps(original_event))
        handle.write(b"\n")

    metadata = ensure_project_metadata_line(jsonl_file)

    lines = jsonl_file.read_text(encoding="utf-8").splitlines()
    parsed_metadata = orjson.loads(lines[0])
    assert parsed_metadata["record_type"] == PROJECT_METADATA_RECORD_TYPE
    assert UUID(parsed_metadata["project_id"]) == metadata.project_id
    assert orjson.loads(lines[1]) == original_event


def test_ensure_project_metadata_line_fails_on_malformed_metadata(tmp_path: Path) -> None:
    """Malformed metadata line should fail fast."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    jsonl_file.write_text(
        '{"record_type":"wrong.record","schema_version":1,"project_id":"00000000-0000-0000-0000-000000000001"}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Malformed metadata"):
        _ = ensure_project_metadata_line(jsonl_file)


def test_run_log_simplification_preserves_metadata_line_exactly(tmp_path: Path) -> None:
    """Simplification should keep metadata line unchanged."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    metadata_line = (
        '{"record_type":"gemini_cli.project_metadata","schema_version":1,'
        '"project_id":"00000000-0000-0000-0000-000000000001"}'
    )
    event = {
        "attributes": {
            "event.name": "gemini_cli.api_response",
            "event.timestamp": "2026-02-17T00:00:00Z",
            "duration_ms": 100,
            "input_token_count": 100,
            "output_token_count": 10,
            "cached_content_token_count": 40,
            "thoughts_token_count": 5,
            "total_token_count": 115,
            "tool_token_count": 0,
            "model": "gemini-2.5-pro",
            "session.id": "session-1",
        },
        "_body": {},
    }
    with jsonl_file.open("wb") as handle:
        handle.write(metadata_line.encode("utf-8"))
        handle.write(b"\n")
        handle.write(orjson.dumps(event))
        handle.write(b"\n")

    _ = run_log_simplification(jsonl_file, level=3, disable_archiving=True)

    first_line = jsonl_file.read_bytes().splitlines()[0]
    assert first_line.decode("utf-8") == metadata_line


def test_run_log_conversion_initializes_metadata_line(tmp_path: Path) -> None:
    """Converting from telemetry.log should create telemetry.jsonl with metadata first line."""
    log_file = tmp_path / "telemetry.log"
    row = {
        "attributes": {
            "event.name": "gemini_cli.api_response",
            "event.timestamp": "2026-02-17T00:00:00Z",
            "duration_ms": 100,
            "input_token_count": 1,
            "output_token_count": 1,
            "cached_content_token_count": 0,
            "thoughts_token_count": 0,
            "total_token_count": 2,
            "tool_token_count": 0,
            "model": "gemini-2.5-pro",
            "session.id": "session-1",
        },
        "_body": {},
    }
    with log_file.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(row, indent=2))
        handle.write("\n")

    output_path = run_log_conversion(log_file)
    first_line = output_path.read_bytes().splitlines()[0]
    parsed = orjson.loads(first_line)
    assert parsed["record_type"] == PROJECT_METADATA_RECORD_TYPE


def test_run_log_simplification_fails_when_metadata_appears_after_line_one(tmp_path: Path) -> None:
    """Simplification must reject metadata records outside line 1."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    with jsonl_file.open("wb") as handle:
        handle.write(
            b'{"record_type":"gemini_cli.project_metadata","schema_version":1,'
            b'"project_id":"00000000-0000-0000-0000-000000000001"}\n'
        )
        handle.write(
            b'{"record_type":"gemini_cli.project_metadata","schema_version":1,'
            b'"project_id":"00000000-0000-0000-0000-000000000001"}\n'
        )

    with pytest.raises(ValueError, match="line 1"):
        _ = run_log_simplification(jsonl_file, level=1, disable_archiving=True)
