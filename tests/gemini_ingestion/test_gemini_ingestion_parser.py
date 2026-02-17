"""Tests for Gemini ingestion parser."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import orjson
import pytest

from gemini_token_usage.ingestion.errors import AppendOnlyViolationError, DuplicateEventError, MetadataValidationError
from gemini_token_usage.ingestion.parser import parse_usage_jsonl
from gemini_token_usage.ingestion.schemas import SourceCheckpoint


def test_parse_usage_jsonl_applies_checkpoint_with_model_tiebreak(tmp_path: Path) -> None:
    """Checkpoint filter should keep same timestamp rows with model >= checkpoint model."""
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(project_id),
            _api_response("2026-02-17T00:00:00Z", "gemini-a"),
            _api_response("2026-02-17T00:01:00Z", "gemini-b"),
            _api_response("2026-02-17T00:01:00Z", "gemini-c"),
        ],
    )

    checkpoint = SourceCheckpoint(
        last_event_timestamp=datetime(2026, 2, 17, 0, 1, tzinfo=UTC),
        last_model_code="gemini-b",
    )
    parsed = parse_usage_jsonl(jsonl_file, expected_project_id=project_id, checkpoint=checkpoint)

    assert parsed.usage_events_total == 3
    assert parsed.usage_events_skipped_before_checkpoint == 1
    assert [(row.event_timestamp.isoformat(), row.model_code) for row in parsed.usage_rows] == [
        ("2026-02-17T00:01:00+00:00", "gemini-b"),
        ("2026-02-17T00:01:00+00:00", "gemini-c"),
    ]


def test_parse_usage_jsonl_fails_on_duplicate_event_keys(tmp_path: Path) -> None:
    """Duplicate `(event_timestamp, model_code)` keys should fail."""
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(project_id),
            _api_response("2026-02-17T00:00:00Z", "gemini-2.5-pro"),
            _api_response("2026-02-17T00:00:00Z", "gemini-2.5-pro"),
        ],
    )

    with pytest.raises(DuplicateEventError):
        _ = parse_usage_jsonl(jsonl_file, expected_project_id=project_id, checkpoint=None)


def test_parse_usage_jsonl_fails_when_file_tail_regresses(tmp_path: Path) -> None:
    """When file max key is behind checkpoint tuple, parser should fail."""
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(project_id),
            _api_response("2026-02-17T00:00:00Z", "gemini-a"),
        ],
    )
    checkpoint = SourceCheckpoint(
        last_event_timestamp=datetime(2026, 2, 17, 0, 1, tzinfo=UTC),
        last_model_code="gemini-z",
    )

    with pytest.raises(AppendOnlyViolationError):
        _ = parse_usage_jsonl(jsonl_file, expected_project_id=project_id, checkpoint=checkpoint)


def test_parse_usage_jsonl_fails_on_metadata_project_mismatch(tmp_path: Path) -> None:
    """Metadata project_id mismatch should fail fast."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(UUID("00000000-0000-0000-0000-000000000001")),
            _api_response("2026-02-17T00:00:00Z", "gemini-a"),
        ],
    )

    with pytest.raises(MetadataValidationError):
        _ = parse_usage_jsonl(
            jsonl_file,
            expected_project_id=UUID("00000000-0000-0000-0000-000000000002"),
            checkpoint=None,
        )


def _metadata(project_id: UUID) -> dict[str, object]:
    return {
        "record_type": "gemini_cli.project_metadata",
        "schema_version": 1,
        "project_id": str(project_id),
    }


def _api_response(timestamp: str, model_code: str) -> dict[str, object]:
    return {
        "attributes": {
            "event.name": "gemini_cli.api_response",
            "event.timestamp": timestamp,
            "model": model_code,
            "input_token_count": 10,
            "cached_content_token_count": 2,
            "output_token_count": 5,
            "thoughts_token_count": 1,
            "total_token_count": 16,
        }
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("wb") as handle:
        for row in rows:
            handle.write(orjson.dumps(row))
            handle.write(b"\n")
