"""Integration tests for Gemini ingestion service."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import duckdb
import orjson
import pytest

from gemini_token_usage.ingestion.errors import ConfirmationDeclinedError
from gemini_token_usage.ingestion.repository import IngestionRepository
from gemini_token_usage.ingestion.service import IngestionService


def test_ingestion_service_is_idempotent_and_checkpoint_resumable(tmp_path: Path) -> None:
    """Ingestion should skip unchanged files and resume from tuple checkpoint."""
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(project_id),
            _api_response("2026-02-17T00:00:00Z", "gemini-a"),
            _api_response("2026-02-17T00:01:00Z", "gemini-b"),
        ],
    )

    repository = IngestionRepository(tmp_path / "usage.duckdb")
    service = IngestionService(repository=repository)

    first = service.ingest([jsonl_file], include_all_active=False, auto_deactivate=False)
    assert first.sources_scanned == 1
    assert first.sources_ingested == 1
    assert first.sources_skipped_unchanged == 0
    assert first.usage_rows_attempted_insert == 2

    second = service.ingest([jsonl_file], include_all_active=False, auto_deactivate=False)
    assert second.sources_scanned == 1
    assert second.sources_ingested == 0
    assert second.sources_skipped_unchanged == 1

    _append_jsonl(jsonl_file, [_api_response("2026-02-17T00:01:00Z", "gemini-c")])
    os.utime(jsonl_file, (jsonl_file.stat().st_atime + 10, jsonl_file.stat().st_mtime + 10))

    third = service.ingest([jsonl_file], include_all_active=False, auto_deactivate=False)
    assert third.sources_scanned == 1
    assert third.sources_ingested == 1
    assert third.usage_events_total == 3
    assert third.usage_events_skipped_before_checkpoint == 1
    assert third.usage_rows_attempted_insert == 2

    connection = duckdb.connect(str(tmp_path / "usage.duckdb"))
    try:
        count = connection.execute("SELECT COUNT(*) FROM gemini_usage_events").fetchone()[0]
        assert count == 3
        rows = connection.execute(
            """
            SELECT model_code
            FROM gemini_usage_events
            ORDER BY event_timestamp, model_code
            """
        ).fetchall()
        assert rows == [("gemini-a",), ("gemini-b",), ("gemini-c",)]
    finally:
        connection.close()
        repository.close()


def test_ingestion_service_auto_deactivates_missing_active_sources(tmp_path: Path) -> None:
    """`--all-active --auto-deactivate` should mark missing source paths inactive."""
    repository = IngestionRepository(tmp_path / "usage.duckdb")
    repository.ensure_schema()
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    repository.insert_source(project_id=project_id, jsonl_file_path=str(tmp_path / "missing.jsonl"), active=True)

    service = IngestionService(repository=repository)
    counters = service.ingest([], include_all_active=True, auto_deactivate=True)

    assert counters.sources_auto_deactivated == 1
    source = repository.get_source_by_project_id(project_id)
    assert source is not None
    assert source.active is False
    repository.close()


def test_ingestion_service_fails_when_confirmation_declined(tmp_path: Path) -> None:
    """Declined new-source confirmation should fail with non-success signal."""
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(project_id),
            _api_response("2026-02-17T00:00:00Z", "gemini-a"),
        ],
    )
    repository = IngestionRepository(tmp_path / "usage.duckdb")
    service = IngestionService(repository=repository, confirm_new_source=lambda _path, _project_id: False)

    with pytest.raises(ConfirmationDeclinedError):
        _ = service.ingest([jsonl_file], include_all_active=False, auto_deactivate=False)

    repository.close()


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
            "cached_content_token_count": 1,
            "output_token_count": 5,
            "thoughts_token_count": 0,
            "total_token_count": 15,
        }
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("wb") as handle:
        for row in rows:
            handle.write(orjson.dumps(row))
            handle.write(b"\n")


def _append_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("ab") as handle:
        for row in rows:
            handle.write(orjson.dumps(row))
            handle.write(b"\n")
