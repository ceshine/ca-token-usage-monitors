"""Tests for source bookkeeping orchestration."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from gemini_token_usage.ingestion.repository import IngestionRepository
from gemini_token_usage.ingestion.source_bookkeeping import SourceBookkeepingService


def test_source_bookkeeping_auto_deactivates_missing_active_sources(tmp_path: Path) -> None:
    """Missing active sources should be deactivated when auto-deactivate is enabled."""
    repository = IngestionRepository(tmp_path / "usage.duckdb")
    repository.ensure_schema()

    missing_project_id = UUID("00000000-0000-0000-0000-000000000001")
    existing_project_id = UUID("00000000-0000-0000-0000-000000000002")
    existing_file = tmp_path / "telemetry.jsonl"
    existing_file.write_text("{}", encoding="utf-8")
    repository.insert_source(
        project_id=missing_project_id, jsonl_file_path=str(tmp_path / "missing.jsonl"), active=True
    )
    repository.insert_source(project_id=existing_project_id, jsonl_file_path=str(existing_file), active=True)

    service = SourceBookkeepingService(repository=repository)
    selection = service.resolve_all_active_paths(auto_deactivate=True)

    assert selection.sources_auto_deactivated == 1
    assert selection.sources_missing == 0
    assert selection.jsonl_paths == [existing_file.resolve()]
    deactivated_source = repository.get_source_by_project_id(missing_project_id)
    assert deactivated_source is not None
    assert deactivated_source.active is False

    repository.close()


def test_source_bookkeeping_counts_missing_without_auto_deactivate(tmp_path: Path) -> None:
    """Missing active sources should be counted but remain active without auto-deactivate."""
    repository = IngestionRepository(tmp_path / "usage.duckdb")
    repository.ensure_schema()

    missing_project_id = UUID("00000000-0000-0000-0000-000000000001")
    repository.insert_source(
        project_id=missing_project_id, jsonl_file_path=str(tmp_path / "missing.jsonl"), active=True
    )

    service = SourceBookkeepingService(repository=repository)
    selection = service.resolve_all_active_paths(auto_deactivate=False)

    assert selection.sources_auto_deactivated == 0
    assert selection.sources_missing == 1
    assert selection.jsonl_paths == []
    still_active_source = repository.get_source_by_project_id(missing_project_id)
    assert still_active_source is not None
    assert still_active_source.active is True

    repository.close()
