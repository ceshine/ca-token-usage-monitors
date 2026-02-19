"""Tests for Gemini ingestion repository primitives."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from gemini_token_usage.ingestion.repository import IngestionRepository


def test_repository_deactivate_sources_updates_active_rows_only(tmp_path: Path) -> None:
    """Bulk deactivation should update only active rows and return affected count."""
    repository = IngestionRepository(tmp_path / "usage.duckdb")
    repository.ensure_schema()

    active_project_id = UUID("00000000-0000-0000-0000-000000000001")
    inactive_project_id = UUID("00000000-0000-0000-0000-000000000002")
    repository.insert_source(project_id=active_project_id, jsonl_file_path="/tmp/active.jsonl", active=True)
    repository.insert_source(project_id=inactive_project_id, jsonl_file_path="/tmp/inactive.jsonl", active=False)

    updated_rows = repository.deactivate_sources([active_project_id, inactive_project_id, active_project_id])

    assert updated_rows == 1
    active_source = repository.get_source_by_project_id(active_project_id)
    assert active_source is not None
    assert active_source.active is False
    inactive_source = repository.get_source_by_project_id(inactive_project_id)
    assert inactive_source is not None
    assert inactive_source.active is False

    repository.close()
