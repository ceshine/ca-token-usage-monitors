"""Source bookkeeping orchestration for Gemini ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from uuid import UUID

from ..preprocessing.metadata import read_project_metadata
from .errors import ConfirmationDeclinedError, MetadataValidationError, SourceConflictError
from .repository import IngestionRepository
from .schemas import IngestionSourceRow

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveSourceSelection:
    """Resolved active source paths and bookkeeping counters."""

    jsonl_paths: list[Path]
    sources_missing: int
    sources_auto_deactivated: int


class SourceBookkeepingService:
    """Coordinates source lifecycle reconciliation and active-source selection."""

    def __init__(
        self,
        repository: IngestionRepository,
        confirm_new_source: Callable[[Path, UUID], bool] | None = None,
        confirm_reactivate: Callable[[IngestionSourceRow], bool] | None = None,
        confirm_project_path_move: Callable[[IngestionSourceRow, Path], bool] | None = None,
    ) -> None:
        self._repository = repository
        self._confirm_new_source = confirm_new_source or (lambda _path, _project_id: True)
        self._confirm_reactivate = confirm_reactivate or (lambda _source: True)
        self._confirm_project_path_move = confirm_project_path_move or (lambda _source, _path: True)

    def resolve_all_active_paths(self, auto_deactivate: bool) -> ActiveSourceSelection:
        """Resolve currently active source rows to existing JSONL paths."""
        active_sources = self._repository.list_active_sources()
        sources_auto_deactivated = 0

        if auto_deactivate:
            missing_project_ids = [
                source.project_id for source in active_sources if not Path(source.jsonl_file_path).exists()
            ]
            sources_auto_deactivated = self._repository.deactivate_sources(missing_project_ids)
            if sources_auto_deactivated > 0:
                LOGGER.info("Auto-deactivated %d missing active sources.", sources_auto_deactivated)
            active_sources = self._repository.list_active_sources()

        sources_missing = 0
        resolved_paths: dict[str, Path] = {}
        for source in active_sources:
            source_path = Path(source.jsonl_file_path)
            if not source_path.exists():
                sources_missing += 1
                LOGGER.warning("Active source does not exist: %s", source.jsonl_file_path)
                continue
            canonical_path = source_path.resolve()
            resolved_paths[str(canonical_path)] = canonical_path

        return ActiveSourceSelection(
            jsonl_paths=list(resolved_paths.values()),
            sources_missing=sources_missing,
            sources_auto_deactivated=sources_auto_deactivated,
        )

    def reconcile_source(self, jsonl_file_path: Path) -> IngestionSourceRow:
        """Find or create/refresh tracked source row for one JSONL path."""
        path_key = str(jsonl_file_path.resolve())
        metadata = _read_metadata_for_reconciliation(jsonl_file_path)
        source_by_path = self._repository.get_source_by_path(path_key)
        source_by_project = self._repository.get_source_by_project_id(metadata.project_id)

        if source_by_path is not None and source_by_path.project_id != metadata.project_id:
            raise SourceConflictError(
                (
                    f"Tracked source path {jsonl_file_path} maps to project_id {source_by_path.project_id}, "
                    f"but metadata contains {metadata.project_id}."
                )
            )

        if source_by_path is not None:
            resolved_source = source_by_path
        elif source_by_project is not None:
            self._handle_project_path_move(source_by_project, jsonl_file_path)
            refreshed = self._repository.get_source_by_project_id(source_by_project.project_id)
            assert refreshed is not None
            resolved_source = refreshed
        else:
            if not self._confirm_new_source(jsonl_file_path, metadata.project_id):
                raise ConfirmationDeclinedError(f"Source registration declined for {jsonl_file_path}.")
            self._repository.insert_source(project_id=metadata.project_id, jsonl_file_path=path_key, active=True)
            refreshed = self._repository.get_source_by_project_id(metadata.project_id)
            assert refreshed is not None
            resolved_source = refreshed

        if not resolved_source.active:
            if not self._confirm_reactivate(resolved_source):
                raise ConfirmationDeclinedError(f"Source reactivation declined for {resolved_source.jsonl_file_path}.")
            self._repository.set_source_active(resolved_source.project_id, True)
            refreshed = self._repository.get_source_by_project_id(resolved_source.project_id)
            assert refreshed is not None
            resolved_source = refreshed

        return resolved_source

    def _handle_project_path_move(self, existing_source: IngestionSourceRow, new_path: Path) -> None:
        old_path = Path(existing_source.jsonl_file_path)
        if old_path != new_path and _old_path_still_valid_for_project(old_path, existing_source.project_id):
            raise SourceConflictError(
                (
                    "Detected multiple valid paths for the same project_id. "
                    f"Existing path: {old_path}. New path: {new_path}. "
                    "Resolve this manually before ingestion."
                )
            )
        if not self._confirm_project_path_move(existing_source, new_path):
            raise ConfirmationDeclinedError(
                (
                    "Project path update declined for project_id="
                    f"{existing_source.project_id}: {existing_source.jsonl_file_path} -> {new_path}"
                )
            )
        self._repository.update_source_path(existing_source.project_id, str(new_path))


def _read_metadata_for_reconciliation(jsonl_file_path: Path):
    try:
        return read_project_metadata(jsonl_file_path)
    except (FileNotFoundError, ValueError) as exc:
        raise MetadataValidationError(str(exc)) from exc


def _old_path_still_valid_for_project(path: Path, project_id: UUID) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        metadata = read_project_metadata(path)
    except (FileNotFoundError, ValueError):
        return False
    return metadata.project_id == project_id
