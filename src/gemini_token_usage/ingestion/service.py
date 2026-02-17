"""Service orchestration for Gemini JSONL ingestion."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
from pathlib import Path
import shlex
from typing import Callable
from uuid import UUID

from ..preprocessing.metadata import read_project_metadata

from .errors import (
    ConfirmationDeclinedError,
    MetadataValidationError,
    PathResolutionError,
    SourceConflictError,
)
from .parser import parse_usage_jsonl
from .repository import IngestionRepository
from .schemas import IngestionCounters, IngestionSourceRow, JsonlFileState, ResolvedInputPath, SourceCheckpoint

LOGGER = logging.getLogger(__name__)


class IngestionService:
    """Coordinates path resolution, reconciliation, parsing, and persistence."""

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

    def ingest(
        self,
        input_paths: list[Path],
        include_all_active: bool,
        auto_deactivate: bool,
    ) -> IngestionCounters:
        """Run ingestion and return operation counters."""
        self._repository.ensure_schema()
        _raise_if_active_project_collision(self._repository)

        if not input_paths and not include_all_active:
            raise PathResolutionError("No input paths provided. Pass one or more paths or use --all-active.")

        counters = IngestionCounters()
        resolved_positional_paths = _resolve_input_paths(input_paths)
        candidate_paths: dict[str, Path] = {}
        for resolved in resolved_positional_paths:
            candidate_paths[str(resolved.jsonl_file_path)] = resolved.jsonl_file_path

        active_sources = self._repository.list_active_sources() if include_all_active else []
        if include_all_active and auto_deactivate:
            for source in active_sources:
                source_path = Path(source.jsonl_file_path)
                if source_path.exists():
                    continue
                self._repository.set_source_active(source.project_id, False)
                counters.sources_auto_deactivated += 1
                LOGGER.info("Marked inactive missing source: %s", source.jsonl_file_path)
            active_sources = self._repository.list_active_sources()

        for source in active_sources:
            source_path = Path(source.jsonl_file_path)
            if not source_path.exists():
                counters.sources_missing += 1
                LOGGER.warning("Active source does not exist: %s", source.jsonl_file_path)
                continue
            candidate_paths[str(source_path.resolve())] = source_path.resolve()

        for jsonl_file_path in candidate_paths.values():
            counters.sources_scanned += 1
            source_row = self._reconcile_source(jsonl_file_path)
            _raise_if_active_project_collision(self._repository)

            current_state = _build_file_state(jsonl_file_path)
            if _file_state_matches(source_row, current_state):
                counters.sources_skipped_unchanged += 1
                continue

            parsed = parse_usage_jsonl(
                jsonl_file_path=jsonl_file_path,
                expected_project_id=source_row.project_id,
                checkpoint=source_row.checkpoint,
            )
            updated_checkpoint = _resolve_checkpoint(source_row.checkpoint, parsed.max_event_key)
            with self._repository.transaction():
                self._repository.insert_usage_events(parsed.usage_rows)
                self._repository.update_source_bookkeeping(source_row.project_id, current_state, updated_checkpoint)

            counters.sources_ingested += 1
            counters.usage_events_total += parsed.usage_events_total
            counters.usage_events_skipped_before_checkpoint += parsed.usage_events_skipped_before_checkpoint
            counters.usage_rows_attempted_insert += len(parsed.usage_rows)

        return counters

    def _reconcile_source(self, jsonl_file_path: Path) -> IngestionSourceRow:
        path_key = str(jsonl_file_path)
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


def _resolve_input_paths(input_paths: list[Path]) -> list[ResolvedInputPath]:
    resolved: list[ResolvedInputPath] = []
    seen_paths: set[str] = set()
    for input_path in input_paths:
        resolved_jsonl = resolve_ingest_input_path(input_path)
        canonical_key = str(resolved_jsonl)
        if canonical_key in seen_paths:
            continue
        seen_paths.add(canonical_key)
        resolved.append(ResolvedInputPath(original_path=input_path, jsonl_file_path=resolved_jsonl))
    return resolved


def resolve_ingest_input_path(input_path: Path) -> Path:
    """Resolve a user-supplied path to a canonical preprocessed JSONL file path."""
    expanded_input = input_path.expanduser()
    if expanded_input.is_dir():
        candidates = (
            expanded_input / "telemetry.jsonl",
            expanded_input / ".gemini" / "telemetry.jsonl",
        )
        for candidate in candidates:
            if candidate.is_file():
                return candidate.resolve()
        raise PathResolutionError(_build_missing_jsonl_message(input_path))

    if expanded_input.is_file() and expanded_input.suffix == ".jsonl":
        return expanded_input.resolve()

    raise PathResolutionError(_build_missing_jsonl_message(input_path))


def _build_missing_jsonl_message(original_path: Path) -> str:
    suggestion = f"gemini-token-usage preprocess {shlex.quote(str(original_path))}"
    return f"Preprocessed telemetry.jsonl not found for {original_path}. Run: {suggestion}"


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


def _raise_if_active_project_collision(repository: IngestionRepository) -> None:
    collisions = repository.detect_active_project_collisions()
    if collisions:
        collision_values = ", ".join(str(project_id) for project_id in collisions)
        raise SourceConflictError(f"Duplicate active source rows detected for project IDs: {collision_values}")


def _build_file_state(jsonl_file_path: Path) -> JsonlFileState:
    stat_result = jsonl_file_path.stat()
    return JsonlFileState(
        jsonl_file_path=str(jsonl_file_path),
        file_size_bytes=stat_result.st_size,
        file_mtime=datetime.fromtimestamp(stat_result.st_mtime, tz=UTC),
    )


def _file_state_matches(source: IngestionSourceRow, file_state: JsonlFileState) -> bool:
    if source.file_size_bytes is None or source.file_mtime is None:
        return False
    return source.file_size_bytes == file_state.file_size_bytes and source.file_mtime == file_state.file_mtime


def _resolve_checkpoint(
    existing_checkpoint: SourceCheckpoint | None,
    parsed_max_event_key: tuple[datetime, str] | None,
) -> SourceCheckpoint | None:
    if parsed_max_event_key is None:
        return existing_checkpoint
    parsed_checkpoint = SourceCheckpoint(
        last_event_timestamp=parsed_max_event_key[0],
        last_model_code=parsed_max_event_key[1],
    )
    if existing_checkpoint is None:
        return parsed_checkpoint

    existing_key = (existing_checkpoint.last_event_timestamp, existing_checkpoint.last_model_code)
    parsed_key = (parsed_checkpoint.last_event_timestamp, parsed_checkpoint.last_model_code)
    if parsed_key > existing_key:
        return parsed_checkpoint
    return existing_checkpoint
