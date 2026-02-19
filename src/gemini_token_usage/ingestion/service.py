"""Service orchestration for Gemini JSONL ingestion."""

from __future__ import annotations

import shlex
from uuid import UUID
from pathlib import Path
from datetime import UTC, datetime
from typing import Callable, final

from .errors import PathResolutionError, SourceConflictError
from .parser import parse_usage_jsonl
from .repository import IngestionRepository
from .schemas import IngestionCounters, IngestionSourceRow, JsonlFileState, ResolvedInputPath, SourceCheckpoint
from .source_bookkeeping import SourceBookkeepingService


@final
class IngestionService:
    """Coordinates path resolution, reconciliation, parsing, and persistence."""

    def __init__(
        self,
        repository: IngestionRepository,
        source_bookkeeping: SourceBookkeepingService | None = None,
        confirm_new_source: Callable[[Path, UUID], bool] | None = None,
        confirm_reactivate: Callable[[IngestionSourceRow], bool] | None = None,
        confirm_project_path_move: Callable[[IngestionSourceRow, Path], bool] | None = None,
    ) -> None:
        self._repository = repository
        self._source_bookkeeping = source_bookkeeping or SourceBookkeepingService(
            repository=repository,
            confirm_new_source=confirm_new_source,
            confirm_reactivate=confirm_reactivate,
            confirm_project_path_move=confirm_project_path_move,
        )

    def ingest(
        self,
        input_paths: list[Path],
    ) -> IngestionCounters:
        """Run ingestion and return operation counters."""
        self._repository.ensure_schema()
        _raise_if_active_project_collision(self._repository)

        if not input_paths:
            raise PathResolutionError("No input paths provided. Pass one or more paths or use --all-active.")

        counters = IngestionCounters()
        resolved_positional_paths = _resolve_input_paths(input_paths)
        for resolved_path in resolved_positional_paths:
            jsonl_file_path = resolved_path.jsonl_file_path
            counters.sources_scanned += 1
            source_row = self._source_bookkeeping.reconcile_source(jsonl_file_path)
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
