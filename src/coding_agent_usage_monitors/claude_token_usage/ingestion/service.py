"""Service orchestration for Claude Code token ingestion."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from .errors import (
    ParseError,
    SessionIdentityError,
)
from .parser import derive_project_name, discover_session_roots, parse_session_file, parse_session_identity
from .repository import IngestionRepository
from .schemas import IngestionCounters, IngestionFileState

LOGGER = logging.getLogger(__name__)


class IngestionService:
    """Coordinates discovery, parsing, and persistence for Claude Code sessions."""

    def __init__(self, repository: IngestionRepository, session_roots: list[Path] | None = None) -> None:
        self._repository: IngestionRepository = repository
        self._session_roots: list[Path] = session_roots if session_roots is not None else discover_session_roots()

    def ingest(self) -> IngestionCounters:
        """Run ingestion over session files and return operation counters.

        Returns:
            IngestionCounters with summary of ingestion run.
        """
        self._repository.ensure_schema()
        counters = IngestionCounters()

        for session_file_path in discover_session_files(self._session_roots):
            counters.files_scanned += 1
            current_file_state = _build_file_state(session_file_path)
            prior_file_state = self._repository.get_file_state(str(session_file_path))

            if _file_state_matches(prior_file_state, current_file_state):
                counters.files_skipped_unchanged += 1
                continue

            try:
                session_id, slug, cwd, version, agent_id = parse_session_identity(session_file_path)
                project_name = derive_project_name(session_file_path, self._session_roots)
                checkpoint = self._repository.get_session_checkpoint(session_id, agent_id)
                parsed = parse_session_file(
                    session_file_path,
                    session_id=session_id,
                    checkpoint=checkpoint,
                    project_name=project_name,
                    slug=slug,
                    cwd=cwd,
                    version=version,
                )

                with self._repository.transaction():
                    self._repository.upsert_session_metadata(parsed.metadata)
                    self._repository.insert_usage_events(parsed.usage_rows)
                    self._repository.upsert_file_state(current_file_state)

                counters.files_ingested += 1
                counters.sessions_ingested += 1
                counters.usage_rows_raw += parsed.usage_rows_raw
                counters.usage_rows_deduped += len(parsed.usage_rows)
                counters.usage_rows_skipped_synthetic += parsed.usage_rows_skipped_synthetic
                counters.usage_rows_skipped_before_checkpoint += parsed.usage_rows_skipped_before_checkpoint
                counters.duplicate_rows_skipped += parsed.duplicate_rows_skipped
            except (ParseError, SessionIdentityError) as exc:
                counters.parse_errors += 1
                counters.failed_files.append(str(session_file_path))
                LOGGER.error("Failed to ingest %s: %s", session_file_path, exc)

        return counters


def discover_session_files(session_roots: list[Path]) -> list[Path]:
    """Discover JSONL session files across all roots in sorted path order.

    Args:
        session_roots: List of root directories to search.

    Returns:
        Sorted list of JSONL file paths.
    """
    files: list[Path] = []
    for root in session_roots:
        if not root.exists():
            continue
        files.extend(path for path in root.rglob("*.jsonl") if path.is_file())
    return sorted(files)


def _build_file_state(session_file_path: Path) -> IngestionFileState:
    """Build file state from filesystem metadata.

    Args:
        session_file_path: Path to the session file.

    Returns:
        IngestionFileState with current file metadata.
    """
    stat_result = session_file_path.stat()
    return IngestionFileState(
        session_file_path=str(session_file_path),
        file_size_bytes=stat_result.st_size,
        file_mtime=datetime.fromtimestamp(stat_result.st_mtime, tz=UTC),
    )


def _file_state_matches(existing: IngestionFileState | None, current: IngestionFileState) -> bool:
    """Return True when prior file state exactly matches current metadata.

    Args:
        existing: Previously recorded file state, or None.
        current: Current file state from filesystem.

    Returns:
        True if file is unchanged.
    """
    if existing is None:
        return False
    return existing.file_size_bytes == current.file_size_bytes and existing.file_mtime == current.file_mtime
