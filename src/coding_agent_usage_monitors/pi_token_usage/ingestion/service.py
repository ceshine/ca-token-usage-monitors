"""Service orchestration for Pi agent token ingestion."""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import UTC, datetime

from .errors import ParseError
from .parser import parse_session_file, discover_session_root, discover_session_files, parse_session_identity
from .schemas import IngestionCounters, IngestionFileState
from .repository import IngestionRepository

LOGGER = logging.getLogger(__name__)


class IngestionService:
    """Coordinates discovery, parsing, and persistence for Pi agent sessions."""

    def __init__(
        self,
        repository: IngestionRepository,
        session_root: Path | None = None,
    ) -> None:
        self._repository: IngestionRepository = repository
        self._session_root: Path = discover_session_root(session_root)

    def ingest(self, full_refresh: bool = False) -> IngestionCounters:
        """Run ingestion over Pi session files and return counters.

        Args:
            full_refresh: When True, bypass both the file-state cache and
                per-session checkpoint. Existing DB rows are kept via
                ON CONFLICT DO NOTHING.

        Returns:
            IngestionCounters summarising the run.
        """
        self._repository.ensure_schema()
        counters = IngestionCounters()

        for session_file_path in discover_session_files(self._session_root):
            counters.files_scanned += 1
            current_file_state = _build_file_state(session_file_path)
            prior_file_state = self._repository.get_file_state(str(session_file_path))

            if not full_refresh and _file_state_matches(prior_file_state, current_file_state):
                counters.files_skipped_unchanged += 1
                continue

            try:
                self._ingest_one_file(
                    session_file_path=session_file_path,
                    current_file_state=current_file_state,
                    full_refresh=full_refresh,
                    counters=counters,
                )
            except ParseError as exc:
                counters.parse_errors += 1
                counters.failed_files.append(str(session_file_path))
                LOGGER.error("Failed to ingest %s: %s", session_file_path, exc)

        return counters

    def _ingest_one_file(
        self,
        session_file_path: Path,
        current_file_state: IngestionFileState,
        full_refresh: bool,
        counters: IngestionCounters,
    ) -> None:
        """Parse one session file and persist results in a single transaction."""
        identity = parse_session_identity(session_file_path)
        session_id = identity[0].session_id
        checkpoint = None if full_refresh else self._repository.get_session_checkpoint(session_id)

        parsed = parse_session_file(session_file_path, checkpoint=checkpoint, identity=identity)

        with self._repository.transaction():
            self._repository.upsert_session_metadata(parsed.metadata)
            self._repository.insert_usage_events(parsed.usage_rows)
            self._repository.upsert_file_state(current_file_state)

        counters.files_ingested += 1
        counters.sessions_ingested += 1
        counters.usage_rows_raw += parsed.usage_rows_raw
        counters.usage_rows_persisted += len(parsed.usage_rows)
        counters.usage_rows_skipped_before_checkpoint += parsed.usage_rows_skipped_before_checkpoint
        if parsed.cwd_recovered_from_path:
            counters.sessions_cwd_recovered_from_path += 1


def _build_file_state(session_file_path: Path) -> IngestionFileState:
    """Build file state from filesystem metadata."""
    stat_result = session_file_path.stat()
    return IngestionFileState(
        session_file_path=str(session_file_path),
        file_size_bytes=stat_result.st_size,
        file_mtime=datetime.fromtimestamp(stat_result.st_mtime, tz=UTC),
    )


def _file_state_matches(existing: IngestionFileState | None, current: IngestionFileState) -> bool:
    """Return True when prior file state exactly matches current metadata."""
    if existing is None:
        return False
    return existing.file_size_bytes == current.file_size_bytes and existing.file_mtime == current.file_mtime
