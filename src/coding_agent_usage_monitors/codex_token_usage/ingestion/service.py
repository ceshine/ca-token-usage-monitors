"""Service orchestration for Codex token ingestion."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from .dedupe import dedupe_and_validate_token_rows
from .errors import (
    DeltaConsistencyError,
    DuplicateConflictError,
    ModelAttributionError,
    MonotonicityError,
    ParseError,
    SessionIdentityError,
)
from .parser import parse_session_file, parse_session_identity
from .repository import IngestionRepository
from .schemas import IngestionCounters, IngestionFileState

LOGGER = logging.getLogger(__name__)


class IngestionService:
    """Coordinates discovery, parsing, dedupe, and persistence."""

    def __init__(self, repository: IngestionRepository, sessions_root: Path | None = None) -> None:
        self._repository = repository
        self._sessions_root = sessions_root or (Path.home() / ".codex" / "sessions")

    def ingest(self) -> IngestionCounters:
        """Run ingestion over session files and return operation counters."""
        self._repository.ensure_schema()
        counters = IngestionCounters()

        for session_file_path in discover_session_files(self._sessions_root):
            counters.files_scanned += 1
            current_file_state = _build_file_state(session_file_path)
            prior_file_state = self._repository.get_file_state(str(session_file_path))

            if _file_state_matches(prior_file_state, current_file_state):
                counters.files_skipped_unchanged += 1
                continue

            try:
                session_identity = parse_session_identity(session_file_path)
                checkpoint = self._repository.get_session_checkpoint(session_identity.session_id)
                parsed = parse_session_file(session_file_path, session_identity, checkpoint)
                dedupe_result = dedupe_and_validate_token_rows(session_file_path, parsed.token_rows)

                with self._repository.transaction():
                    self._repository.upsert_session_metadata(parsed.metadata)
                    self._repository.insert_session_details(dedupe_result.token_rows)
                    self._repository.upsert_file_state(current_file_state)

                counters.files_ingested += 1
                counters.sessions_ingested += 1
                counters.token_rows_raw += parsed.token_rows_raw
                counters.token_rows_deduped += len(dedupe_result.token_rows)
                counters.token_rows_skipped_info_null += parsed.token_rows_skipped_info_null
                counters.token_rows_skipped_before_checkpoint += parsed.token_rows_skipped_before_checkpoint
                counters.duplicate_rows_skipped += dedupe_result.duplicate_rows_skipped
            except MonotonicityError as exc:
                counters.monotonicity_errors += 1
                counters.failed_files.append(str(session_file_path))
                LOGGER.error("Monotonicity check failed for %s: %s", session_file_path, exc)
            except DeltaConsistencyError as exc:
                counters.delta_consistency_errors += 1
                counters.failed_files.append(str(session_file_path))
                LOGGER.error("Delta consistency check failed for %s: %s", session_file_path, exc)
            except (ParseError, SessionIdentityError, ModelAttributionError, DuplicateConflictError) as exc:
                counters.parse_errors += 1
                counters.failed_files.append(str(session_file_path))
                LOGGER.error("Failed to ingest %s: %s", session_file_path, exc)

        return counters


def discover_session_files(sessions_root: Path) -> list[Path]:
    """Discover JSONL session files in sorted path order."""
    if not sessions_root.exists():
        return []
    return sorted(path for path in sessions_root.rglob("*.jsonl") if path.is_file())


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
