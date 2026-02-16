"""DuckDB repository for token ingestion persistence."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator
from uuid import UUID

import duckdb

from .schemas import IngestionFileState, SessionCheckpoint, SessionMetadataRow, TokenEventRow

LOGGER = logging.getLogger(__name__)


class IngestionRepository:
    """DuckDB-backed repository for ingestion state and token rows."""

    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path
        self._connection = duckdb.connect(str(database_path))

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def ensure_schema(self) -> None:
        """Create ingestion tables when missing."""
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS codex_session_metadata (
    session_id UUID PRIMARY KEY,
    session_timestamp TIMESTAMPTZ,
    cwd VARCHAR,
    session_file_path VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS codex_session_details (
    session_id UUID NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_line_number BIGINT NOT NULL,
    model_code VARCHAR,
    turn_id UUID,
    total_tokens_cumulative BIGINT NOT NULL,
    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_output_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, total_tokens_cumulative)
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS codex_ingestion_files (
    session_file_path VARCHAR PRIMARY KEY,
    file_size_bytes BIGINT NOT NULL,
    file_mtime TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Open a DB transaction scope."""
        _ = self._connection.execute("BEGIN TRANSACTION")
        try:
            yield
        except Exception:
            _ = self._connection.execute("ROLLBACK")
            raise
        else:
            _ = self._connection.execute("COMMIT")

    def get_file_state(self, session_file_path: str) -> IngestionFileState | None:
        """Fetch ingestion file bookkeeping row by file path."""
        row = self._connection.execute(
            """
SELECT file_size_bytes, CAST(file_mtime AS VARCHAR)
FROM codex_ingestion_files
WHERE session_file_path = ?
            """,
            [session_file_path],
        ).fetchone()
        if row is None:
            return None
        return IngestionFileState(
            session_file_path=session_file_path,
            file_size_bytes=int(row[0]),
            file_mtime=_parse_db_timestamp(row[1]),
        )

    def get_session_checkpoint(self, session_id: UUID) -> SessionCheckpoint | None:
        """Fetch latest ingestion checkpoint for a session."""
        row = self._connection.execute(
            """
SELECT CAST(event_timestamp AS VARCHAR), total_tokens_cumulative
FROM codex_session_details
WHERE session_id = ?
ORDER BY event_timestamp DESC, total_tokens_cumulative DESC
LIMIT 1
            """,
            [str(session_id)],
        ).fetchone()
        if row is None:
            return None
        return SessionCheckpoint(
            last_ts=_parse_db_timestamp(row[0]),
            last_total_tokens_cumulative=int(row[1]),
        )

    def upsert_session_metadata(self, metadata: SessionMetadataRow) -> None:
        """Insert or update session metadata row."""
        _ = self._connection.execute(
            """
INSERT INTO codex_session_metadata (
    session_id,
    session_timestamp,
    cwd,
    session_file_path
)
VALUES (?, ?, ?, ?)
ON CONFLICT (session_id)
DO UPDATE SET
    session_timestamp = EXCLUDED.session_timestamp,
    cwd = EXCLUDED.cwd,
    session_file_path = EXCLUDED.session_file_path,
    ingested_at = NOW()
""",
            [
                str(metadata.session_id),
                metadata.session_timestamp,
                metadata.cwd,
                metadata.session_file_path,
            ],
        )

    def insert_session_details(self, token_rows: list[TokenEventRow]) -> None:
        """Insert deduped token rows using conflict-ignore semantics."""
        if not token_rows:
            return

        _ = self._connection.executemany(
            """
INSERT INTO codex_session_details (
    session_id,
    event_timestamp,
    event_line_number,
    model_code,
    turn_id,
    total_tokens_cumulative,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    reasoning_output_tokens,
    total_tokens
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session_id, total_tokens_cumulative) DO NOTHING
            """,
            [
                [
                    str(row.session_id),
                    row.event_timestamp,
                    row.event_line_number,
                    row.model_code,
                    str(row.turn_id) if row.turn_id is not None else None,
                    row.total_tokens_cumulative,
                    row.last_usage.input_tokens,
                    row.last_usage.cached_input_tokens,
                    row.last_usage.output_tokens,
                    row.last_usage.reasoning_output_tokens,
                    row.last_usage.total_tokens,
                ]
                for row in token_rows
            ],
        )

    def upsert_file_state(self, file_state: IngestionFileState) -> None:
        """Insert or update file ingestion bookkeeping row."""
        _ = self._connection.execute(
            """
INSERT INTO codex_ingestion_files (
    session_file_path,
    file_size_bytes,
    file_mtime
)
VALUES (?, ?, ?)
ON CONFLICT (session_file_path)
DO UPDATE SET
    file_size_bytes = EXCLUDED.file_size_bytes,
    file_mtime = EXCLUDED.file_mtime,
    ingested_at = NOW()
            """,
            [
                file_state.session_file_path,
                file_state.file_size_bytes,
                file_state.file_mtime,
            ],
        )


def _parse_db_timestamp(value: str) -> datetime:
    """Parse DuckDB TIMESTAMPTZ string output into an aware datetime."""
    if not isinstance(value, str):
        raise TypeError(f"Expected timestamp string from DB, got {type(value).__name__}")
    normalized = value.replace(" ", "T")
    if normalized.endswith("+00"):
        normalized = f"{normalized}:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
