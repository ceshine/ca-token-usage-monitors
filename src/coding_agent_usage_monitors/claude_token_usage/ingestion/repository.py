"""DuckDB repository for Claude Code token ingestion persistence."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator

import duckdb

from coding_agent_usage_monitors.common.database import parse_db_timestamp
from .schemas import IngestionFileState, SessionCheckpoint, SessionMetadataRow, UsageEventRow

LOGGER = logging.getLogger(__name__)


class IngestionRepository:
    """DuckDB-backed repository for ingestion state and usage rows."""

    def __init__(self, database_path: Path) -> None:
        self._database_path: Path = database_path
        self._connection: duckdb.DuckDBPyConnection = duckdb.connect(str(database_path))

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def ensure_schema(self) -> None:
        """Create ingestion tables when missing."""
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS claude_session_metadata (
    session_id VARCHAR PRIMARY KEY,
    project_name VARCHAR,
    slug VARCHAR,
    cwd VARCHAR,
    version VARCHAR,
    session_file_path VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS claude_usage_events (
    session_id VARCHAR NOT NULL,
    message_id VARCHAR NOT NULL,
    request_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_line_number BIGINT NOT NULL,
    model_code VARCHAR NOT NULL,
    is_sidechain BOOLEAN NOT NULL DEFAULT FALSE,
    agent_id VARCHAR,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0,
    cache_read_input_tokens BIGINT NOT NULL DEFAULT 0,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, message_id, request_id)
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS claude_ingestion_files (
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
        """Fetch ingestion file bookkeeping row by file path.

        Args:
            session_file_path: Absolute file path string.

        Returns:
            IngestionFileState if found, None otherwise.
        """
        row = self._connection.execute(
            """
SELECT file_size_bytes, CAST(file_mtime AS VARCHAR)
FROM claude_ingestion_files
WHERE session_file_path = ?
            """,
            [session_file_path],
        ).fetchone()
        if row is None:
            return None
        file_mtime = parse_db_timestamp(row[1])
        if file_mtime is None:
            return None
        return IngestionFileState(
            session_file_path=session_file_path,
            file_size_bytes=int(row[0]),
            file_mtime=file_mtime,
        )

    def get_session_checkpoint(self, session_id: str, agent_id: str | None) -> SessionCheckpoint | None:
        """Fetch latest ingestion checkpoint for a session and agent scope.

        Uses (session_id, agent_id) as the checkpoint key so that subagent
        files (non-None agent_id) maintain independent checkpoints from the
        main session file (agent_id=None).

        Args:
            session_id: Session ID string.
            agent_id: Agent ID string, or None for the main session file.

        Returns:
            SessionCheckpoint if found, None otherwise.
        """
        row = self._connection.execute(
            """
SELECT CAST(event_timestamp AS VARCHAR), message_id, request_id
FROM claude_usage_events
WHERE session_id = ? AND agent_id IS NOT DISTINCT FROM ?
ORDER BY event_timestamp DESC, message_id DESC, request_id DESC
LIMIT 1
            """,
            [session_id, agent_id],
        ).fetchone()
        if row is None:
            return None
        last_ts = parse_db_timestamp(row[0])
        if last_ts is None:
            return None
        return SessionCheckpoint(
            last_ts=last_ts,
            last_message_id=str(row[1]),
            last_request_id=str(row[2]),
        )

    def upsert_session_metadata(self, metadata: SessionMetadataRow) -> None:
        """Insert or update session metadata row.

        Args:
            metadata: Session metadata to persist.
        """
        _ = self._connection.execute(
            """
INSERT INTO claude_session_metadata (
    session_id,
    project_name,
    slug,
    cwd,
    version,
    session_file_path
)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (session_id)
DO UPDATE SET
    project_name = EXCLUDED.project_name,
    slug = EXCLUDED.slug,
    cwd = EXCLUDED.cwd,
    version = EXCLUDED.version,
    session_file_path = EXCLUDED.session_file_path,
    ingested_at = NOW()
""",
            [
                metadata.session_id,
                metadata.project_name,
                metadata.slug,
                metadata.cwd,
                metadata.version,
                metadata.session_file_path,
            ],
        )

    def insert_usage_events(self, usage_rows: list[UsageEventRow]) -> None:
        """Insert deduped usage rows using conflict-ignore semantics.

        Args:
            usage_rows: List of usage event rows to persist.
        """
        if not usage_rows:
            return

        _ = self._connection.executemany(
            """
INSERT INTO claude_usage_events (
    session_id,
    message_id,
    request_id,
    event_timestamp,
    event_line_number,
    model_code,
    is_sidechain,
    agent_id,
    input_tokens,
    output_tokens,
    cache_creation_input_tokens,
    cache_read_input_tokens
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session_id, message_id, request_id) DO NOTHING
            """,
            [
                [
                    row.session_id,
                    row.message_id,
                    row.request_id,
                    row.event_timestamp,
                    row.event_line_number,
                    row.model_code,
                    row.is_sidechain,
                    row.agent_id,
                    row.usage.input_tokens,
                    row.usage.output_tokens,
                    row.usage.cache_creation_input_tokens,
                    row.usage.cache_read_input_tokens,
                ]
                for row in usage_rows
            ],
        )

    def upsert_file_state(self, file_state: IngestionFileState) -> None:
        """Insert or update file ingestion bookkeeping row.

        Args:
            file_state: File state to persist.
        """
        _ = self._connection.execute(
            """
INSERT INTO claude_ingestion_files (
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
