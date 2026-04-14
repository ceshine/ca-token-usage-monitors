"""DuckDB repository for Pi agent ingestion persistence."""

from __future__ import annotations

import logging
from pathlib import Path
from contextlib import contextmanager
from collections.abc import Iterator

import duckdb

from coding_agent_usage_monitors.common.database import parse_db_timestamp

from .schemas import UsageEventRow, SessionCheckpoint, IngestionFileState, SessionMetadataRow

LOGGER = logging.getLogger(__name__)


class IngestionRepository:
    """DuckDB-backed repository for Pi sessions and message usage rows."""

    def __init__(self, database_path: Path) -> None:
        self._database_path: Path = database_path
        self._connection: duckdb.DuckDBPyConnection = duckdb.connect(str(database_path))

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def ensure_schema(self) -> None:
        """Create Pi ingestion tables when missing."""
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS pi_session_metadata (
    session_id VARCHAR PRIMARY KEY,
    session_version INTEGER NOT NULL,
    cwd VARCHAR NOT NULL,
    session_started_at TIMESTAMPTZ NOT NULL,
    session_file_path VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS pi_usage_events (
    session_id VARCHAR NOT NULL,
    message_id VARCHAR NOT NULL,
    parent_id VARCHAR,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_line_number BIGINT NOT NULL,
    provider_code VARCHAR,
    model_code VARCHAR,
    stop_reason VARCHAR,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL DEFAULT 0,
    cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT,
    pi_reported_cost_input_usd DOUBLE,
    pi_reported_cost_output_usd DOUBLE,
    pi_reported_cost_cache_read_usd DOUBLE,
    pi_reported_cost_cache_write_usd DOUBLE,
    pi_reported_cost_total_usd DOUBLE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, message_id)
)
            """
        )
        _ = self._connection.execute(
            """
CREATE INDEX IF NOT EXISTS idx_pi_usage_checkpoint
ON pi_usage_events (session_id, event_timestamp, message_id)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS pi_ingestion_files (
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
FROM pi_ingestion_files
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

    def get_session_checkpoint(self, session_id: str) -> SessionCheckpoint | None:
        """Fetch latest ingestion checkpoint for a session."""
        row = self._connection.execute(
            """
SELECT CAST(event_timestamp AS VARCHAR), message_id
FROM pi_usage_events
WHERE session_id = ?
ORDER BY event_timestamp DESC, message_id DESC
LIMIT 1
            """,
            [session_id],
        ).fetchone()
        if row is None:
            return None
        last_ts = parse_db_timestamp(row[0])
        if last_ts is None:
            return None
        return SessionCheckpoint(last_ts=last_ts, last_message_id=str(row[1]))

    def upsert_session_metadata(self, metadata: SessionMetadataRow) -> None:
        """Insert or update session metadata row."""
        _ = self._connection.execute(
            """
INSERT INTO pi_session_metadata (
    session_id,
    session_version,
    cwd,
    session_started_at,
    session_file_path
)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (session_id)
DO UPDATE SET
    session_version = EXCLUDED.session_version,
    cwd = EXCLUDED.cwd,
    session_started_at = EXCLUDED.session_started_at,
    session_file_path = EXCLUDED.session_file_path,
    ingested_at = NOW()
            """,
            [
                metadata.session_id,
                metadata.session_version,
                metadata.cwd,
                metadata.session_started_at,
                metadata.session_file_path,
            ],
        )

    def insert_usage_events(self, usage_rows: list[UsageEventRow]) -> None:
        """Insert usage rows with conflict-ignore semantics on (session_id, message_id)."""
        if not usage_rows:
            return

        _ = self._connection.executemany(
            """
INSERT INTO pi_usage_events (
    session_id,
    message_id,
    parent_id,
    event_timestamp,
    event_line_number,
    provider_code,
    model_code,
    stop_reason,
    input_tokens,
    output_tokens,
    cache_read_tokens,
    cache_write_tokens,
    total_tokens,
    pi_reported_cost_input_usd,
    pi_reported_cost_output_usd,
    pi_reported_cost_cache_read_usd,
    pi_reported_cost_cache_write_usd,
    pi_reported_cost_total_usd
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (session_id, message_id) DO NOTHING
            """,
            [
                [
                    row.session_id,
                    row.message_id,
                    row.parent_id,
                    row.event_timestamp,
                    row.event_line_number,
                    row.provider_code,
                    row.model_code,
                    row.stop_reason,
                    row.input_tokens,
                    row.output_tokens,
                    row.cache_read_tokens,
                    row.cache_write_tokens,
                    row.total_tokens,
                    row.reported_cost.input_usd,
                    row.reported_cost.output_usd,
                    row.reported_cost.cache_read_usd,
                    row.reported_cost.cache_write_usd,
                    row.reported_cost.total_usd,
                ]
                for row in usage_rows
            ],
        )

    def upsert_file_state(self, file_state: IngestionFileState) -> None:
        """Insert or update file ingestion bookkeeping row."""
        _ = self._connection.execute(
            """
INSERT INTO pi_ingestion_files (
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
