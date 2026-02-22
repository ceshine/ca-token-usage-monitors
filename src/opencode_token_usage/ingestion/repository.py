"""DuckDB repository for OpenCode ingestion persistence."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from .schemas import MessageUsageRow, SessionRow, SourceCheckpoint


class IngestionRepository:
    """DuckDB-backed repository for OpenCode sessions and message usage rows."""

    def __init__(self, database_path: Path) -> None:
        self._connection = duckdb.connect(str(database_path))

    def close(self) -> None:
        """Close DuckDB connection."""
        self._connection.close()

    def ensure_schema(self) -> None:
        """Create OpenCode ingestion tables when missing."""
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS opencode_sessions (
    session_id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    project_worktree VARCHAR,
    session_title VARCHAR NOT NULL,
    session_directory VARCHAR NOT NULL,
    session_version VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS opencode_message_usage (
    message_id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    message_created_at TIMESTAMPTZ NOT NULL,
    message_completed_at TIMESTAMPTZ,
    provider_code VARCHAR,
    model_code VARCHAR,
    agent VARCHAR,
    mode VARCHAR,
    finish_reason VARCHAR,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL,
    cache_write_tokens BIGINT NOT NULL,
    total_tokens BIGINT,
    cost_usd DOUBLE,
    source_time_updated_ms BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )
        _ = self._connection.execute(
            """
CREATE INDEX IF NOT EXISTS idx_opencode_usage_checkpoint
ON opencode_message_usage (source_time_updated_ms, message_id)
            """
        )
        _ = self._connection.execute(
            """
CREATE INDEX IF NOT EXISTS idx_opencode_usage_project_updated
ON opencode_message_usage (project_id, source_time_updated_ms)
            """
        )
        _ = self._connection.execute(
            """
CREATE INDEX IF NOT EXISTS idx_opencode_usage_model_updated
ON opencode_message_usage (model_code, source_time_updated_ms)
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

    def get_checkpoint(self) -> SourceCheckpoint | None:
        """Read latest ingestion checkpoint from usage table."""
        row = self._connection.execute(
            """
SELECT source_time_updated_ms, message_id
FROM opencode_message_usage
ORDER BY source_time_updated_ms DESC, message_id DESC
LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return SourceCheckpoint(last_time_updated_ms=int(row[0]), last_message_id=str(row[1]))

    def upsert_sessions(self, rows: list[SessionRow]) -> None:
        """Upsert session dimension rows by `session_id`."""
        if not rows:
            return
        _ = self._connection.executemany(
            """
INSERT INTO opencode_sessions (
    session_id,
    project_id,
    project_worktree,
    session_title,
    session_directory,
    session_version
)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (session_id)
DO UPDATE SET
    project_id = EXCLUDED.project_id,
    project_worktree = EXCLUDED.project_worktree,
    session_title = EXCLUDED.session_title,
    session_directory = EXCLUDED.session_directory,
    session_version = EXCLUDED.session_version,
    updated_at = NOW()
            """,
            [
                [
                    row.session_id,
                    row.project_id,
                    row.project_worktree,
                    row.session_title,
                    row.session_directory,
                    row.session_version,
                ]
                for row in rows
            ],
        )

    def upsert_message_usage(self, rows: list[MessageUsageRow]) -> None:
        """Upsert assistant message usage rows by `message_id`."""
        if not rows:
            return

        _ = self._connection.executemany(
            """
INSERT INTO opencode_message_usage (
    message_id,
    session_id,
    project_id,
    message_created_at,
    message_completed_at,
    provider_code,
    model_code,
    agent,
    mode,
    finish_reason,
    input_tokens,
    output_tokens,
    reasoning_tokens,
    cache_read_tokens,
    cache_write_tokens,
    total_tokens,
    cost_usd,
    source_time_updated_ms
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (message_id)
DO UPDATE SET
    session_id = EXCLUDED.session_id,
    project_id = EXCLUDED.project_id,
    message_created_at = EXCLUDED.message_created_at,
    message_completed_at = EXCLUDED.message_completed_at,
    provider_code = EXCLUDED.provider_code,
    model_code = EXCLUDED.model_code,
    agent = EXCLUDED.agent,
    mode = EXCLUDED.mode,
    finish_reason = EXCLUDED.finish_reason,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    reasoning_tokens = EXCLUDED.reasoning_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    cache_write_tokens = EXCLUDED.cache_write_tokens,
    total_tokens = EXCLUDED.total_tokens,
    cost_usd = EXCLUDED.cost_usd,
    source_time_updated_ms = EXCLUDED.source_time_updated_ms,
    ingested_at = NOW()
            """,
            [
                [
                    row.message_id,
                    row.session_id,
                    row.project_id,
                    row.message_created_at,
                    row.message_completed_at,
                    row.provider_code,
                    row.model_code,
                    row.agent,
                    row.mode,
                    row.finish_reason,
                    row.input_tokens,
                    row.output_tokens,
                    row.reasoning_tokens,
                    row.cache_read_tokens,
                    row.cache_write_tokens,
                    row.total_tokens,
                    row.cost_usd,
                    row.source_time_updated_ms,
                ]
                for row in rows
            ],
        )
