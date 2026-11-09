"""DuckDB repository for AntiGravity token usage persistence."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from .schemas import UsageEventRow

LOGGER = logging.getLogger(__name__)


class IngestionRepository:
    """DuckDB repository for AntiGravity token usage canonical events."""

    def __init__(self, database_path: Path | str | duckdb.DuckDBPyConnection) -> None:
        """Initialize IngestionRepository.

        Args:
            database_path (Path | str | duckdb.DuckDBPyConnection): DuckDB file path or connection.
        """
        if isinstance(database_path, duckdb.DuckDBPyConnection):
            self._connection: duckdb.DuckDBPyConnection = database_path
            self._owned_connection: bool = False
        else:
            self._connection = duckdb.connect(str(database_path))
            self._owned_connection = True

    def close(self) -> None:
        """Close the underlying DuckDB connection if owned."""
        if self._owned_connection:
            self._connection.close()

    def ensure_schema(self) -> None:
        """Create the antigravity_usage_events table if missing and ensure schema migrations."""
        self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS antigravity_usage_events (
    event_key VARCHAR PRIMARY KEY,
    discovery_source VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMPTZ,
    model_code VARCHAR NOT NULL,
    model_id BIGINT,
    provider_id BIGINT,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_tokens BIGINT NOT NULL,
    total_output_tokens BIGINT NOT NULL,
    cache_creation_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    response_id VARCHAR,
    provider_assigned_message_id VARCHAR,
    message_id VARCHAR,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
            """
        )
        self._connection.execute("ALTER TABLE antigravity_usage_events ADD COLUMN IF NOT EXISTS model_id BIGINT;")

    def update_all_events(self, events: list[UsageEventRow]) -> None:
        """Upsert canonical events into DuckDB in a single transaction.

        Args:
            events (list[UsageEventRow]): Deduplicated canonical events to persist.
        """
        self.ensure_schema()
        self._connection.execute("BEGIN TRANSACTION")
        try:
            if events:
                self._connection.executemany(
                    """
INSERT INTO antigravity_usage_events (
    event_key,
    discovery_source,
    session_id,
    event_timestamp,
    model_code,
    model_id,
    provider_id,
    input_tokens,
    output_tokens,
    reasoning_tokens,
    total_output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_tokens,
    response_id,
    provider_assigned_message_id,
    message_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (event_key) DO UPDATE SET
    discovery_source = EXCLUDED.discovery_source,
    session_id = EXCLUDED.session_id,
    event_timestamp = EXCLUDED.event_timestamp,
    model_code = EXCLUDED.model_code,
    model_id = COALESCE(EXCLUDED.model_id, antigravity_usage_events.model_id),
    provider_id = EXCLUDED.provider_id,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    reasoning_tokens = EXCLUDED.reasoning_tokens,
    total_output_tokens = EXCLUDED.total_output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_tokens = EXCLUDED.total_tokens,
    response_id = EXCLUDED.response_id,
    provider_assigned_message_id = EXCLUDED.provider_assigned_message_id,
    message_id = EXCLUDED.message_id,
    ingested_at = NOW();
                    """,
                    [
                        [
                            e.event_key,
                            e.discovery_source,
                            e.session_id,
                            e.event_timestamp,
                            e.model_code,
                            e.model_id,
                            e.provider_id,
                            e.input_tokens,
                            e.output_tokens,
                            e.reasoning_tokens,
                            e.total_output_tokens,
                            e.cache_creation_tokens,
                            e.cache_read_tokens,
                            e.total_tokens,
                            e.response_id,
                            e.provider_assigned_message_id,
                            e.message_id,
                        ]
                        for e in events
                    ],
                )
            self._connection.execute("COMMIT")
        except Exception:
            self._connection.execute("ROLLBACK")
            raise
