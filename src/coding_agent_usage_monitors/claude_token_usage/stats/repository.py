"""DuckDB repository for Claude token usage statistics queries."""

from __future__ import annotations

from pathlib import Path

import duckdb

from coding_agent_usage_monitors.common.database import parse_db_timestamp

from .schemas import TokenUsageEvent


class StatsRepositoryError(RuntimeError):
    """Raised when stats queries cannot be executed."""


class StatsRepository:
    """Read-only repository for Claude token usage events."""

    def __init__(self, database_path: Path) -> None:
        self._connection = duckdb.connect(str(database_path), read_only=True)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Load token events from Claude ingestion table."""
        try:
            rows = self._connection.execute(
                """
SELECT
    COALESCE(model_code, 'unknown') AS model_code,
    CAST(event_timestamp AS VARCHAR) AS event_timestamp,
    input_tokens,
    output_tokens,
    cache_creation_input_tokens,
    cache_read_input_tokens
FROM claude_usage_events
ORDER BY event_timestamp, model_code
                """
            ).fetchall()
        except duckdb.Error as exc:
            raise StatsRepositoryError(
                "Failed to query claude_usage_events. Run `claude-token-usage ingest` first."
            ) from exc

        events: list[TokenUsageEvent] = []
        for row in rows:
            event_timestamp = parse_db_timestamp(row[1])
            if event_timestamp is None:
                continue
            events.append(
                TokenUsageEvent(
                    model_code=str(row[0]),
                    event_timestamp=event_timestamp,
                    input_tokens=int(row[2]),
                    output_tokens=int(row[3]),
                    cache_creation_input_tokens=int(row[4]),
                    cache_read_input_tokens=int(row[5]),
                )
            )
        return events
