"""DuckDB repository for OpenCode token usage statistics queries."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .schemas import TokenUsageEvent


class StatsRepositoryError(RuntimeError):
    """Raised when stats queries cannot be executed."""


class StatsRepository:
    """Read-only repository for OpenCode token usage events."""

    def __init__(self, database_path: Path) -> None:
        self._connection = duckdb.connect(str(database_path), read_only=True)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Load token events from OpenCode ingestion details table."""
        try:
            rows = self._connection.execute(
                """
SELECT
    COALESCE(provider_code, 'unknown') AS provider_code,
    COALESCE(model_code, 'unknown') AS model_code,
    CAST(COALESCE(message_completed_at, message_created_at) AS VARCHAR) AS event_timestamp,
    input_tokens,
    cache_read_tokens,
    cache_write_tokens,
    output_tokens,
    reasoning_tokens
FROM opencode_message_usage
ORDER BY event_timestamp, provider_code, model_code
                """
            ).fetchall()
        except duckdb.Error as exc:
            raise StatsRepositoryError(
                "Failed to query opencode_message_usage. Run `opencode-token-usage ingest` first."
            ) from exc

        events: list[TokenUsageEvent] = []
        for row in rows:
            events.append(
                TokenUsageEvent(
                    provider_code=str(row[0]),
                    model_code=str(row[1]),
                    event_timestamp=_parse_db_timestamp(row[2]),
                    input_tokens=int(row[3]),
                    cache_read_tokens=int(row[4]),
                    cache_write_tokens=int(row[5]),
                    output_tokens=int(row[6]),
                    reasoning_tokens=int(row[7]),
                )
            )
        return events


def _parse_db_timestamp(value: str) -> datetime:
    """Parse DuckDB TIMESTAMPTZ string output into an aware datetime."""
    if not isinstance(value, str):
        raise TypeError(f"Expected timestamp string from DB, got {type(value).__name__}.")
    normalized = value.replace(" ", "T")
    if normalized.endswith("+00"):
        normalized = f"{normalized}:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
