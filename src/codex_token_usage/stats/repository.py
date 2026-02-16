"""DuckDB repository for token usage statistics queries."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .schemas import TokenUsageEvent


class StatsRepositoryError(RuntimeError):
    """Raised when stats queries cannot be executed."""


class StatsRepository:
    """Read-only repository for token usage events."""

    def __init__(self, database_path: Path) -> None:
        self._connection = duckdb.connect(str(database_path), read_only=True)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Load token events from the ingestion details table."""
        try:
            rows = self._connection.execute(
                """
SELECT
    COALESCE(model_code, 'unknown') AS model_code,
    CAST(event_timestamp AS VARCHAR) AS event_timestamp,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    reasoning_output_tokens
FROM codex_session_details
ORDER BY event_timestamp, model_code
                """
            ).fetchall()
        except duckdb.Error as exc:
            raise StatsRepositoryError(
                "Failed to query codex_session_details. Run `codex-token-usage ingest` first."
            ) from exc

        events: list[TokenUsageEvent] = []
        for row in rows:
            events.append(
                TokenUsageEvent(
                    model_code=str(row[0]),
                    event_timestamp=_parse_db_timestamp(row[1]),
                    input_tokens=int(row[2]),
                    cached_input_tokens=int(row[3]),
                    output_tokens=int(row[4]),
                    reasoning_output_tokens=int(row[5]),
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
