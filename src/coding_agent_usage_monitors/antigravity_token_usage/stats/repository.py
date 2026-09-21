"""DuckDB repository for AntiGravity token usage statistics queries."""

from __future__ import annotations

from pathlib import Path

import duckdb

from coding_agent_usage_monitors.common.database import parse_db_timestamp

from .schemas import TokenUsageEvent


class StatsRepositoryError(RuntimeError):
    """Raised when stats queries cannot be executed."""


class StatsRepository:
    """Read-only repository for canonical AntiGravity usage events."""

    def __init__(self, database_path: Path) -> None:
        """Open the usage database in read-only mode.

        Args:
            database_path: Path to the DuckDB usage database.
        """
        self._connection = duckdb.connect(str(database_path), read_only=True)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._connection.close()

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Load canonical token events from the AntiGravity usage table.

        Returns:
            Parsed canonical token usage events with a valid timestamp.

        Raises:
            StatsRepositoryError: If the canonical usage table cannot be queried.
        """
        # TODO: Push the since/until bounds into this query instead of loading the whole table.
        # StatsService currently filters by local reporting date after timezone conversion, so a
        # SQL-side filter needs to apply the same timezone offset to event_timestamp first.
        try:
            rows = self._connection.execute(
                """
SELECT
    COALESCE(model_code, 'unknown') AS model_code,
    CAST(event_timestamp AS VARCHAR) AS event_timestamp,
    input_tokens,
    output_tokens,
    reasoning_tokens,
    cache_creation_tokens,
    cache_read_tokens
FROM antigravity_usage_events
ORDER BY event_timestamp, model_code
                """
            ).fetchall()
        except duckdb.Error as exc:
            raise StatsRepositoryError(
                "Failed to query antigravity_usage_events. Run `agy-token-usage ingest` first."
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
                    reasoning_tokens=int(row[4]),
                    cache_creation_tokens=int(row[5]),
                    cache_read_tokens=int(row[6]),
                )
            )
        return events
