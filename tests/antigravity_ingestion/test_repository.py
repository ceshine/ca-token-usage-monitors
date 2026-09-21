"""Tests for AntiGravity DuckDB ingestion repository."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.schemas import UsageEventRow
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.repository import IngestionRepository

TS = datetime(2026, 6, 23, 10, 0, 0, tzinfo=UTC)


def test_ensure_schema_and_replace_all_events() -> None:
    """Verify ensure_schema creates table and replace_all_events performs full transactional replacement."""
    conn = duckdb.connect(":memory:")
    repo = IngestionRepository(conn)

    row1 = UsageEventRow(
        event_key="response:resp1",
        discovery_source="cli",
        session_id="session1",
        event_timestamp=TS,
        model_code="gemini-2.5-flash",
        provider_id=3,
        input_tokens=100,
        output_tokens=50,
        reasoning_tokens=0,
        total_output_tokens=50,
        cache_creation_tokens=10,
        cache_read_tokens=5,
        total_tokens=165,
        response_id="resp1",
        provider_assigned_message_id=None,
        message_id=None,
    )

    repo.replace_all_events([row1])

    rows = conn.execute("SELECT event_key, model_code, total_tokens FROM antigravity_usage_events").fetchall()
    assert len(rows) == 1
    assert rows[0] == ("response:resp1", "gemini-2.5-flash", 165)

    # Second replacement with empty list
    repo.replace_all_events([])
    rows_after = conn.execute("SELECT COUNT(*) FROM antigravity_usage_events").fetchone()
    assert rows_after[0] == 0

    repo.close()
