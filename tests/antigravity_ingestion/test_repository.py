"""Tests for AntiGravity DuckDB ingestion repository."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.schemas import UsageEventRow
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.repository import IngestionRepository

TS = datetime(2026, 6, 23, 10, 0, 0, tzinfo=UTC)


def test_ensure_schema_and_update_all_events() -> None:
    """Verify ensure_schema creates table and update_all_events performs transactional upsert."""
    conn = duckdb.connect(":memory:")
    repo = IngestionRepository(conn)

    row1 = UsageEventRow(
        event_key="response:resp1",
        discovery_source="cli",
        session_id="session1",
        event_timestamp=TS,
        model_code="gemini-2.5-flash",
        model_id=312,
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

    repo.update_all_events([row1])

    rows = conn.execute("SELECT event_key, model_code, model_id, total_tokens FROM antigravity_usage_events").fetchall()
    assert len(rows) == 1
    assert rows[0] == ("response:resp1", "gemini-2.5-flash", 312, 165)

    # Upsert with empty list preserves existing rows
    repo.update_all_events([])
    rows_after_empty = conn.execute("SELECT COUNT(*) FROM antigravity_usage_events").fetchone()
    assert rows_after_empty is not None
    assert rows_after_empty[0] == 1

    # Upsert with updated row1 (model_id=None preserves existing model_id via COALESCE) and new row2
    row1_updated = UsageEventRow(
        event_key="response:resp1",
        discovery_source="default",
        session_id="session1",
        event_timestamp=TS,
        model_code="gemini-2.5-flash",
        model_id=None,
        provider_id=3,
        input_tokens=120,
        output_tokens=60,
        reasoning_tokens=0,
        total_output_tokens=60,
        cache_creation_tokens=10,
        cache_read_tokens=5,
        total_tokens=195,
        response_id="resp1",
        provider_assigned_message_id=None,
        message_id=None,
    )
    row2 = UsageEventRow(
        event_key="response:resp2",
        discovery_source="cli",
        session_id="session2",
        event_timestamp=TS,
        model_code="gemini-4-argon-medium",
        model_id=1418,
        provider_id=24,
        input_tokens=200,
        output_tokens=80,
        reasoning_tokens=20,
        total_output_tokens=100,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=300,
        response_id="resp2",
        provider_assigned_message_id=None,
        message_id=None,
    )
    repo.update_all_events([row1_updated, row2])

    all_rows = conn.execute(
        "SELECT event_key, discovery_source, model_code, model_id, total_tokens "
        "FROM antigravity_usage_events ORDER BY event_key"
    ).fetchall()
    assert all_rows == [
        ("response:resp1", "default", "gemini-2.5-flash", 312, 195),
        ("response:resp2", "cli", "gemini-4-argon-medium", 1418, 300),
    ]

    repo.close()


def test_legacy_table_schema_alter_on_update_all_events() -> None:
    """Verify legacy table without model_id receives the column via ensure_schema and preserves old rows."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
CREATE TABLE antigravity_usage_events (
    event_key VARCHAR PRIMARY KEY,
    discovery_source VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMPTZ,
    model_code VARCHAR NOT NULL,
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
    conn.execute(
        """
INSERT INTO antigravity_usage_events (
    event_key, discovery_source, session_id, event_timestamp, model_code, provider_id,
    input_tokens, output_tokens, reasoning_tokens, total_output_tokens,
    cache_creation_tokens, cache_read_tokens, total_tokens
) VALUES
    ('legacy_key', 'cli', 'legacy_session', '2026-11-01T00:00:00Z', 'gemini-2.5-flash', 3, 10, 5, 0, 5, 0, 0, 15);
        """
    )

    repo = IngestionRepository(conn)
    new_row = UsageEventRow(
        event_key="new_key",
        discovery_source="cli",
        session_id="new_session",
        event_timestamp=TS,
        model_code="gemini-4-argon-medium",
        model_id=1418,
        provider_id=24,
        input_tokens=50,
        output_tokens=25,
        reasoning_tokens=0,
        total_output_tokens=25,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=75,
        response_id="new_key",
        provider_assigned_message_id=None,
        message_id=None,
    )
    repo.update_all_events([new_row])

    rows = conn.execute(
        "SELECT event_key, model_code, model_id FROM antigravity_usage_events ORDER BY event_key"
    ).fetchall()
    assert rows == [
        ("legacy_key", "gemini-2.5-flash", None),
        ("new_key", "gemini-4-argon-medium", 1418),
    ]
    repo.close()
