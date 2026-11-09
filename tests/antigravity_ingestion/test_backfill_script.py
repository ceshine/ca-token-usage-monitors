"""Tests for the one-time AntiGravity model_id backfill and model_code re-normalization script."""

from __future__ import annotations

import duckdb

from scripts.one_time_scripts.backfill_antigravity_model_id import (
    PRIMARY_MODEL_ID_BY_CODE,
    infer_model_id_from_code,
    backfill_antigravity_model_ids,
)


def test_backfill_antigravity_model_ids_and_renormalize() -> None:
    """Verify legacy table without model_id receives column and backfill script migrates rows."""
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
    ('e1', 'cli', 's1', '2026-11-01T00:00:00Z', 'model_placeholder_m418', 24, 10, 5, 0, 5, 0, 0, 15),
    ('e2', 'cli', 's1', '2026-11-01T00:00:00Z', 'antigravity-model-999', 24, 10, 5, 0, 5, 0, 0, 15),
    ('e3', 'cli', 's1', '2026-11-01T00:00:00Z', 'gemini-3.6-flash-high', 24, 10, 5, 0, 5, 0, 0, 15),
    ('e4', 'cli', 's1', '2026-11-01T00:00:00Z', 'gemini-3.5-flash-high', 24, 10, 5, 0, 5, 0, 0, 15);
        """
    )

    assert PRIMARY_MODEL_ID_BY_CODE["gemini-3.6-flash-high"] == 1071
    assert PRIMARY_MODEL_ID_BY_CODE["gemini-3.5-flash-high"] == 1132
    assert infer_model_id_from_code("model_placeholder_m418") == 1418
    assert infer_model_id_from_code("antigravity-model-999") == 999

    # Dry run rolls back
    dry_stats = backfill_antigravity_model_ids(conn, dry_run=True)
    assert dry_stats.total_rows == 4
    assert dry_stats.model_id_backfilled == 4
    assert dry_stats.model_code_updated == 1

    # Actual run commits
    stats = backfill_antigravity_model_ids(conn, dry_run=False)
    assert stats.total_rows == 4
    assert stats.model_id_backfilled == 4
    assert stats.model_code_updated == 1

    migrated = conn.execute(
        "SELECT event_key, model_code, model_id FROM antigravity_usage_events ORDER BY event_key"
    ).fetchall()
    assert migrated == [
        ("e1", "gemini-4-argon-medium", 1418),
        ("e2", "antigravity-model-999", 999),
        ("e3", "gemini-3.6-flash-high", 1071),
        ("e4", "gemini-3.5-flash-high", 1132),
    ]
    conn.close()
