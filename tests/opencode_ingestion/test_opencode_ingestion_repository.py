"""Tests for OpenCode ingestion repository."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from opencode_token_usage.ingestion.repository import IngestionRepository
from opencode_token_usage.ingestion.schemas import MessageUsageRow, SessionRow


def test_repository_upserts_sessions_and_usage_and_updates_checkpoint(tmp_path: Path) -> None:
    """Repository should upsert by primary keys and expose latest checkpoint tuple."""
    database_path = tmp_path / "usage.duckdb"
    repository = IngestionRepository(database_path)
    repository.ensure_schema()

    session_row = SessionRow(
        session_id="s1",
        project_id="p1",
        project_worktree="/tmp/project",
        session_title="Session One",
        session_directory="/tmp/project",
        session_version="1.2.0",
    )
    usage_row = MessageUsageRow(
        message_id="m1",
        session_id="s1",
        project_id="p1",
        message_created_at=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
        message_completed_at=datetime(2026, 2, 22, 0, 1, tzinfo=UTC),
        provider_code="openai",
        model_code="gpt-5",
        agent="assistant",
        mode="default",
        finish_reason="stop",
        input_tokens=10,
        output_tokens=5,
        reasoning_tokens=2,
        cache_read_tokens=1,
        cache_write_tokens=0,
        total_tokens=18,
        cost_usd=0.01,
        source_time_updated_ms=1000,
    )

    with repository.transaction():
        repository.upsert_sessions([session_row])
        repository.upsert_message_usage([usage_row])

    # Update same message id via upsert.
    updated_usage = MessageUsageRow(
        message_id="m1",
        session_id="s1",
        project_id="p1",
        message_created_at=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
        message_completed_at=datetime(2026, 2, 22, 0, 2, tzinfo=UTC),
        provider_code="openai",
        model_code="gpt-5",
        agent="assistant",
        mode="default",
        finish_reason="length",
        input_tokens=11,
        output_tokens=6,
        reasoning_tokens=3,
        cache_read_tokens=1,
        cache_write_tokens=1,
        total_tokens=22,
        cost_usd=0.02,
        source_time_updated_ms=2000,
    )
    with repository.transaction():
        repository.upsert_sessions([session_row])
        repository.upsert_message_usage([updated_usage])

    checkpoint = repository.get_checkpoint()
    assert checkpoint is not None
    assert checkpoint.last_time_updated_ms == 2000
    assert checkpoint.last_message_id == "m1"

    connection = duckdb.connect(str(database_path))
    try:
        row_count = connection.execute("SELECT COUNT(*) FROM opencode_message_usage").fetchone()[0]
        assert row_count == 1

        row = connection.execute(
            """
            SELECT input_tokens, output_tokens, finish_reason, source_time_updated_ms
            FROM opencode_message_usage
            WHERE message_id = 'm1'
            """
        ).fetchone()
        assert row == (11, 6, "length", 2000)
    finally:
        connection.close()
        repository.close()
