"""Tests for Pi agent DuckDB ingestion repository."""

from __future__ import annotations

from pathlib import Path
from datetime import UTC, datetime

from coding_agent_usage_monitors.pi_token_usage.ingestion.schemas import (
    UsageEventRow,
    PiReportedCost,
    IngestionFileState,
    SessionMetadataRow,
)
from coding_agent_usage_monitors.pi_token_usage.ingestion.repository import IngestionRepository


def _make_usage_row(session_id: str, message_id: str, ts: datetime) -> UsageEventRow:
    return UsageEventRow(
        session_id=session_id,
        message_id=message_id,
        parent_id=None,
        event_timestamp=ts,
        event_line_number=2,
        provider_code="opencode",
        model_code="minimax-m2.5-free",
        stop_reason="stop",
        input_tokens=100,
        output_tokens=20,
        cache_read_tokens=0,
        cache_write_tokens=0,
        total_tokens=120,
        reported_cost=PiReportedCost(
            input_usd=0.0,
            output_usd=0.0,
            cache_read_usd=0.0,
            cache_write_usd=0.0,
            total_usd=0.0,
        ),
    )


def test_repository_upserts_and_dedupes(tmp_path: Path) -> None:
    repo = IngestionRepository(tmp_path / "pi.duckdb")
    try:
        repo.ensure_schema()
        ts1 = datetime(2026, 4, 13, 15, 43, tzinfo=UTC)
        ts2 = datetime(2026, 4, 13, 15, 44, tzinfo=UTC)

        metadata = SessionMetadataRow(
            session_id="sess-1",
            session_version=3,
            cwd="/home/alice/work",
            session_started_at=ts1,
            session_file_path="/tmp/fake.jsonl",
        )
        rows = [
            _make_usage_row("sess-1", "m-1", ts1),
            _make_usage_row("sess-1", "m-2", ts2),
        ]
        file_state = IngestionFileState(
            session_file_path="/tmp/fake.jsonl",
            file_size_bytes=123,
            file_mtime=ts2,
        )

        with repo.transaction():
            repo.upsert_session_metadata(metadata)
            repo.insert_usage_events(rows)
            repo.upsert_file_state(file_state)

        # Insert again — PK conflict keeps originals.
        with repo.transaction():
            repo.insert_usage_events(rows)

        checkpoint = repo.get_session_checkpoint("sess-1")
        assert checkpoint is not None
        assert checkpoint.last_message_id == "m-2"
        assert checkpoint.last_ts == ts2

        state = repo.get_file_state("/tmp/fake.jsonl")
        assert state is not None
        assert state.file_size_bytes == 123
    finally:
        repo.close()


def test_repository_checkpoint_none_for_unknown_session(tmp_path: Path) -> None:
    repo = IngestionRepository(tmp_path / "pi.duckdb")
    try:
        repo.ensure_schema()
        assert repo.get_session_checkpoint("never-seen") is None
    finally:
        repo.close()
