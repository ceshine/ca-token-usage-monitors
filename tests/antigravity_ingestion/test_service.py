"""Tests for AntiGravity ingestion service orchestration."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import UTC, datetime

import duckdb

from tests.antigravity_ingestion.test_parser import (
    encode_bytes,
    encode_string,
    encode_model_usage,
)
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.schemas import UsageEventRow
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.service import IngestionService
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.repository import IngestionRepository

TS = datetime(2026, 6, 23, 10, 0, 0, tzinfo=UTC)


def test_successful_ingestion_and_idempotency(tmp_path: Path) -> None:
    """Verify successful ingestion flow and idempotency across repeated runs."""
    db_dir = tmp_path / "conversations"
    db_dir.mkdir()
    valid_db = db_dir / "session_good.db"

    conn = sqlite3.connect(str(valid_db))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")
    usage = encode_model_usage(model_id=312, input_tokens=100, visible_output_tokens=50, response_id="r1")
    chat = encode_bytes(4, usage) + encode_string(19, "gemini 2.5 flash")
    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [encode_bytes(1, chat)])
    conn.commit()
    conn.close()

    duck_conn = duckdb.connect(":memory:")
    repo = IngestionRepository(duck_conn)
    service = IngestionService(repo)

    counters1 = service.ingest(override_env={"ANTIGRAVITY_DATA_DIR": str(db_dir)})
    assert counters1.databases_discovered == 1
    assert counters1.databases_parsed == 1
    assert counters1.databases_failed == 0
    assert counters1.raw_events_extracted == 1
    assert counters1.canonical_events_written == 1

    db_events1 = duck_conn.execute("SELECT event_key, model_code FROM antigravity_usage_events").fetchall()
    assert len(db_events1) == 1
    assert db_events1[0] == ("response:r1", "gemini-2.5-flash")

    # Second run is idempotent
    counters2 = service.ingest(override_env={"ANTIGRAVITY_DATA_DIR": str(db_dir)})
    assert counters2.canonical_events_written == 1

    db_events2 = duck_conn.execute("SELECT event_key, model_code FROM antigravity_usage_events").fetchall()
    assert db_events2 == db_events1


def test_no_discovered_databases_empties_table(tmp_path: Path) -> None:
    """Verify run with zero discovered databases successfully empties canonical table."""
    duck_conn = duckdb.connect(":memory:")
    repo = IngestionRepository(duck_conn)
    repo.replace_all_events(
        [
            UsageEventRow(
                event_key="old_key",
                discovery_source="cli",
                session_id="old",
                event_timestamp=TS,
                model_code="gemini-2.5-flash",
                provider_id=None,
                input_tokens=10,
                output_tokens=10,
                reasoning_tokens=0,
                total_output_tokens=10,
                cache_creation_tokens=0,
                cache_read_tokens=0,
                total_tokens=20,
                response_id=None,
                provider_assigned_message_id=None,
                message_id=None,
            )
        ]
    )

    service = IngestionService(repo)
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir()

    counters = service.ingest(override_env={"ANTIGRAVITY_DATA_DIR": str(empty_dir)})
    assert counters.databases_discovered == 0
    assert counters.canonical_events_written == 0

    count = duck_conn.execute("SELECT COUNT(*) FROM antigravity_usage_events").fetchone()[0]
    assert count == 0


def test_failed_database_preserves_prior_canonical_table(tmp_path: Path) -> None:
    """Verify a database failure leaves prior DuckDB table unchanged."""
    db_dir = tmp_path / "conversations"
    db_dir.mkdir()

    # Good database
    good_db = db_dir / "session_good.db"
    conn = sqlite3.connect(str(good_db))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")
    usage = encode_model_usage(model_id=312, input_tokens=100, visible_output_tokens=50, response_id="r1")
    chat = encode_bytes(4, usage) + encode_string(19, "gemini 2.5 flash")
    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [encode_bytes(1, chat)])
    conn.commit()
    conn.close()

    # Bad database (missing gen_metadata)
    bad_db = db_dir / "session_bad.db"
    conn_bad = sqlite3.connect(str(bad_db))
    conn_bad.execute("CREATE TABLE wrong_table (id INT)")
    conn_bad.commit()
    conn_bad.close()

    duck_conn = duckdb.connect(":memory:")
    repo = IngestionRepository(duck_conn)

    # Pre-populate prior table
    prior_row = UsageEventRow(
        event_key="prior_key",
        discovery_source="cli",
        session_id="prior",
        event_timestamp=TS,
        model_code="gemini-2.5-flash",
        provider_id=None,
        input_tokens=5,
        output_tokens=5,
        reasoning_tokens=0,
        total_output_tokens=5,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=10,
        response_id=None,
        provider_assigned_message_id=None,
        message_id=None,
    )
    repo.replace_all_events([prior_row])

    service = IngestionService(repo)
    counters = service.ingest(override_env={"ANTIGRAVITY_DATA_DIR": str(db_dir)})

    assert counters.databases_discovered == 2
    assert counters.databases_parsed == 1
    assert counters.databases_failed == 1
    assert len(counters.failed_databases) == 1
    assert str(bad_db.resolve()) in counters.failed_databases[0]

    # Canonical table remains unchanged with prior_row
    rows = duck_conn.execute("SELECT event_key FROM antigravity_usage_events").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "prior_key"
