"""Tests for the one-time AntiGravity model_id backfill and model_code re-normalization script."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import duckdb
import pytest
from typer.testing import CliRunner

from scripts.one_time_scripts.backfill_antigravity_model_id import (
    TYPER_APP,
    backup_database,
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


def test_backup_database_creates_timestamped_file(tmp_path: Path) -> None:
    """Verify backup_database copies the duckdb file and any wal file with YYMMDD-HHmm timestamp."""
    source_db = tmp_path / "data" / "token_usage.duckdb"
    source_db.parent.mkdir(parents=True, exist_ok=True)
    source_db.write_bytes(b"dummy duckdb content")

    wal_file = tmp_path / "data" / "token_usage.duckdb.wal"
    wal_file.write_bytes(b"dummy wal content")

    backup_dir = tmp_path / "cache"
    fixed_time = datetime(2026, 11, 9, 20, 33)

    backup_path = backup_database(database_path=source_db, backup_dir=backup_dir, now=fixed_time)

    expected_backup_path = backup_dir / "token_usage.261109-2033.duckdb"
    assert backup_path == expected_backup_path
    assert backup_path.exists()
    assert backup_path.read_bytes() == b"dummy duckdb content"

    expected_wal_backup = backup_dir / "token_usage.261109-2033.duckdb.wal"
    assert expected_wal_backup.exists()
    assert expected_wal_backup.read_bytes() == b"dummy wal content"


def test_backup_database_raises_when_source_missing(tmp_path: Path) -> None:
    """Verify backup_database raises FileNotFoundError when source does not exist."""
    missing_db = tmp_path / "nonexistent.duckdb"
    backup_dir = tmp_path / "cache"

    with pytest.raises(FileNotFoundError, match="Database file not found"):
        backup_database(database_path=missing_db, backup_dir=backup_dir)


def test_main_cli_execution_creates_backup_and_backfills(tmp_path: Path) -> None:
    """Verify TYPER_APP CLI backs up the database and commits backfill changes."""
    db_file = tmp_path / "my_usage.duckdb"
    conn = duckdb.connect(str(db_file))
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
INSERT INTO antigravity_usage_events (
    event_key, discovery_source, session_id, event_timestamp, model_code, provider_id,
    input_tokens, output_tokens, reasoning_tokens, total_output_tokens,
    cache_creation_tokens, cache_read_tokens, total_tokens
) VALUES
    ('ev1', 'cli', 's1', '2026-11-01T00:00:00Z', 'model_placeholder_m418', 24, 10, 5, 0, 5, 0, 0, 15);
        """
    )
    conn.close()

    backup_dir = tmp_path / "cache"
    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "--database-path",
            str(db_file),
            "--backup-dir",
            str(backup_dir),
        ],
    )
    assert result.exit_code == 0
    assert "Database backup created:" in result.stdout

    # Check backup was created in backup_dir
    backups = list(backup_dir.glob("my_usage.*.duckdb"))
    assert len(backups) == 1

    # Check db was updated
    verify_conn = duckdb.connect(str(db_file))
    rows = verify_conn.execute("SELECT event_key, model_code, model_id FROM antigravity_usage_events").fetchall()
    verify_conn.close()
    assert rows == [("ev1", "gemini-4-argon-medium", 1418)]
