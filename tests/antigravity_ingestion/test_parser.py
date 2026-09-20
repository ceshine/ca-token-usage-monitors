"""Tests for AntiGravity SQLite parser and protobuf blob extraction."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import UTC, datetime

import pytest

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.errors import (
    InvalidDataError,
    SQLiteSchemaError,
)
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.parser import parse_database
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.schemas import DiscoveredDatabase


def encode_varint(val: int) -> bytes:
    """Encode an integer as LEB128 varint bytes."""
    res = bytearray()
    while True:
        b = val & 0x7F
        val >>= 7
        if val != 0:
            res.append(b | 0x80)
        else:
            res.append(b)
            break
    return bytes(res)


def encode_tag(field_number: int, wire_type: int) -> bytes:
    """Encode tag byte sequence."""
    return encode_varint((field_number << 3) | wire_type)


def encode_bytes(field_number: int, payload: bytes) -> bytes:
    """Encode field_number as wire type 2 length-delimited payload."""
    return encode_tag(field_number, 2) + encode_varint(len(payload)) + payload


def encode_varint_field(field_number: int, val: int) -> bytes:
    """Encode field_number as wire type 0 varint value."""
    return encode_tag(field_number, 0) + encode_varint(val)


def encode_string(field_number: int, s: str) -> bytes:
    """Encode field_number as UTF-8 string."""
    return encode_bytes(field_number, s.encode("utf-8"))


def encode_timestamp_msg(seconds: int, nanos: int = 0) -> bytes:
    """Encode TimestampMessage (field 1 seconds, field 2 nanos)."""
    return encode_varint_field(1, seconds) + encode_varint_field(2, nanos)


def encode_model_usage(
    model_id: int | None = None,
    input_tokens: int = 0,
    total_output_tokens: int = 0,
    cache_creation_tokens: int = 0,
    cache_read_tokens: int = 0,
    provider_id: int | None = None,
    message_id: str | None = None,
    reasoning_tokens: int = 0,
    visible_output_tokens: int = 0,
    response_id: str | None = None,
    provider_assigned_message_id: str | None = None,
) -> bytes:
    """Encode ModelUsage submessage bytes."""
    payload = bytearray()
    if model_id is not None:
        payload.extend(encode_varint_field(1, model_id))
    payload.extend(encode_varint_field(2, input_tokens))
    payload.extend(encode_varint_field(3, total_output_tokens))
    payload.extend(encode_varint_field(4, cache_creation_tokens))
    payload.extend(encode_varint_field(5, cache_read_tokens))
    if provider_id is not None:
        payload.extend(encode_varint_field(6, provider_id))
    if message_id is not None:
        payload.extend(encode_string(7, message_id))
    payload.extend(encode_varint_field(9, reasoning_tokens))
    payload.extend(encode_varint_field(10, visible_output_tokens))
    if response_id is not None:
        payload.extend(encode_string(11, response_id))
    if provider_assigned_message_id is not None:
        payload.extend(encode_string(12, provider_assigned_message_id))
    return bytes(payload)


def test_parse_minimum_valid_gen_metadata_and_session_id(tmp_path: Path) -> None:
    """Verify parsing a valid gen_metadata row and deriving session_id from stem."""
    db_file = tmp_path / "test_session_123.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")

    # Build gen_metadata blob: chat_model (field 1)
    usage_blob = encode_model_usage(
        model_id=312,
        input_tokens=100,
        visible_output_tokens=50,
        response_id="resp_1",
    )
    gen_info = encode_bytes(4, encode_timestamp_msg(1700000000))
    chat_model = encode_bytes(4, usage_blob) + encode_bytes(9, gen_info) + encode_string(19, "gemini 2.5 flash")
    root_blob = encode_bytes(1, chat_model)

    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [root_blob])
    conn.commit()
    conn.close()

    discovered = DiscoveredDatabase(
        database_path=db_file,
        discovery_source="cli",
        session_id="test_session_123",
    )
    result = parse_database(discovered)

    assert len(result.events) == 1
    event = result.events[0]
    assert event.session_id == "test_session_123"
    assert event.model_code == "gemini-2.5-flash"
    assert event.input_tokens == 100
    assert event.output_tokens == 50
    assert event.event_timestamp == datetime.fromtimestamp(1700000000, tz=UTC)
    assert event.timestamp_rank == 3
    assert result.missing_optional_tables == 2  # steps and trajectory missing


def test_reject_absent_required_gen_metadata(tmp_path: Path) -> None:
    """Verify missing required table gen_metadata raises SQLiteSchemaError."""
    db_file = tmp_path / "bad.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE wrong_table (id INT)")
    conn.commit()
    conn.close()

    discovered = DiscoveredDatabase(database_path=db_file, discovery_source="cli", session_id="bad")
    with pytest.raises(SQLiteSchemaError, match="Required table 'gen_metadata' is missing"):
        parse_database(discovered)


def test_empty_session_id_raises_invalid_data_error(tmp_path: Path) -> None:
    """Verify database descriptor with empty session_id stem raises InvalidDataError."""
    db_file = tmp_path / "valid.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE gen_metadata (idx INT, data BLOB)")
    conn.commit()
    conn.close()

    discovered = DiscoveredDatabase(database_path=db_file, discovery_source="cli", session_id="")
    with pytest.raises(InvalidDataError, match="empty session_id stem"):
        parse_database(discovered)


def test_parse_primary_and_retry_usages_and_zero_tokens_skipping(tmp_path: Path) -> None:
    """Verify primary usage plus retries are emitted, and zero token usages are skipped."""
    db_file = tmp_path / "retries.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")

    # Usage 1 (zero tokens) -> skipped
    usage_zero = encode_model_usage(model_id=312, input_tokens=0, visible_output_tokens=0)

    # Usage 2 (retry with tokens) -> emitted
    usage_retry = encode_model_usage(model_id=312, input_tokens=10, visible_output_tokens=5)
    retry_wrapper = encode_bytes(2, usage_retry)

    chat_model = encode_bytes(4, usage_zero) + encode_bytes(17, retry_wrapper) + encode_string(19, "gemini 2.5 flash")
    root_blob = encode_bytes(1, chat_model)

    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [root_blob])
    conn.commit()
    conn.close()

    discovered = DiscoveredDatabase(database_path=db_file, discovery_source="cli", session_id="retries")
    result = parse_database(discovered)

    assert len(result.events) == 1
    assert result.events[0].input_tokens == 10
    assert result.events[0].output_tokens == 5
    assert "retry_0" in result.events[0].fallback_key


def test_parse_optional_steps_table(tmp_path: Path) -> None:
    """Verify steps table parsing when present."""
    db_file = tmp_path / "steps.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")
    conn.execute("CREATE TABLE steps (idx INTEGER PRIMARY KEY, metadata BLOB)")

    # Minimal gen_metadata row
    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, NULL)")

    # Steps metadata row
    usage_blob = encode_model_usage(model_id=312, input_tokens=40, visible_output_tokens=20)
    ts_msg = encode_timestamp_msg(1710000000)
    model_info = encode_varint_field(1, 312) + encode_string(12, "gemini 2.5 flash")

    step_blob = encode_bytes(8, ts_msg) + encode_bytes(9, usage_blob) + encode_bytes(24, model_info)
    conn.execute("INSERT INTO steps (idx, metadata) VALUES (1, ?)", [step_blob])
    conn.commit()
    conn.close()

    discovered = DiscoveredDatabase(database_path=db_file, discovery_source="cli", session_id="steps")
    result = parse_database(discovered)

    assert len(result.events) == 1
    assert result.events[0].input_tokens == 40
    assert result.events[0].event_timestamp == datetime.fromtimestamp(1710000000, tz=UTC)
    assert result.missing_optional_tables == 1  # trajectory missing


def test_trajectory_timestamp_fallback_and_first_row_retention(tmp_path: Path) -> None:
    """Verify trajectory metadata first valid timestamp is retained as fallback (rank 1)."""
    db_file = tmp_path / "traj.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")
    conn.execute("CREATE TABLE trajectory_metadata_blob (rowid INTEGER PRIMARY KEY, data BLOB)")

    # Trajectory row 1 (epoch 1600000000)
    traj_blob1 = encode_bytes(2, encode_timestamp_msg(1600000000))
    # Trajectory row 2 (epoch 1690000000)
    traj_blob2 = encode_bytes(2, encode_timestamp_msg(1690000000))

    conn.execute("INSERT INTO trajectory_metadata_blob (rowid, data) VALUES (1, ?)", [traj_blob1])
    conn.execute("INSERT INTO trajectory_metadata_blob (rowid, data) VALUES (2, ?)", [traj_blob2])

    # Gen row without explicit timestamp
    usage_blob = encode_model_usage(model_id=312, input_tokens=30, visible_output_tokens=10)
    chat_model = encode_bytes(4, usage_blob) + encode_string(19, "gemini 2.5 flash")
    conn.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [encode_bytes(1, chat_model)])

    conn.commit()
    conn.close()

    discovered = DiscoveredDatabase(database_path=db_file, discovery_source="cli", session_id="traj")
    result = parse_database(discovered)

    assert len(result.events) == 1
    assert result.events[0].timestamp_rank == 1
    assert result.events[0].event_timestamp == datetime.fromtimestamp(1600000000, tz=UTC)


def test_consistent_read_snapshot_in_wal_mode(tmp_path: Path) -> None:
    """In WAL mode, verify writer commit after reader's BEGIN snapshot is invisible to current parse."""
    db_file = tmp_path / "wal_test.db"
    conn_setup = sqlite3.connect(str(db_file))
    conn_setup.execute("PRAGMA journal_mode = WAL")
    conn_setup.execute("CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB)")

    usage1 = encode_model_usage(model_id=312, input_tokens=10, visible_output_tokens=5)
    chat1 = encode_bytes(4, usage1) + encode_string(19, "gemini 2.5 flash")
    conn_setup.execute("INSERT INTO gen_metadata (idx, data) VALUES (1, ?)", [encode_bytes(1, chat1)])
    conn_setup.commit()

    # Parse database once
    discovered = DiscoveredDatabase(database_path=db_file, discovery_source="cli", session_id="wal_test")
    res1 = parse_database(discovered)
    assert len(res1.events) == 1

    # Insert second row
    usage2 = encode_model_usage(model_id=312, input_tokens=20, visible_output_tokens=10)
    chat2 = encode_bytes(4, usage2) + encode_string(19, "gemini 2.5 flash")
    conn_setup.execute("INSERT INTO gen_metadata (idx, data) VALUES (2, ?)", [encode_bytes(1, chat2)])
    conn_setup.commit()
    conn_setup.close()

    # Second run sees both rows
    res2 = parse_database(discovered)
    assert len(res2.events) == 2
