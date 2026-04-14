"""Tests for Pi agent session log parser."""

from __future__ import annotations

from pathlib import Path
from datetime import UTC, datetime
from typing import Any

import orjson
import pytest

from coding_agent_usage_monitors.pi_token_usage.ingestion.errors import ParseError
from coding_agent_usage_monitors.pi_token_usage.ingestion.parser import parse_session_file, parse_session_identity
from coding_agent_usage_monitors.pi_token_usage.ingestion.schemas import SessionCheckpoint

SESSION_ID = "11111111-2222-3333-4444-555555555555"


def test_parse_session_identity_extracts_fields(tmp_path: Path) -> None:
    session_file = _build_session_file(tmp_path, cwd="/home/alice/work")

    metadata, recovered = parse_session_identity(session_file)

    assert metadata.session_id == SESSION_ID
    assert metadata.session_version == 3
    assert metadata.cwd == "/home/alice/work"
    assert metadata.session_started_at == datetime(2026, 4, 13, 15, 42, 45, 133000, tzinfo=UTC)
    assert metadata.session_file_path == str(session_file)
    assert recovered is False


def test_parse_session_identity_recovers_cwd_from_parent_dir(tmp_path: Path) -> None:
    project_dir = tmp_path / "--home-alice-work--"
    project_dir.mkdir()
    session_file = project_dir / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    _write_jsonl(
        session_file,
        [_session_entry(cwd=None), _assistant_event("2026-04-13T15:43:00Z", msg_id="m-1")],
    )

    metadata, recovered = parse_session_identity(session_file)

    assert metadata.cwd == "/home/alice/work"
    assert recovered is True


def test_parse_session_identity_fails_when_cwd_missing_and_dir_malformed(tmp_path: Path) -> None:
    malformed_dir = tmp_path / "not-an-encoded-dir"
    malformed_dir.mkdir()
    session_file = malformed_dir / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    _write_jsonl(session_file, [_session_entry(cwd=None)])

    with pytest.raises(ParseError, match="Missing 'cwd'"):
        parse_session_identity(session_file)


def test_parse_session_identity_fails_when_first_line_is_not_session(tmp_path: Path) -> None:
    session_file = tmp_path / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    _write_jsonl(session_file, [_assistant_event("2026-04-13T15:43:00Z", msg_id="m-1")])

    with pytest.raises(ParseError, match="Expected line 1"):
        parse_session_identity(session_file)


def test_parse_session_identity_fails_on_filename_sessionid_mismatch(tmp_path: Path) -> None:
    session_file = tmp_path / "2026-04-13T15-42-45-133Z_99999999-2222-3333-4444-555555555555.jsonl"
    _write_jsonl(session_file, [_session_entry(cwd="/home/alice/work")])

    with pytest.raises(ParseError, match="mismatch"):
        parse_session_identity(session_file)


def test_parse_session_file_extracts_usage_rows(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        assistant_events=[
            _assistant_event(
                "2026-04-13T15:43:00Z",
                msg_id="m-1",
                model="minimax-m2.5-free",
                provider="opencode",
                input_t=3057,
                output_t=73,
                cache_read=0,
                cache_write=0,
                total=3130,
            ),
            _assistant_event(
                "2026-04-13T15:44:00Z",
                msg_id="m-2",
                model="claude-sonnet-4-6",
                input_t=100,
                output_t=50,
                stop_reason="tool_use",
            ),
        ],
    )

    parsed = parse_session_file(session_file)

    assert parsed.metadata.session_id == SESSION_ID
    assert len(parsed.usage_rows) == 2
    assert parsed.usage_rows_raw == 2
    first, second = parsed.usage_rows
    assert first.message_id == "m-1"
    assert first.input_tokens == 3057
    assert first.output_tokens == 73
    assert first.total_tokens == 3130
    assert first.model_code == "minimax-m2.5-free"
    assert first.provider_code == "opencode"
    assert second.stop_reason == "tool_use"


def test_parse_session_file_skips_non_assistant_and_non_message(tmp_path: Path) -> None:
    session_file = tmp_path / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    _write_jsonl(
        session_file,
        [
            _session_entry(cwd="/home/alice/work"),
            {"type": "model_change", "timestamp": "2026-04-13T15:42:50Z"},
            {
                "type": "message",
                "id": "u-1",
                "timestamp": "2026-04-13T15:42:55Z",
                "message": {"role": "user"},
            },
            _assistant_event("2026-04-13T15:43:00Z", msg_id="a-1"),
        ],
    )

    parsed = parse_session_file(session_file)

    assert len(parsed.usage_rows) == 1
    assert parsed.usage_rows[0].message_id == "a-1"


def test_parse_session_file_fails_on_duplicate_id(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        assistant_events=[
            _assistant_event("2026-04-13T15:43:00Z", msg_id="dup"),
            _assistant_event("2026-04-13T15:44:00Z", msg_id="dup"),
        ],
    )

    with pytest.raises(ParseError, match="Duplicate entry id"):
        parse_session_file(session_file)


def test_parse_session_file_fails_on_missing_timestamp(tmp_path: Path) -> None:
    event = _assistant_event("2026-04-13T15:43:00Z", msg_id="a-1")
    del event["timestamp"]
    session_file = _build_session_file(tmp_path, cwd="/home/alice/work", assistant_events=[event])

    with pytest.raises(ParseError, match="Missing required timestamp"):
        parse_session_file(session_file)


def test_parse_session_file_fails_on_missing_input(tmp_path: Path) -> None:
    event = _assistant_event("2026-04-13T15:43:00Z", msg_id="a-1")
    # Remove 'input' from usage
    usage = event["message"]["usage"]
    usage_without_input: dict[str, Any] = {k: v for k, v in usage.items() if k != "input"}
    usage_without_input["output"] = 10
    event["message"]["usage"] = usage_without_input
    # Must still satisfy filter by having both input and output keys present; removing 'input'
    # makes the row silently skipped. Instead test a present-but-non-int case:
    event["message"]["usage"]["input"] = "oops"
    session_file = _build_session_file(tmp_path, cwd="/home/alice/work", assistant_events=[event])

    with pytest.raises(ParseError, match=r"message\.usage\.input"):
        parse_session_file(session_file)


def test_parse_session_file_applies_checkpoint(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        assistant_events=[
            _assistant_event("2026-04-13T15:43:00Z", msg_id="m-1"),
            _assistant_event("2026-04-13T15:44:00Z", msg_id="m-2"),
            _assistant_event("2026-04-13T15:45:00Z", msg_id="m-3"),
        ],
    )
    checkpoint = SessionCheckpoint(
        last_ts=datetime(2026, 4, 13, 15, 44, 0, tzinfo=UTC),
        last_message_id="m-2",
    )

    parsed = parse_session_file(session_file, checkpoint=checkpoint)

    assert parsed.usage_rows_raw == 3
    assert parsed.usage_rows_skipped_before_checkpoint == 2
    assert [row.message_id for row in parsed.usage_rows] == ["m-3"]


def test_parse_session_file_persists_cost_verbatim(tmp_path: Path) -> None:
    event = _assistant_event("2026-04-13T15:43:00Z", msg_id="m-1")
    event["message"]["usage"]["cost"] = {
        "input": 0.01,
        "output": 0.02,
        "cacheRead": 0.003,
        "cacheWrite": 0.004,
        "total": 0.037,
    }
    session_file = _build_session_file(tmp_path, cwd="/home/alice/work", assistant_events=[event])

    parsed = parse_session_file(session_file)

    cost = parsed.usage_rows[0].reported_cost
    assert cost.input_usd == 0.01
    assert cost.output_usd == 0.02
    assert cost.cache_read_usd == 0.003
    assert cost.cache_write_usd == 0.004
    assert cost.total_usd == 0.037


# --- helpers ---


def _build_session_file(
    tmp_path: Path,
    *,
    cwd: str | None,
    assistant_events: list[dict[str, Any]] | None = None,
) -> Path:
    session_file = tmp_path / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    events: list[dict[str, Any]] = [_session_entry(cwd=cwd)]
    if assistant_events:
        events.extend(assistant_events)
    else:
        events.append(_assistant_event("2026-04-13T15:43:00Z", msg_id="default"))
    _write_jsonl(session_file, events)
    return session_file


def _session_entry(*, cwd: str | None) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "session",
        "version": 3,
        "id": SESSION_ID,
        "timestamp": "2026-04-13T15:42:45.133Z",
    }
    if cwd is not None:
        entry["cwd"] = cwd
    return entry


def _assistant_event(
    timestamp: str,
    *,
    msg_id: str,
    model: str = "minimax-m2.5-free",
    provider: str = "opencode",
    api: str = "anthropic-messages",
    input_t: int = 100,
    output_t: int = 20,
    cache_read: int = 0,
    cache_write: int = 0,
    total: int | None = None,
    stop_reason: str | None = "stop",
    parent_id: str | None = None,
) -> dict[str, Any]:
    usage: dict[str, Any] = {
        "input": input_t,
        "output": output_t,
        "cacheRead": cache_read,
        "cacheWrite": cache_write,
    }
    if total is not None:
        usage["totalTokens"] = total
    event: dict[str, Any] = {
        "type": "message",
        "id": msg_id,
        "parentId": parent_id,
        "timestamp": timestamp,
        "message": {
            "role": "assistant",
            "api": api,
            "provider": provider,
            "model": model,
            "stopReason": stop_reason,
            "responseId": f"resp-{msg_id}",
            "usage": usage,
            "timestamp": 0,
        },
    }
    return event


def _write_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    with path.open("wb") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")
