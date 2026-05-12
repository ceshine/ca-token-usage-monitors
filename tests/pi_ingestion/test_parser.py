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
    assert first.source_type == "message"
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


def test_parse_session_file_fails_on_duplicate_id_across_source_types(tmp_path: Path) -> None:
    """Duplicate id across message and branch_summary should be rejected."""
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            _assistant_event("2026-04-13T15:43:00Z", msg_id="dup"),
            _branch_summary_event(
                "2026-04-13T15:44:00Z",
                summary_id="dup",
                input_t=500,
                output_t=200,
            ),
        ],
    )

    with pytest.raises(ParseError, match="Duplicate entry id"):
        parse_session_file(session_file)


# --- branch_summary tests ---


def test_parse_branch_summary_extracts_usage_from_details(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            _branch_summary_event(
                "2026-04-13T15:44:00Z",
                summary_id="bs-1",
                input_t=2978,
                output_t=567,
                cache_read=23296,
                cache_write=0,
                total=26841,
                provider="opencode-go",
                model="deepseek-v4-pro",
                api="openai-completions",
                parent_id="parent-1",
            ),
        ],
    )

    parsed = parse_session_file(session_file)

    assert len(parsed.usage_rows) == 1
    assert parsed.usage_rows_raw == 1
    row = parsed.usage_rows[0]
    assert row.message_id == "bs-1"
    assert row.source_type == "branch_summary"
    assert row.input_tokens == 2978
    assert row.output_tokens == 567
    assert row.cache_read_tokens == 23296
    assert row.cache_write_tokens == 0
    assert row.total_tokens == 26841
    assert row.provider_code == "opencode-go"
    assert row.model_code == "deepseek-v4-pro"
    assert row.stop_reason is None
    assert row.parent_id == "parent-1"


def test_parse_branch_summary_skips_when_no_usage_in_details(tmp_path: Path) -> None:
    """Branch summary without usage (no extension installed) is skipped."""
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            _branch_summary_event(
                "2026-04-13T15:44:00Z",
                summary_id="bs-no-usage",
                include_usage=False,
            ),
        ],
    )

    parsed = parse_session_file(session_file)

    assert len(parsed.usage_rows) == 0


def test_parse_branch_summary_skips_when_details_missing(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            {
                "type": "branch_summary",
                "id": "bs-no-details",
                "parentId": "p-1",
                "timestamp": "2026-04-13T15:44:00Z",
                "fromId": "f-1",
                "summary": "No details here",
                "fromHook": False,
            },
        ],
    )

    parsed = parse_session_file(session_file)

    assert len(parsed.usage_rows) == 0


def test_parse_branch_summary_extracts_cost_from_details(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            _branch_summary_event(
                "2026-04-13T15:44:00Z",
                summary_id="bs-cost",
                input_t=1000,
                output_t=500,
                cost_input=0.001,
                cost_output=0.002,
                cost_cache_read=0.0003,
                cost_cache_write=0.0004,
                cost_total=0.0037,
            ),
        ],
    )

    parsed = parse_session_file(session_file)

    assert len(parsed.usage_rows) == 1
    cost = parsed.usage_rows[0].reported_cost
    assert cost.input_usd == 0.001
    assert cost.output_usd == 0.002
    assert cost.cache_read_usd == 0.0003
    assert cost.cache_write_usd == 0.0004
    assert cost.total_usd == 0.0037


def test_parse_branch_summary_fails_on_missing_input(tmp_path: Path) -> None:
    """A branch_summary with a non-int 'input' in details.usage should fail."""
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            _branch_summary_event(
                "2026-04-13T15:44:00Z",
                summary_id="bs-bad",
                input_t=100,
                output_t=500,
            ),
        ],
    )
    # Mutate: replace the int 'input' with a string to trigger ParseError
    raw_events = _read_jsonl(session_file)
    raw_events[1]["details"]["usage"]["input"] = "oops"
    _write_jsonl(session_file, raw_events)

    with pytest.raises(ParseError, match=r"details\.usage\.input"):
        parse_session_file(session_file)


def test_parse_branch_summary_fails_on_missing_id(tmp_path: Path) -> None:
    session_file = _build_session_file(
        tmp_path,
        cwd="/home/alice/work",
        events=[
            _session_entry(cwd="/home/alice/work"),
            _branch_summary_event(
                "2026-04-13T15:44:00Z",
                summary_id="",
                input_t=100,
                output_t=20,
            ),
        ],
    )

    with pytest.raises(ParseError, match="Missing required 'id'"):
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
    events: list[dict[str, Any]] | None = None,
) -> Path:
    session_file = tmp_path / f"2026-04-13T15-42-45-133Z_{SESSION_ID}.jsonl"
    if events is not None:
        entries: list[dict[str, Any]] = list(events)
    else:
        entries = [_session_entry(cwd=cwd)]
        if assistant_events:
            entries.extend(assistant_events)
        else:
            entries.append(_assistant_event("2026-04-13T15:43:00Z", msg_id="default"))
    _write_jsonl(session_file, entries)
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


def _branch_summary_event(
    timestamp: str,
    *,
    summary_id: str,
    input_t: int | None = 100,
    output_t: int | None = 50,
    cache_read: int = 0,
    cache_write: int = 0,
    total: int | None = None,
    provider: str | None = None,
    model: str | None = None,
    api: str | None = None,
    parent_id: str | None = None,
    include_usage: bool = True,
    cost_input: float | None = None,
    cost_output: float | None = None,
    cost_cache_read: float | None = None,
    cost_cache_write: float | None = None,
    cost_total: float | None = None,
) -> dict[str, Any]:
    """Build a ``type: branch_summary`` event matching the shape produced
    by the ``branch-summary-usage`` Pi extension."""
    details: dict[str, Any] = {
        "readFiles": [],
        "modifiedFiles": [],
    }
    if include_usage and input_t is not None and output_t is not None:
        usage: dict[str, Any] = {
            "input": input_t,
            "output": output_t,
            "cacheRead": cache_read,
            "cacheWrite": cache_write,
        }
        if total is not None:
            usage["totalTokens"] = total
        cost: dict[str, float] = {}
        if cost_input is not None:
            cost["input"] = cost_input
        if cost_output is not None:
            cost["output"] = cost_output
        if cost_cache_read is not None:
            cost["cacheRead"] = cost_cache_read
        if cost_cache_write is not None:
            cost["cacheWrite"] = cost_cache_write
        if cost_total is not None:
            cost["total"] = cost_total
        if cost:
            usage["cost"] = cost
        details["usage"] = usage
    if provider is not None:
        details["provider"] = provider
    if model is not None:
        details["model"] = model
    if api is not None:
        details["api"] = api

    return {
        "type": "branch_summary",
        "id": summary_id,
        "parentId": parent_id,
        "timestamp": timestamp,
        "fromId": "some-from-id",
        "summary": "The user explored a different conversation branch\u2026",
        "details": details,
        "fromHook": True,
    }


def _write_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    with path.open("wb") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read all JSONL events from a file (for test mutation)."""
    events: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            events.append(orjson.loads(raw_line))
    return events
