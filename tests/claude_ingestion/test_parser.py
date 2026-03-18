"""Tests for Claude Code session log parser."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import orjson
import pytest

from coding_agent_usage_monitors.claude_token_usage.ingestion.errors import (
    ParseError,
    SessionIdentityError,
)
from coding_agent_usage_monitors.claude_token_usage.ingestion.parser import (
    derive_project_name,
    parse_session_file,
    parse_session_identity,
)
from coding_agent_usage_monitors.claude_token_usage.ingestion.schemas import SessionCheckpoint


def test_parse_session_file_extracts_rows(tmp_path: Path) -> None:
    """Parser should extract assistant entries with usage."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=100, output_t=50),
            _assistant_event("2026-03-15T00:00:02Z", model="claude-opus-4-6", input_t=200, output_t=100),
        ],
    )

    session_id, slug, cwd, version, agent_id = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 2
    assert parsed.usage_rows_raw == 2
    assert parsed.usage_rows[0].model_code == "claude-sonnet-4-6"
    assert parsed.usage_rows[0].usage.input_tokens == 100
    assert parsed.usage_rows[1].model_code == "claude-opus-4-6"
    assert parsed.usage_rows[1].usage.output_tokens == 100


def test_parse_session_file_skips_non_assistant(tmp_path: Path) -> None:
    """Parser should skip non-assistant entries."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            {"type": "user", "sessionId": "sess-001", "timestamp": "2026-03-15T00:00:00Z", "message": {"role": "user"}},
            _assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=100, output_t=50),
            {"type": "system", "sessionId": "sess-001", "timestamp": "2026-03-15T00:00:02Z"},
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1
    assert parsed.usage_rows_raw == 1


def test_parse_session_file_skips_assistant_without_usage(tmp_path: Path) -> None:
    """Parser should skip assistant entries that lack message.usage."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            {
                "type": "assistant",
                "sessionId": "sess-001",
                "timestamp": "2026-03-15T00:00:01Z",
                "message": {"id": "msg-1", "model": "claude-sonnet-4-6", "role": "assistant"},
                "requestId": "req-1",
            },
            _assistant_event("2026-03-15T00:00:02Z", model="claude-sonnet-4-6", input_t=100, output_t=50),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1


def test_parse_session_file_skips_synthetic_model(tmp_path: Path) -> None:
    """Parser should skip entries with <synthetic> model."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event("2026-03-15T00:00:01Z", model="<synthetic>", input_t=0, output_t=0),
            _assistant_event("2026-03-15T00:00:02Z", model="claude-sonnet-4-6", input_t=100, output_t=50),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1
    assert parsed.usage_rows_skipped_synthetic == 1


def test_parse_session_file_handles_speed_fast(tmp_path: Path) -> None:
    """Parser should append -fast suffix when speed is fast."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=100, output_t=50, speed="fast"),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert parsed.usage_rows[0].model_code == "claude-sonnet-4-6-fast"


def test_parse_session_file_dedup_by_message_id_request_id(tmp_path: Path) -> None:
    """Parser should deduplicate by message.id + requestId."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event(
                "2026-03-15T00:00:01Z",
                model="claude-sonnet-4-6",
                input_t=100,
                output_t=50,
                message_id="msg-dup",
                request_id="req-dup",
            ),
            _assistant_event(
                "2026-03-15T00:00:02Z",
                model="claude-sonnet-4-6",
                input_t=100,
                output_t=50,
                message_id="msg-dup",
                request_id="req-dup",
            ),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1
    assert parsed.duplicate_rows_skipped == 1


def test_parse_session_file_dedup_conflict_intermediate_skipped(tmp_path: Path) -> None:
    """Duplicate with conflicting tokens and no stop_reason should be treated as intermediate and skipped."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event(
                "2026-03-15T00:00:01Z",
                model="claude-sonnet-4-6",
                input_t=100,
                output_t=8,
                message_id="msg-dup",
                request_id="req-dup",
            ),
            _assistant_event(
                "2026-03-15T00:00:02Z",
                model="claude-sonnet-4-6",
                input_t=100,
                output_t=168,
                message_id="msg-dup",
                request_id="req-dup",
            ),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1
    assert parsed.duplicate_rows_skipped == 1
    # First entry wins when the conflicting duplicate has no stop_reason.
    assert parsed.usage_rows[0].usage.output_tokens == 8


def test_parse_session_file_dedup_final_entry_supersedes(tmp_path: Path) -> None:
    """Final entry (stop_reason set) should replace the earlier streaming partial."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event(
                "2026-03-15T00:00:01Z",
                model="claude-sonnet-4-6",
                input_t=3,
                output_t=8,
                message_id="msg-dup",
                request_id="req-dup",
            ),
            _assistant_event(
                "2026-03-15T00:00:02Z",
                model="claude-sonnet-4-6",
                input_t=3,
                output_t=168,
                message_id="msg-dup",
                request_id="req-dup",
                stop_reason="tool_use",
            ),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1
    # Final entry supersedes the streaming partial.
    assert parsed.usage_rows[0].usage.output_tokens == 168


def test_parse_session_file_checkpoint_filters_old_rows(tmp_path: Path) -> None:
    """Parser should skip rows older than checkpoint."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event(
                "2026-03-15T00:00:01Z",
                model="claude-sonnet-4-6",
                input_t=100,
                output_t=50,
                message_id="msg-1",
                request_id="req-1",
            ),
            _assistant_event(
                "2026-03-15T00:00:02Z",
                model="claude-sonnet-4-6",
                input_t=200,
                output_t=100,
                message_id="msg-2",
                request_id="req-2",
            ),
            _assistant_event(
                "2026-03-15T00:00:03Z",
                model="claude-opus-4-6",
                input_t=300,
                output_t=150,
                message_id="msg-3",
                request_id="req-3",
            ),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    checkpoint = SessionCheckpoint(
        last_ts=datetime(2026, 3, 15, 0, 0, 2, tzinfo=UTC),
        last_message_id="msg-2",
        last_request_id="req-2",
    )

    parsed = parse_session_file(session_file, session_id, checkpoint)

    assert parsed.usage_rows_raw == 3
    assert parsed.usage_rows_skipped_before_checkpoint == 1
    assert len(parsed.usage_rows) == 2
    assert parsed.usage_rows[0].message_id == "msg-2"
    assert parsed.usage_rows[1].message_id == "msg-3"


def test_parse_session_identity_returns_agent_id_for_subagent_file(tmp_path: Path) -> None:
    """Should return agent_id from subagent entries, None for main session."""
    main_file = tmp_path / "session.jsonl"
    _write_jsonl(
        main_file, [_assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=10, output_t=5)]
    )
    _, _, _, _, agent_id = parse_session_identity(main_file)
    assert agent_id is None

    subagent_file = tmp_path / "subagent.jsonl"
    event = _assistant_event("2026-03-15T00:00:01Z", model="claude-haiku-4-5-20251001", input_t=5, output_t=3)
    event["agentId"] = "agent-abc123"
    _write_jsonl(subagent_file, [event])
    _, _, _, _, agent_id = parse_session_identity(subagent_file)
    assert agent_id == "agent-abc123"


def test_parse_session_identity_fails_on_malformed_json(tmp_path: Path) -> None:
    """Malformed JSON lines should raise ParseError."""
    session_file = tmp_path / "session.jsonl"
    session_file.write_text('{"type": "assistant"\n', encoding="utf-8")

    with pytest.raises(ParseError):
        parse_session_identity(session_file)


def test_parse_session_identity_fails_when_no_session_id(tmp_path: Path) -> None:
    """Should raise SessionIdentityError when no entry has sessionId."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [{"type": "user", "timestamp": "2026-03-15T00:00:00Z"}],
    )

    with pytest.raises(SessionIdentityError):
        parse_session_identity(session_file)


def test_derive_project_name(tmp_path: Path) -> None:
    """Should derive project name from directory relative to root."""
    root = tmp_path / "projects"
    project_dir = root / "my-project-hash"
    project_dir.mkdir(parents=True)
    session_file = project_dir / "session-abc.jsonl"
    session_file.touch()

    assert derive_project_name(session_file, [root]) == "my-project-hash"


def test_parse_session_file_extracts_sidechain_info(tmp_path: Path) -> None:
    """Parser should extract isSidechain and agentId from subagent entries."""
    session_file = tmp_path / "session.jsonl"
    _write_jsonl(
        session_file,
        [
            _assistant_event(
                "2026-03-15T00:00:01Z",
                model="claude-haiku-4-5-20251001",
                input_t=50,
                output_t=25,
                is_sidechain=True,
                agent_id="agent-xyz",
            ),
        ],
    )

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert len(parsed.usage_rows) == 1
    assert parsed.usage_rows[0].is_sidechain is True
    assert parsed.usage_rows[0].agent_id == "agent-xyz"


def test_parse_session_file_handles_cache_tokens(tmp_path: Path) -> None:
    """Parser should extract cache_creation and cache_read tokens."""
    session_file = tmp_path / "session.jsonl"
    event = _assistant_event("2026-03-15T00:00:01Z", model="claude-sonnet-4-6", input_t=100, output_t=50)
    event["message"]["usage"]["cache_creation_input_tokens"] = 30
    event["message"]["usage"]["cache_read_input_tokens"] = 20
    _write_jsonl(session_file, [event])

    session_id, _, _, _, _ = parse_session_identity(session_file)
    parsed = parse_session_file(session_file, session_id, checkpoint=None)

    assert parsed.usage_rows[0].usage.cache_creation_input_tokens == 30
    assert parsed.usage_rows[0].usage.cache_read_input_tokens == 20


# --- helpers ---


def _assistant_event(
    timestamp: str,
    *,
    model: str,
    input_t: int,
    output_t: int,
    message_id: str | None = None,
    request_id: str | None = None,
    speed: str | None = None,
    is_sidechain: bool = False,
    agent_id: str | None = None,
    session_id: str = "sess-001",
    stop_reason: str | None = None,
) -> dict[str, object]:
    """Build an assistant entry with usage fields."""
    msg_id = message_id or f"msg-{timestamp}"
    req_id = request_id or f"req-{timestamp}"
    event: dict[str, object] = {
        "type": "assistant",
        "sessionId": session_id,
        "timestamp": timestamp,
        "requestId": req_id,
        "message": {
            "id": msg_id,
            "model": model,
            "role": "assistant",
            "stop_reason": stop_reason,
            "usage": {
                "input_tokens": input_t,
                "output_tokens": output_t,
            },
        },
    }
    if speed is not None:
        event["speed"] = speed
    if is_sidechain:
        event["isSidechain"] = True
    if agent_id is not None:
        event["agentId"] = agent_id
    return event


def _write_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    """Write JSONL events to disk."""
    with path.open("wb") as handle:
        for event in events:
            handle.write(orjson.dumps(event))
            handle.write(b"\n")
