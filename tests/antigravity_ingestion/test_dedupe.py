"""Tests for AntiGravity global event deduplication module."""

from __future__ import annotations

from datetime import UTC, datetime

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.dedupe import (
    get_identity_keys,
    deduplicate_events,
)
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.schemas import RawUsageEvent

TS_EARLY = datetime(2026, 6, 23, 10, 0, 0, tzinfo=UTC)
TS_LATE = datetime(2026, 6, 23, 12, 0, 0, tzinfo=UTC)


def test_get_identity_keys_preference_order() -> None:
    """Verify identity key extraction in preference order: response, provider, message."""
    event = RawUsageEvent(
        session_id="session1",
        discovery_source="cli",
        event_timestamp=TS_EARLY,
        timestamp_rank=3,
        model_code="gemini-2.5-flash",
        provider_id=3,
        input_tokens=10,
        output_tokens=20,
        reasoning_tokens=0,
        total_output_tokens=20,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=30,
        response_id="resp_123",
        provider_assigned_message_id="prov_456",
        message_id="msg_789",
        fallback_key="fallback:db:table:0:0",
    )
    keys = get_identity_keys(event)
    assert keys == ["response:resp_123", "provider:prov_456", "message:msg_789"]


def test_deduplicate_across_cli_and_ide_sources() -> None:
    """Deduplicate events across separate cli and ide databases sharing response_id."""
    event_cli = RawUsageEvent(
        session_id="session_cli",
        discovery_source="cli",
        event_timestamp=TS_LATE,
        timestamp_rank=3,
        model_code="gemini-internal-model",
        provider_id=None,
        input_tokens=50,
        output_tokens=20,
        reasoning_tokens=0,
        total_output_tokens=20,
        cache_creation_tokens=5,
        cache_read_tokens=0,
        total_tokens=75,
        response_id="resp_dup",
        provider_assigned_message_id=None,
        message_id=None,
        fallback_key="fallback:cli:gen:0:0",
    )
    event_default = RawUsageEvent(
        session_id="session_default",
        discovery_source="default",
        event_timestamp=TS_EARLY,
        timestamp_rank=3,
        model_code="gemini-3.6-flash",
        provider_id=24,
        input_tokens=30,
        output_tokens=40,
        reasoning_tokens=10,
        total_output_tokens=40,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=70,
        response_id="resp_dup",
        provider_assigned_message_id="prov_dup",
        message_id=None,
        fallback_key="fallback:default:gen:0:0",
    )

    merged = deduplicate_events([event_cli, event_default])
    assert len(merged) == 1
    row = merged[0]

    # Discovery source priority: default > cli
    assert row.discovery_source == "default"

    # Token counters field-by-field max
    assert row.input_tokens == 50
    assert row.output_tokens == 40
    assert row.reasoning_tokens == 10
    assert row.total_output_tokens == 40
    assert row.cache_creation_tokens == 5
    assert row.total_tokens == 50 + 5 + 0 + 40  # 95

    # Non-default model preferred over gemini-internal-model
    assert row.model_code == "gemini-3.6-flash"
    assert row.provider_id == 24

    # Timestamp tie-breaker: earlier timestamp selected (TS_EARLY)
    assert row.event_timestamp == TS_EARLY
    assert row.event_key == "response:resp_dup"


def test_transitive_identity_merging() -> None:
    """Verify transitive duplicate merging (Event 1 <-> Event 2 <-> Event 3)."""
    # Event 1: key A (message:m1)
    e1 = RawUsageEvent(
        session_id="s1",
        discovery_source="env",
        event_timestamp=TS_EARLY,
        timestamp_rank=1,
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
        message_id="m1",
        fallback_key="fallback:1",
    )
    # Event 2: key A (message:m1) AND key B (response:r2)
    e2 = RawUsageEvent(
        session_id="s2",
        discovery_source="env",
        event_timestamp=TS_EARLY,
        timestamp_rank=1,
        model_code="gemini-2.5-flash",
        provider_id=None,
        input_tokens=20,
        output_tokens=10,
        reasoning_tokens=0,
        total_output_tokens=10,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=30,
        response_id="r2",
        provider_assigned_message_id=None,
        message_id="m1",
        fallback_key="fallback:2",
    )
    # Event 3: key B (response:r2)
    e3 = RawUsageEvent(
        session_id="s3",
        discovery_source="env",
        event_timestamp=TS_EARLY,
        timestamp_rank=1,
        model_code="gemini-2.5-flash",
        provider_id=None,
        input_tokens=15,
        output_tokens=25,
        reasoning_tokens=0,
        total_output_tokens=25,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=40,
        response_id="r2",
        provider_assigned_message_id=None,
        message_id=None,
        fallback_key="fallback:3",
    )

    merged = deduplicate_events([e1, e2, e3])
    assert len(merged) == 1
    row = merged[0]
    assert row.input_tokens == 20
    assert row.output_tokens == 25
    assert row.event_key == "response:r2"


def test_no_identity_events_remain_distinct() -> None:
    """Verify events without identity keys are retained as separate distinct events."""
    e1 = RawUsageEvent(
        session_id="s1",
        discovery_source="cli",
        event_timestamp=TS_EARLY,
        timestamp_rank=0,
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
        fallback_key="fallback:db1:gen:0:0",
    )
    e2 = RawUsageEvent(
        session_id="s1",
        discovery_source="cli",
        event_timestamp=TS_EARLY,
        timestamp_rank=0,
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
        fallback_key="fallback:db1:gen:0:1",
    )

    merged = deduplicate_events([e1, e2])
    assert len(merged) == 2
    keys = {r.event_key for r in merged}
    assert keys == {"fallback:db1:gen:0:0", "fallback:db1:gen:0:1"}


def test_timestamp_rank_precedence() -> None:
    """Verify explicit ts (rank 3) beats trajectory ts (rank 1) and mtime (rank 0)."""
    e_rank0 = RawUsageEvent(
        session_id="s1",
        discovery_source="cli",
        event_timestamp=TS_EARLY,
        timestamp_rank=0,
        model_code="gemini-2.5-flash",
        provider_id=None,
        input_tokens=10,
        output_tokens=10,
        reasoning_tokens=0,
        total_output_tokens=10,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=20,
        response_id="r1",
        provider_assigned_message_id=None,
        message_id=None,
        fallback_key="f0",
    )
    e_rank3 = RawUsageEvent(
        session_id="s1",
        discovery_source="cli",
        event_timestamp=TS_LATE,
        timestamp_rank=3,
        model_code="gemini-2.5-flash",
        provider_id=None,
        input_tokens=10,
        output_tokens=10,
        reasoning_tokens=0,
        total_output_tokens=10,
        cache_creation_tokens=0,
        cache_read_tokens=0,
        total_tokens=20,
        response_id="r1",
        provider_assigned_message_id=None,
        message_id=None,
        fallback_key="f3",
    )

    merged = deduplicate_events([e_rank0, e_rank3])
    assert len(merged) == 1
    # Rank 3 wins over rank 0 even though TS_LATE is later than TS_EARLY
    assert merged[0].event_timestamp == TS_LATE
