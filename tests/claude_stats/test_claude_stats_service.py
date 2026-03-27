"""Unit tests for Claude stats service pricing and aggregation logic."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from coding_agent_usage_monitors.claude_token_usage.stats.schemas import TokenUsageEvent
from coding_agent_usage_monitors.claude_token_usage.stats.service import (
    StatsService,
    calculate_event_cost,
    resolve_pricing_model_name,
)


def test_resolve_pricing_model_name_returns_model_code_as_is() -> None:
    """Model price name should be returned unchanged."""
    assert resolve_pricing_model_name("claude-sonnet-4-5-20250514") == "claude-sonnet-4-5-20250514"
    assert resolve_pricing_model_name("claude-opus-4-6") == "claude-opus-4-6"


def test_calculate_event_cost_uses_direct_model_key() -> None:
    """Cost should be calculated using model code as pricing key."""
    event = TokenUsageEvent(
        model_code="claude-test-model",
        event_timestamp=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
        input_tokens=100,
        output_tokens=10,
        cache_creation_input_tokens=10,
        cache_read_input_tokens=20,
    )
    price_spec = {
        "claude-test-model": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
            "cache_creation_input_token_cost": 0.25,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (100 * 1.0) + (10 * 2.0) + (20 * 0.5) + (10 * 0.25)

    assert cost == pytest.approx(expected)


def test_calculate_event_cost_falls_back_to_anthropic_prefix() -> None:
    """Cost should use anthropic/ prefix as fallback when direct key is not found."""
    event = TokenUsageEvent(
        model_code="claude-test-model",
        event_timestamp=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
        input_tokens=100,
        output_tokens=10,
        cache_creation_input_tokens=10,
        cache_read_input_tokens=20,
    )
    price_spec = {
        "anthropic/claude-test-model": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
            "cache_creation_input_token_cost": 0.25,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (100 * 1.0) + (10 * 2.0) + (20 * 0.5) + (10 * 0.25)

    assert cost == pytest.approx(expected)


def test_calculate_event_cost_falls_back_to_input_cost_for_missing_cache_write_pricing() -> None:
    """Missing cache write price should fall back to input token price."""
    event = TokenUsageEvent(
        model_code="claude-test-model",
        event_timestamp=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
        input_tokens=100,
        output_tokens=10,
        cache_creation_input_tokens=10,
        cache_read_input_tokens=20,
    )
    price_spec = {
        "claude-test-model": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (100 * 1.0) + (10 * 2.0) + (20 * 0.5) + (10 * 1.0)

    assert cost == pytest.approx(expected)


def test_calculate_event_cost_returns_zero_for_unknown_model() -> None:
    """Unknown model with empty price spec should produce zero cost."""
    event = TokenUsageEvent(
        model_code="unknown-model",
        event_timestamp=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
        input_tokens=100,
        output_tokens=10,
        cache_creation_input_tokens=5,
        cache_read_input_tokens=0,
    )

    assert calculate_event_cost(event, {}) == 0.0


def test_collect_daily_statistics_groups_by_model() -> None:
    """Aggregated usage should keep models separate."""
    events = [
        TokenUsageEvent(
            model_code="claude-opus-4-6",
            event_timestamp=datetime(2026, 2, 22, 0, 0, tzinfo=UTC),
            input_tokens=100,
            output_tokens=10,
            cache_creation_input_tokens=5,
            cache_read_input_tokens=40,
        ),
        TokenUsageEvent(
            model_code="claude-sonnet-4-5",
            event_timestamp=datetime(2026, 2, 22, 1, 0, tzinfo=UTC),
            input_tokens=200,
            output_tokens=20,
            cache_creation_input_tokens=2,
            cache_read_input_tokens=50,
        ),
    ]
    repository = _FakeStatsRepository(events)
    service = StatsService(repository=repository, price_spec={})  # type: ignore[arg-type]

    report = service.collect_daily_statistics()

    assert report.total_events == 2
    assert report.overall_usage["claude-opus-4-6"].input_tokens == 100
    assert report.overall_usage["claude-sonnet-4-5"].input_tokens == 200


class _FakeStatsRepository:
    """Simple in-memory repository for stats tests."""

    def __init__(self, events: list[TokenUsageEvent]) -> None:
        self._events = events

    def fetch_token_events(self, cwd: str | None = None) -> list[TokenUsageEvent]:
        """Return static token events."""
        return self._events
