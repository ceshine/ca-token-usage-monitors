"""Unit tests for AntiGravity stats aggregation and pricing."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from coding_agent_usage_monitors.antigravity_token_usage.stats.schemas import TokenUsageEvent
from coding_agent_usage_monitors.antigravity_token_usage.stats.service import (
    StatsService,
    calculate_event_cost,
    resolve_pricing_model_name,
)


def test_resolve_pricing_model_name_normalizes_antigravity_model_codes() -> None:
    """Pricing lookup should normalize effort and version-first provider model names."""
    assert resolve_pricing_model_name("gemini-3.8-flash-high") == "google/gemini-3.8-flash"
    assert resolve_pricing_model_name("gemini-2.5-flash-thinking") == "google/gemini-2.5-flash"
    assert resolve_pricing_model_name("claude-4.5-sonnet") == "anthropic/claude-sonnet-4-5"
    assert resolve_pricing_model_name("claude-opus-4-6") == "anthropic/claude-opus-4-6"
    assert resolve_pricing_model_name("gpt-oss-120b-medium") is None


def test_calculate_event_cost_uses_normalized_gemini_price_and_cache_categories() -> None:
    """Gemini effort models should use their base API model pricing entry."""
    event = TokenUsageEvent(
        model_code="gemini-3.8-flash-high",
        event_timestamp=datetime(2026, 9, 21, tzinfo=UTC),
        input_tokens=100,
        output_tokens=10,
        reasoning_tokens=5,
        cache_creation_tokens=4,
        cache_read_tokens=20,
    )
    price_spec = {
        "google/gemini-3.8-flash": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
            "cache_creation_input_token_cost": 0.25,
        }
    }

    assert calculate_event_cost(event, price_spec) == pytest.approx(141.0)


def test_calculate_event_cost_returns_zero_for_unsupported_model_family() -> None:
    """Non-Gemini and non-Claude models should remain unpriced."""
    event = TokenUsageEvent(
        model_code="gpt-oss-120b-medium",
        event_timestamp=datetime(2026, 9, 21, tzinfo=UTC),
        input_tokens=100,
        output_tokens=10,
        reasoning_tokens=5,
        cache_creation_tokens=4,
        cache_read_tokens=20,
    )

    assert calculate_event_cost(event, {"openai/gpt-oss-120b-medium": {"input_cost_per_token": 1.0}}) == 0.0


def test_collect_daily_statistics_filters_dates_and_preserves_raw_model_labels() -> None:
    """Reports should filter local dates while retaining canonical Antigravity labels."""
    events = [
        TokenUsageEvent(
            model_code="gemini-3.8-flash-high",
            event_timestamp=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
            input_tokens=100,
            output_tokens=10,
            reasoning_tokens=5,
            cache_creation_tokens=4,
            cache_read_tokens=20,
        ),
        TokenUsageEvent(
            model_code="claude-4.5-sonnet",
            event_timestamp=datetime(2026, 9, 21, 1, 0, tzinfo=UTC),
            input_tokens=200,
            output_tokens=20,
            reasoning_tokens=0,
            cache_creation_tokens=0,
            cache_read_tokens=50,
        ),
    ]
    service = StatsService(
        repository=_FakeStatsRepository(events),  # type: ignore[arg-type]
        since=date(2026, 9, 21),
        price_spec={},
    )

    report = service.collect_daily_statistics()

    assert report.total_events == 1
    assert set(report.overall_usage) == {"claude-4.5-sonnet"}
    assert report.overall_usage["claude-4.5-sonnet"].input_tokens == 200


class _FakeStatsRepository:
    """Static in-memory stats source for service tests."""

    def __init__(self, events: list[TokenUsageEvent]) -> None:
        """Store static events.

        Args:
            events: Events returned by fetch_token_events.
        """
        self._events = events

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Return configured static events.

        Returns:
            Static token usage events.
        """
        return self._events
