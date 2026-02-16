"""Unit tests for stats service pricing logic."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from codex_token_usage.stats.schemas import TokenUsageEvent
from codex_token_usage.stats.service import StatsService, calculate_event_cost


def test_calculate_event_cost_uses_above_200k_tier() -> None:
    """Cost calculation should use the above-200k tier when input exceeds threshold."""
    event = TokenUsageEvent(
        model_code="gpt-5",
        event_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
        input_tokens=250000,
        cached_input_tokens=50000,
        output_tokens=1000,
        reasoning_output_tokens=500,
    )
    price_spec = {
        "gpt-5": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
            "input_cost_per_token_above_200k_tokens": 3.0,
            "output_cost_per_token_above_200k_tokens": 4.0,
            "cache_read_input_token_cost_above_200k_tokens": 1.5,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (200000 * 3.0) + (1000 * 4.0) + (50000 * 1.5)

    assert cost == expected


def test_calculate_event_cost_returns_zero_for_unknown_model() -> None:
    """Unknown models should produce zero cost when no pricing is present."""
    event = TokenUsageEvent(
        model_code="unknown-model",
        event_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
        input_tokens=100,
        cached_input_tokens=20,
        output_tokens=10,
        reasoning_output_tokens=3,
    )

    assert calculate_event_cost(event, {}) == 0.0


def test_calculate_event_cost_falls_back_to_gpt_5_2_codex_pricing() -> None:
    """gpt-5.3-codex should temporarily use gpt-5.2-codex pricing when missing."""
    event = TokenUsageEvent(
        model_code="gpt-5.3-codex",
        event_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
        input_tokens=100,
        cached_input_tokens=20,
        output_tokens=10,
        reasoning_output_tokens=5,
    )
    price_spec = {
        "gpt-5.2-codex": {
            "input_cost_per_token": 0.001,
            "output_cost_per_token": 0.002,
            "cache_read_input_token_cost": 0.0001,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (80 * 0.001) + (10 * 0.002) + (20 * 0.0001)

    assert cost == pytest.approx(expected)


def test_collect_daily_statistics_uses_non_cached_input_tokens() -> None:
    """Aggregated input/output token stats should exclude overlapping counts."""
    events = [
        TokenUsageEvent(
            model_code="gpt-5",
            event_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
            input_tokens=100,
            cached_input_tokens=40,
            output_tokens=10,
            reasoning_output_tokens=5,
        )
    ]
    repository = _FakeStatsRepository(events)
    service = StatsService(
        repository=repository,  # type: ignore[arg-type]
        price_spec={
            "gpt-5": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
            }
        },
    )

    report = service.collect_daily_statistics()
    stats = report.overall_usage["gpt-5"]

    assert stats.input_tokens == 60
    assert stats.cached_tokens == 40
    assert stats.output_tokens == 5
    assert stats.thoughts_tokens == 5
    assert stats.cost == pytest.approx(0.084)


def test_collect_daily_statistics_since_filters_older_dates() -> None:
    """Service should skip events that resolve before the since date."""
    events = [
        TokenUsageEvent(
            model_code="gpt-5",
            event_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
            input_tokens=100,
            cached_input_tokens=40,
            output_tokens=10,
            reasoning_output_tokens=5,
        ),
        TokenUsageEvent(
            model_code="o3",
            event_timestamp=datetime(2026, 2, 16, 0, 0, tzinfo=UTC),
            input_tokens=200,
            cached_input_tokens=100,
            output_tokens=50,
            reasoning_output_tokens=10,
        ),
    ]
    repository = _FakeStatsRepository(events)
    service = StatsService(repository=repository, since=datetime(2026, 2, 16, tzinfo=UTC).date(), price_spec={})

    report = service.collect_daily_statistics()

    assert report.total_events == 1
    assert list(report.overall_usage.keys()) == ["o3"]


class _FakeStatsRepository:
    """Simple in-memory repository for stats tests."""

    def __init__(self, events: list[TokenUsageEvent]) -> None:
        self._events = events

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Return static token events."""
        return self._events
