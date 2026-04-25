"""Unit tests for Pi agent stats service pricing and aggregation logic."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from coding_agent_usage_monitors.pi_token_usage.stats.schemas import TokenUsageEvent
from coding_agent_usage_monitors.pi_token_usage.stats.service import (
    StatsService,
    _strip_suffixes,
    calculate_event_cost,
    resolve_pricing_model_name,
)


@pytest.mark.parametrize(
    "model_code,expected",
    [
        ("gpt-4:free", "gpt-4"),
        ("gpt-4-free", "gpt-4"),
        ("model:suffix:extra", "model"),
        ("lucas-test-model:free", "lucas-test-model"),
        ("deepseek/deepseek-chat-v3-0324:nitro", "deepseek/deepseek-chat-v3-0324"),
        ("gpt-4", "gpt-4"),
        ("", ""),
        ("model:", "model"),
    ],
)
def test_strip_suffixes_removes_colon_and_free_suffixes(model_code: str, expected: str) -> None:
    """_strip_suffixes should remove all :.* suffixes and -free suffixes."""
    assert _strip_suffixes(model_code) == expected


def test_resolve_pricing_model_name_applies_rule_order() -> None:
    """Model price names should follow Pi (OpenCode-shared) provider/model transformation rules."""
    assert resolve_pricing_model_name(provider_code="opencode", model_code="gpt-5-free") == "gpt-5"
    assert resolve_pricing_model_name(provider_code="openrouter", model_code="qwen/qwen3-coder:free") == (
        "openrouter/qwen/qwen3-coder"
    )
    assert resolve_pricing_model_name(provider_code="opencode", model_code="kimi-k2.5") == "opencode/kimi-k2.5"
    assert resolve_pricing_model_name(provider_code="opencode", model_code="minimax-m2.5") == "opencode/minimax-m2.5"
    assert resolve_pricing_model_name(provider_code="opencode", model_code="glm-5") == "opencode/glm-5"
    assert resolve_pricing_model_name(provider_code="opencode", model_code="big-pickle") == "opencode/big-pickle"


def test_calculate_event_cost_uses_cache_write_tokens_when_pricing_exists() -> None:
    """Cost should include cache write tokens when cache creation pricing is present."""
    event = TokenUsageEvent(
        provider_code="opencode",
        model_code="big-pickle",
        event_timestamp=datetime(2026, 4, 13, 0, 0, tzinfo=UTC),
        input_tokens=100,
        cache_read_tokens=20,
        cache_write_tokens=10,
        output_tokens=10,
    )
    price_spec = {
        "opencode/big-pickle": {
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
        provider_code="opencode",
        model_code="big-pickle",
        event_timestamp=datetime(2026, 4, 13, 0, 0, tzinfo=UTC),
        input_tokens=100,
        cache_read_tokens=20,
        cache_write_tokens=10,
        output_tokens=10,
    )
    price_spec = {
        "opencode/big-pickle": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (100 * 1.0) + (10 * 2.0) + (20 * 0.5) + (10 * 1.0)

    assert cost == pytest.approx(expected)


def test_calculate_event_cost_for_opencode_with_openrouter_only_key_returns_zero() -> None:
    """opencode without matching key in price_spec returns zero cost."""
    event = TokenUsageEvent(
        provider_code="opencode",
        model_code="big-pickle",
        event_timestamp=datetime(2026, 4, 13, 0, 0, tzinfo=UTC),
        input_tokens=100,
        cache_read_tokens=20,
        cache_write_tokens=10,
        output_tokens=10,
    )
    # Price spec only has openrouter key, not opencode - no fallback exists
    price_spec = {
        "openrouter/big-pickle": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    # No matching price spec found, cost is 0
    assert cost == pytest.approx(0.0)


def test_calculate_event_cost_returns_zero_for_lm_studio_provider() -> None:
    """lmstudio provider should always produce zero costs."""
    event = TokenUsageEvent(
        provider_code="lmstudio",
        model_code="llama-3.3",
        event_timestamp=datetime(2026, 4, 13, 0, 0, tzinfo=UTC),
        input_tokens=100,
        cache_read_tokens=20,
        cache_write_tokens=10,
        output_tokens=10,
    )

    assert calculate_event_cost(event, {"llama-3.3": {"input_cost_per_token": 1000.0}}) == 0.0


def test_collect_daily_statistics_groups_by_provider_and_model() -> None:
    """Aggregated usage should keep provider/model pairs separate."""
    events = [
        TokenUsageEvent(
            provider_code="opencode",
            model_code="gpt-5",
            event_timestamp=datetime(2026, 4, 13, 0, 0, tzinfo=UTC),
            input_tokens=100,
            cache_read_tokens=40,
            cache_write_tokens=5,
            output_tokens=10,
        ),
        TokenUsageEvent(
            provider_code="openrouter",
            model_code="gpt-5",
            event_timestamp=datetime(2026, 4, 13, 1, 0, tzinfo=UTC),
            input_tokens=200,
            cache_read_tokens=50,
            cache_write_tokens=2,
            output_tokens=20,
        ),
    ]
    repository = _FakeStatsRepository(events)
    service = StatsService(repository=repository, price_spec={})  # type: ignore[arg-type]

    report = service.collect_daily_statistics()

    assert report.total_events == 2
    assert report.overall_usage[("opencode", "gpt-5")].input_tokens == 100
    assert report.overall_usage[("openrouter", "gpt-5")].input_tokens == 200


class _FakeStatsRepository:
    """Simple in-memory repository for stats tests."""

    def __init__(self, events: list[TokenUsageEvent]) -> None:
        self._events = events

    def fetch_token_events(self) -> list[TokenUsageEvent]:
        """Return static token events."""
        return self._events
