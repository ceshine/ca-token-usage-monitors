"""Aggregation service for daily token usage statistics."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from model_pricing import get_price_spec

from .repository import StatsRepository
from .schemas import DailyUsageStatistics, TokenUsageEvent, UsageStats


class StatsService:
    """Collect daily usage and cost statistics from persisted token events."""

    def __init__(
        self,
        repository: StatsRepository,
        timezone: ZoneInfo | None = None,
        since: date | None = None,
        price_spec: dict[str, Any] | None = None,
    ) -> None:
        self._repository = repository
        self._timezone = timezone
        self._since = since
        self._price_spec = price_spec if price_spec is not None else get_price_spec()

    def collect_daily_statistics(self) -> DailyUsageStatistics:
        """Aggregate token usage and costs by day and model."""
        events = self._repository.fetch_token_events()
        usage_by_model_day: dict[tuple[str, date], UsageStats] = defaultdict(UsageStats)
        daily_costs: dict[date, float] = defaultdict(float)
        overall_usage: dict[str, UsageStats] = defaultdict(UsageStats)
        total_events = 0

        for event in events:
            event_date = _resolve_event_date(event.event_timestamp, self._timezone)
            if self._since is not None and event_date < self._since:
                continue

            event_cost = calculate_event_cost(event, self._price_spec)

            daily_stats = usage_by_model_day[(event.model_code, event_date)]
            _accumulate_usage_stats(daily_stats, event, event_cost)

            overall_stats = overall_usage[event.model_code]
            _accumulate_usage_stats(overall_stats, event, event_cost)

            daily_costs[event_date] += event_cost
            total_events += 1

        return DailyUsageStatistics(
            usage_by_model_day=dict(usage_by_model_day),
            daily_costs=dict(daily_costs),
            overall_usage=dict(overall_usage),
            total_events=total_events,
        )


def calculate_event_cost(event: TokenUsageEvent, price_spec: dict[str, Any]) -> float:
    """Calculate USD cost for one event using model pricing data."""
    model_price_spec = _resolve_model_price_spec(event.model_code, price_spec)
    non_cached_input_tokens = _non_cached_input_tokens(event.input_tokens, event.cached_input_tokens)
    non_reasoning_output_tokens = _non_reasoning_output_tokens(event.output_tokens, event.reasoning_output_tokens)

    input_cost_per_token = model_price_spec.get("input_cost_per_token", 0)
    output_cost_per_token = model_price_spec.get("output_cost_per_token", 0)
    cached_cost_per_token = model_price_spec.get("cache_read_input_token_cost", 0)

    if event.input_tokens > 200000:
        input_cost_per_token = model_price_spec.get("input_cost_per_token_above_200k_tokens", input_cost_per_token)
        output_cost_per_token = model_price_spec.get("output_cost_per_token_above_200k_tokens", output_cost_per_token)
        cached_cost_per_token = model_price_spec.get(
            "cache_read_input_token_cost_above_200k_tokens",
            cached_cost_per_token,
        )

    return (
        (non_cached_input_tokens * input_cost_per_token)
        + ((non_reasoning_output_tokens + event.reasoning_output_tokens) * output_cost_per_token)
        + (event.cached_input_tokens * cached_cost_per_token)
    )


def _accumulate_usage_stats(stats: UsageStats, event: TokenUsageEvent, cost: float) -> None:
    """Update aggregate stats with one event."""
    stats.input_tokens += _non_cached_input_tokens(event.input_tokens, event.cached_input_tokens)
    stats.output_tokens += _non_reasoning_output_tokens(event.output_tokens, event.reasoning_output_tokens)
    stats.cached_tokens += event.cached_input_tokens
    stats.thoughts_tokens += event.reasoning_output_tokens
    stats.count += 1
    stats.cost += cost


def _resolve_event_date(event_timestamp: datetime, timezone: ZoneInfo | None) -> date:
    """Resolve event date in the selected timezone (or local system timezone)."""
    normalized = event_timestamp if event_timestamp.tzinfo is not None else event_timestamp.replace(tzinfo=UTC)
    return normalized.astimezone(timezone).date()


def _non_cached_input_tokens(input_tokens: int, cached_input_tokens: int) -> int:
    """Return input token count with cached tokens removed."""
    return max(input_tokens - cached_input_tokens, 0)


def _non_reasoning_output_tokens(output_tokens: int, reasoning_output_tokens: int) -> int:
    """Return output token count with reasoning tokens removed."""
    return max(output_tokens - reasoning_output_tokens, 0)


def _resolve_model_price_spec(model_code: str, price_spec: dict[str, Any]) -> dict[str, Any]:
    """Resolve model pricing with temporary compatibility fallbacks."""
    resolved = price_spec.get(model_code)
    if resolved is not None:
        return resolved
    if model_code == "gpt-5.3-codex":
        fallback = price_spec.get("gpt-5.2-codex")
        if fallback is not None:
            return fallback
    return {}
