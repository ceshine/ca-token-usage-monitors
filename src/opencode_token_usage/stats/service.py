"""Aggregation service for OpenCode token usage statistics."""

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
        """Aggregate token usage and costs by day, provider, and model."""
        events = self._repository.fetch_token_events()
        usage_by_model_day: dict[tuple[str, str, date], UsageStats] = defaultdict(UsageStats)
        daily_costs: dict[date, float] = defaultdict(float)
        overall_usage: dict[tuple[str, str], UsageStats] = defaultdict(UsageStats)
        total_events = 0

        for event in events:
            event_date = _resolve_event_date(event.event_timestamp, self._timezone)
            if self._since is not None and event_date < self._since:
                continue

            event_cost = calculate_event_cost(event, self._price_spec)

            day_key = (event.provider_code, event.model_code, event_date)
            day_stats = usage_by_model_day[day_key]
            _accumulate_usage_stats(day_stats, event, event_cost)

            overall_key = (event.provider_code, event.model_code)
            model_stats = overall_usage[overall_key]
            _accumulate_usage_stats(model_stats, event, event_cost)

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
    if event.provider_code == "lmstudio":
        return 0.0

    model_price_spec = _resolve_model_price_spec(event.provider_code, event.model_code, price_spec)
    input_cost_per_token = model_price_spec.get("input_cost_per_token", 0.0)
    output_cost_per_token = model_price_spec.get("output_cost_per_token", 0.0)
    cache_read_cost_per_token = model_price_spec.get("cache_read_input_token_cost", 0.0)
    cache_write_cost_per_token = model_price_spec.get(
        "cache_creation_input_token_cost",
        input_cost_per_token,
    )

    if event.input_tokens > 200000:
        input_cost_per_token = model_price_spec.get("input_cost_per_token_above_200k_tokens", input_cost_per_token)
        output_cost_per_token = model_price_spec.get("output_cost_per_token_above_200k_tokens", output_cost_per_token)
        cache_read_cost_per_token = model_price_spec.get(
            "cache_read_input_token_cost_above_200k_tokens",
            cache_read_cost_per_token,
        )
        cache_write_cost_per_token = model_price_spec.get(
            "cache_creation_input_token_cost_above_200k_tokens",
            cache_write_cost_per_token,
        )

    billable_input_tokens = _non_cached_input_tokens(event.input_tokens, event.cache_read_tokens)
    billable_output_tokens = _non_reasoning_output_tokens(event.output_tokens, event.reasoning_tokens)
    return (
        (billable_input_tokens * input_cost_per_token)
        + ((billable_output_tokens + event.reasoning_tokens) * output_cost_per_token)
        + (event.cache_read_tokens * cache_read_cost_per_token)
        + (event.cache_write_tokens * cache_write_cost_per_token)
    )


def resolve_pricing_model_name(provider_code: str, model_code: str) -> str:
    """Resolve canonical model key used for pricing lookup."""
    normalized_model = _strip_free_suffixes(model_code)

    if normalized_model.startswith("gpt"):
        # Use GPT model names as-is
        return normalized_model

    if provider_code == "openrouter":
        return f"openrouter/{normalized_model}"

    if provider_code == "opencode":
        if normalized_model.startswith("kimi"):
            return f"moonshot/{normalized_model}"
        if normalized_model.startswith("minimax"):
            return f"minimax/{normalized_model.replace('minimax-m', 'MiniMax-M')}"
        if normalized_model.startswith("glm"):
            return f"openrouter/z-ai/{normalized_model}"
        if normalized_model == "grok-code":
            return "xai/grok-code-fast-1"
        return f"opencode/{normalized_model}"

    return normalized_model


def _accumulate_usage_stats(stats: UsageStats, event: TokenUsageEvent, cost: float) -> None:
    """Update aggregate stats with one event."""
    stats.input_tokens += _non_cached_input_tokens(event.input_tokens, event.cache_read_tokens)
    stats.output_tokens += _non_reasoning_output_tokens(event.output_tokens, event.reasoning_tokens)
    stats.cached_tokens += event.cache_read_tokens
    stats.cache_write_tokens += event.cache_write_tokens
    stats.thoughts_tokens += event.reasoning_tokens
    stats.count += 1
    stats.cost += cost


def _resolve_event_date(event_timestamp: datetime, timezone: ZoneInfo | None) -> date:
    """Resolve event date in the selected timezone (or local system timezone)."""
    normalized = event_timestamp if event_timestamp.tzinfo is not None else event_timestamp.replace(tzinfo=UTC)
    return normalized.astimezone(timezone).date()


def _non_cached_input_tokens(input_tokens: int, cache_read_tokens: int) -> int:
    """Return input token count with cache-read tokens removed."""
    return max(input_tokens - cache_read_tokens, 0)


def _non_reasoning_output_tokens(output_tokens: int, reasoning_tokens: int) -> int:
    """Return output token count with reasoning tokens removed."""
    return max(output_tokens - reasoning_tokens, 0)


def _resolve_model_price_spec(provider_code: str, model_code: str, price_spec: dict[str, Any]) -> dict[str, Any]:
    """Resolve model pricing data from provider/model specific naming rules."""
    resolved_name = resolve_pricing_model_name(provider_code=provider_code, model_code=model_code)
    resolved = price_spec.get(resolved_name)
    if isinstance(resolved, dict):
        return resolved
    if provider_code == "opencode":
        fallback = price_spec.get(f"openrouter/{_strip_free_suffixes(model_code)}")
        if isinstance(fallback, dict):
            return fallback
    return {}


def _strip_free_suffixes(model_code: str) -> str:
    """Strip known free-tier suffixes from a model code."""
    stripped = model_code
    while stripped.endswith(":free") or stripped.endswith("-free"):
        if stripped.endswith(":free"):
            stripped = stripped[: -len(":free")]
        elif stripped.endswith("-free"):
            stripped = stripped[: -len("-free")]
    return stripped
