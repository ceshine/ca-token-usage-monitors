"""Aggregation service for Claude token usage statistics."""

from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Any

from coding_agent_usage_monitors.common.model_pricing import get_price_spec

from .schemas import UsageStats, TokenUsageEvent, DailyUsageStatistics
from .repository import StatsRepository


class StatsService:
    """Collect daily usage and cost statistics from persisted token events."""

    def __init__(
        self,
        repository: StatsRepository,
        timezone: ZoneInfo | None = None,
        since: date | None = None,
        until: date | None = None,
        cwd: str | None = None,
        price_spec: dict[str, Any] | None = None,
    ) -> None:
        """Initialise the service with filtering options.

        Args:
            repository (StatsRepository): Data source for raw token usage events.
            timezone (ZoneInfo | None): Timezone used to bucket events into calendar
                days. Defaults to None, which uses the system local timezone.
            since (date | None): Inclusive lower bound for event dates. Events before
                this date are excluded. Defaults to None (no lower bound).
            until (date | None): Exclusive upper bound for event dates. Events on or
                after this date are excluded. Defaults to None (no upper bound).
            cwd (str | None): Absolute working-directory path used to restrict results
                to a specific project session. When provided, only events whose session
                cwd exactly matches this string are included. Defaults to None (all
                sessions).
            price_spec (dict[str, Any] | None): Model pricing data keyed by model name.
                Defaults to None, which loads the bundled price specification via
                ``get_price_spec()``.
        """
        self._repository = repository
        self._timezone = timezone
        self._since = since
        self._until = until
        self._cwd = cwd
        self._price_spec = price_spec if price_spec is not None else get_price_spec()

    def collect_daily_statistics(self) -> DailyUsageStatistics:
        """Aggregate token usage and costs by day and model."""
        events = self._repository.fetch_token_events(cwd=self._cwd)
        usage_by_model_day: dict[tuple[str, date], UsageStats] = defaultdict(UsageStats)
        daily_costs: dict[date, float] = defaultdict(float)
        overall_usage: dict[str, UsageStats] = defaultdict(UsageStats)
        total_events = 0

        for event in events:
            event_date = _resolve_event_date(event.event_timestamp, self._timezone)
            if self._since is not None and event_date < self._since:
                continue
            if self._until is not None and event_date >= self._until:
                continue

            event_cost = calculate_event_cost(event, self._price_spec)

            day_key = (event.model_code, event_date)
            day_stats = usage_by_model_day[day_key]
            _accumulate_usage_stats(day_stats, event, event_cost)

            model_stats = overall_usage[event.model_code]
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
    model_price_spec = _resolve_model_price_spec(event.model_code, price_spec)
    input_cost_per_token = model_price_spec.get("input_cost_per_token", 0.0)
    output_cost_per_token = model_price_spec.get("output_cost_per_token", 0.0)
    cache_read_cost_per_token = model_price_spec.get("cache_read_input_token_cost", 0.0)
    cache_write_cost_per_token = model_price_spec.get(
        "cache_creation_input_token_cost",
        input_cost_per_token,
    )

    total_context_tokens = event.input_tokens + event.cache_read_input_tokens + event.cache_creation_input_tokens
    if total_context_tokens > 200000:
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

    return float(
        (event.input_tokens * input_cost_per_token)
        + (event.output_tokens * output_cost_per_token)
        + (event.cache_read_input_tokens * cache_read_cost_per_token)
        + (event.cache_creation_input_tokens * cache_write_cost_per_token)
    )


def resolve_pricing_model_name(model_code: str) -> str:
    """Resolve canonical model key used for pricing lookup."""
    # Temporary override for claude-opus-4.7, whose pricing info has not been added to the database yet
    # The pricing is exactly the same as claude-opus-4.6
    if model_code == "claude-opus-4-7":
        return "claude-opus-4-6"
    return model_code


def _accumulate_usage_stats(stats: UsageStats, event: TokenUsageEvent, cost: float) -> None:
    """Update aggregate stats with one event."""
    stats.input_tokens += event.input_tokens
    stats.output_tokens += event.output_tokens
    stats.cached_tokens += event.cache_read_input_tokens
    stats.cache_write_tokens += event.cache_creation_input_tokens
    stats.count += 1
    stats.cost += cost


def _resolve_event_date(event_timestamp: datetime, timezone: ZoneInfo | None) -> date:
    """Resolve event date in the selected timezone (or local system timezone)."""
    normalized = event_timestamp if event_timestamp.tzinfo is not None else event_timestamp.replace(tzinfo=UTC)
    return normalized.astimezone(timezone).date()


def _resolve_model_price_spec(model_code: str, price_spec: dict[str, Any]) -> dict[str, Any]:
    """Resolve model pricing data from model code naming rules."""
    resolved_name = resolve_pricing_model_name(model_code)
    resolved = price_spec.get(resolved_name)
    if isinstance(resolved, dict):
        return resolved
    fallback = price_spec.get(f"anthropic/{model_code}")
    if isinstance(fallback, dict):
        return fallback
    return {}
