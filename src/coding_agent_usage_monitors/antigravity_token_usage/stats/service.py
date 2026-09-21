"""Aggregation service for AntiGravity token usage statistics."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Any

from coding_agent_usage_monitors.common.model_pricing import get_price_spec

from .schemas import UsageStats, TokenUsageEvent, DailyUsageStatistics
from .repository import StatsRepository

_GEMINI_EFFORT_MODEL_PATTERN = re.compile(
    r"^(gemini-\d+\.\d+-(?:flash|pro))(?:-(?:thinking|high|medium|low|extra-low))$"
)
_CLAUDE_VERSION_FIRST_PATTERN = re.compile(r"^claude-(\d+(?:[.-]\d+)?)-(opus|sonnet|haiku)$")


class StatsService:
    """Collect daily usage and cost statistics from persisted AntiGravity events."""

    def __init__(
        self,
        repository: StatsRepository,
        timezone: ZoneInfo | None = None,
        since: date | None = None,
        until: date | None = None,
        price_spec: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the statistics service.

        Args:
            repository: Source for canonical AntiGravity token events.
            timezone: Timezone used to group events into calendar dates.
            since: Inclusive lower bound for event dates.
            until: Exclusive upper bound for event dates.
            price_spec: Flat provider/model pricing data, or None to load the shared catalog.
        """
        self._repository = repository
        self._timezone = timezone
        self._since = since
        self._until = until
        self._price_spec = price_spec if price_spec is not None else get_price_spec()

    def collect_daily_statistics(self) -> DailyUsageStatistics:
        """Aggregate canonical events by day and model.

        Returns:
            Aggregated usage and cost statistics.
        """
        usage_by_model_day: dict[tuple[str, date], UsageStats] = defaultdict(UsageStats)
        daily_costs: dict[date, float] = defaultdict(float)
        overall_usage: dict[str, UsageStats] = defaultdict(UsageStats)
        total_events = 0

        for event in self._repository.fetch_token_events():
            event_date = _resolve_event_date(event.event_timestamp, self._timezone)
            if self._since is not None and event_date < self._since:
                continue
            if self._until is not None and event_date >= self._until:
                continue

            event_cost = calculate_event_cost(event, self._price_spec)
            _accumulate_usage_stats(usage_by_model_day[(event.model_code, event_date)], event, event_cost)
            _accumulate_usage_stats(overall_usage[event.model_code], event, event_cost)
            daily_costs[event_date] += event_cost
            total_events += 1

        return DailyUsageStatistics(
            usage_by_model_day=dict(usage_by_model_day),
            daily_costs=dict(daily_costs),
            overall_usage=dict(overall_usage),
            total_events=total_events,
        )


def calculate_event_cost(event: TokenUsageEvent, price_spec: dict[str, Any]) -> float:
    """Calculate an event's estimated USD cost from its normalized model code.

    Args:
        event: Canonical AntiGravity token usage event.
        price_spec: Flat provider/model pricing data.

    Returns:
        Estimated USD cost, or zero when the model has no supported price entry.
    """
    model_price_spec = _resolve_model_price_spec(event.model_code, price_spec)
    input_cost = float(model_price_spec.get("input_cost_per_token", 0.0))
    output_cost = float(model_price_spec.get("output_cost_per_token", 0.0))
    cache_read_cost = float(model_price_spec.get("cache_read_input_token_cost", 0.0))
    cache_creation_cost = float(model_price_spec.get("cache_creation_input_token_cost", input_cost))
    return (
        (event.input_tokens * input_cost)
        + ((event.output_tokens + event.reasoning_tokens) * output_cost)
        + (event.cache_read_tokens * cache_read_cost)
        + (event.cache_creation_tokens * cache_creation_cost)
    )


def resolve_pricing_model_name(model_code: str) -> str | None:
    """Resolve an AntiGravity model code to a shared provider/model price key.

    Args:
        model_code: Canonical model code persisted by AntiGravity ingestion.

    Returns:
        Shared provider/model price key, or None for unsupported model families.
    """
    if model_code.startswith("gemini-"):
        effort_match = _GEMINI_EFFORT_MODEL_PATTERN.fullmatch(model_code)
        normalized_model = effort_match.group(1) if effort_match else model_code
        return f"google/{normalized_model}"

    if model_code.startswith("claude-"):
        version_first_match = _CLAUDE_VERSION_FIRST_PATTERN.fullmatch(model_code)
        if version_first_match:
            version = version_first_match.group(1).replace(".", "-")
            family = version_first_match.group(2)
            return f"anthropic/claude-{family}-{version}"
        return f"anthropic/{model_code}"

    return None


def _accumulate_usage_stats(stats: UsageStats, event: TokenUsageEvent, cost: float) -> None:
    """Update aggregate stats with one event.

    Args:
        stats: Mutable aggregate to update.
        event: Canonical event to add.
        cost: Estimated USD cost for the event.
    """
    stats.input_tokens += event.input_tokens
    stats.output_tokens += event.output_tokens
    stats.reasoning_tokens += event.reasoning_tokens
    stats.cache_creation_tokens += event.cache_creation_tokens
    stats.cache_read_tokens += event.cache_read_tokens
    stats.count += 1
    stats.cost += cost


def _resolve_event_date(event_timestamp: datetime, timezone: ZoneInfo | None) -> date:
    """Resolve an event timestamp to its local reporting date.

    Args:
        event_timestamp: Timestamp persisted for a usage event.
        timezone: Optional requested timezone; None uses the system local timezone.

    Returns:
        Calendar date used for aggregation.
    """
    normalized = event_timestamp if event_timestamp.tzinfo is not None else event_timestamp.replace(tzinfo=UTC)
    return normalized.astimezone(timezone).date()


def _resolve_model_price_spec(model_code: str, price_spec: dict[str, Any]) -> dict[str, Any]:
    """Find pricing for a supported AntiGravity model code.

    Args:
        model_code: Canonical persisted AntiGravity model code.
        price_spec: Flat provider/model pricing data.

    Returns:
        Matching pricing data, or an empty mapping when unavailable.
    """
    resolved_name = resolve_pricing_model_name(model_code)
    if resolved_name is None:
        return {}
    resolved = price_spec.get(resolved_name)
    return resolved if isinstance(resolved, dict) else {}
