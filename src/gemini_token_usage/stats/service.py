"""Aggregation service for Gemini token usage statistics."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime
import logging
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import orjsonl
from model_pricing import get_price_spec

from .schemas import DailyUsageStatistics, TokenUsageEvent, UsageStats

LOGGER = logging.getLogger(__name__)


class StatsService:
    """Collect daily usage and cost statistics from Gemini telemetry JSONL."""

    def __init__(self, price_spec: dict[str, Any] | None = None) -> None:
        self._price_spec = price_spec if price_spec is not None else get_price_spec()

    def collect_daily_statistics(
        self,
        log_file_path: Path,
        timezone: ZoneInfo | None = None,
    ) -> DailyUsageStatistics:
        """Aggregate token usage and costs by day and model from JSONL."""
        events: list[TokenUsageEvent] = []
        encountered_errors = False

        try:
            for entry in orjsonl.stream(log_file_path):
                if not isinstance(entry, dict):
                    encountered_errors = True
                    LOGGER.warning("Skipping malformed record that is not a JSON object.")
                    continue

                event = _parse_usage_event(entry)
                if event is None:
                    continue

                events.append(event)
        except Exception as exc:
            encountered_errors = True
            LOGGER.error("Error while processing %s: %s", log_file_path, exc)

        report = self.collect_daily_statistics_from_events(events=events, timezone=timezone)
        return DailyUsageStatistics(
            usage_by_model_day=report.usage_by_model_day,
            daily_costs=report.daily_costs,
            overall_usage=report.overall_usage,
            total_events=report.total_events,
            encountered_errors=encountered_errors,
        )

    def collect_daily_statistics_from_events(
        self,
        events: list[TokenUsageEvent],
        timezone: ZoneInfo | None = None,
    ) -> DailyUsageStatistics:
        """Aggregate token usage and costs by day and model from parsed events."""
        usage_by_model_day: dict[tuple[str, date], UsageStats] = defaultdict(UsageStats)
        daily_costs: dict[date, float] = defaultdict(float)
        overall_usage: dict[str, UsageStats] = defaultdict(UsageStats)
        total_events = 0

        for event in events:
            event_date = _resolve_event_date(event.event_timestamp, timezone)
            event_cost = calculate_event_cost(event, self._price_spec)

            day_stats = usage_by_model_day[(event.model_code, event_date)]
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
            encountered_errors=False,
        )


def calculate_event_cost(event: TokenUsageEvent, price_spec: dict[str, Any]) -> float:
    """Calculate USD cost for one usage event."""
    model_price_spec = price_spec.get(event.model_code, {})
    non_cached_input_tokens = max(event.input_tokens - event.cached_input_tokens, 0)
    output_billable_tokens = max(event.output_tokens, 0) + max(event.thoughts_tokens, 0)

    input_cost_per_token = model_price_spec.get("input_cost_per_token", 0.0)
    output_cost_per_token = model_price_spec.get("output_cost_per_token", 0.0)
    cached_cost_per_token = model_price_spec.get("cache_read_input_token_cost", 0.0)

    if event.input_tokens > 200000:
        input_cost_per_token = model_price_spec.get("input_cost_per_token_above_200k_tokens", input_cost_per_token)
        output_cost_per_token = model_price_spec.get("output_cost_per_token_above_200k_tokens", output_cost_per_token)
        cached_cost_per_token = model_price_spec.get(
            "cache_read_input_token_cost_above_200k_tokens",
            cached_cost_per_token,
        )

    return (
        (non_cached_input_tokens * input_cost_per_token)
        + (output_billable_tokens * output_cost_per_token)
        + (event.cached_input_tokens * cached_cost_per_token)
    )


def _parse_usage_event(entry: dict[str, Any]) -> TokenUsageEvent | None:
    """Parse one `gemini_cli.api_response` telemetry entry into a typed event."""
    attributes = entry.get("attributes")
    if not isinstance(attributes, dict):
        return None
    if attributes.get("event.name") != "gemini_cli.api_response":
        return None

    model_code = str(attributes.get("model") or "unknown")
    event_timestamp = _parse_timestamp(attributes.get("event.timestamp"))
    input_tokens = _parse_int(attributes.get("input_token_count"))
    output_tokens = _parse_int(attributes.get("output_token_count"))
    cached_tokens = _parse_int(attributes.get("cached_content_token_count"))
    thoughts_tokens = _parse_int(attributes.get("thoughts_token_count"))

    return TokenUsageEvent(
        model_code=model_code,
        event_timestamp=event_timestamp,
        input_tokens=input_tokens,
        cached_input_tokens=cached_tokens,
        output_tokens=output_tokens,
        thoughts_tokens=thoughts_tokens,
    )


def _accumulate_usage_stats(stats: UsageStats, event: TokenUsageEvent, cost: float) -> None:
    """Update aggregate stats with one event."""
    stats.input_tokens += event.input_tokens
    stats.output_tokens += event.output_tokens
    stats.cached_tokens += event.cached_input_tokens
    stats.thoughts_tokens += event.thoughts_tokens
    stats.count += 1
    stats.cost += cost


def _resolve_event_date(event_timestamp: datetime, timezone: ZoneInfo | None) -> date:
    """Resolve event date in selected timezone (or local system timezone)."""
    normalized = event_timestamp if event_timestamp.tzinfo is not None else event_timestamp.replace(tzinfo=UTC)
    return normalized.astimezone(timezone).date()


def _parse_timestamp(raw_value: Any) -> datetime:
    """Parse optional timestamp, defaulting to `datetime.min` in UTC when missing or invalid."""
    if not isinstance(raw_value, str) or not raw_value:
        return datetime.min.replace(tzinfo=UTC)

    normalized = raw_value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        LOGGER.warning("Invalid timestamp format: %s", raw_value)
        return datetime.min.replace(tzinfo=UTC)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_int(raw_value: Any) -> int:
    """Parse a token count value as an integer, defaulting to zero."""
    if raw_value is None:
        return 0
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid token value: %s", raw_value)
        return 0
