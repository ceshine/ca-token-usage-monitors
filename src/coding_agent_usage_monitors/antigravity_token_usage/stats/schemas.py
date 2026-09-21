"""Typed schemas used by the AntiGravity stats pipeline."""

from __future__ import annotations

from datetime import date, datetime
from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True)
class TokenUsageEvent:
    """One canonical AntiGravity usage event loaded from DuckDB."""

    model_code: str
    event_timestamp: datetime
    input_tokens: int
    output_tokens: int
    reasoning_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int


@dataclass
class UsageStats:
    """Accumulates token usage and cost statistics."""

    input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cache_creation_tokens: int = 0
    cache_read_tokens: int = 0
    count: int = 0
    cost: float = 0.0

    def __add__(self, other: UsageStats) -> UsageStats:
        """Return a new object with summed stats."""
        return UsageStats(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            reasoning_tokens=self.reasoning_tokens + other.reasoning_tokens,
            cache_creation_tokens=self.cache_creation_tokens + other.cache_creation_tokens,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            count=self.count + other.count,
            cost=self.cost + other.cost,
        )

    def __iadd__(self, other: UsageStats) -> Self:
        """Mutate this object by adding stats in-place."""
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.reasoning_tokens += other.reasoning_tokens
        self.cache_creation_tokens += other.cache_creation_tokens
        self.cache_read_tokens += other.cache_read_tokens
        self.count += other.count
        self.cost += other.cost
        return self


@dataclass(frozen=True)
class DailyUsageStatistics:
    """Aggregated daily usage statistics and costs."""

    usage_by_model_day: dict[tuple[str, date], UsageStats]
    # TODO: `daily_costs` is populated by StatsService but never read; the renderer derives
    # per-day costs from `usage_by_model_day`. Remove the field or consume it in the renderer.
    daily_costs: dict[date, float]
    overall_usage: dict[str, UsageStats]
    total_events: int
