"""Typed schemas used by the OpenCode stats pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class TokenUsageEvent:
    """One usage event loaded from DuckDB."""

    provider_code: str
    model_code: str
    event_timestamp: datetime
    input_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    output_tokens: int
    reasoning_tokens: int


@dataclass
class UsageStats:
    """Accumulates token usage and cost statistics."""

    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    cache_write_tokens: int = 0
    thoughts_tokens: int = 0
    count: int = 0
    cost: float = 0.0

    def __add__(self, other: "UsageStats") -> "UsageStats":
        """Return a new object with summed stats."""
        return UsageStats(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            cached_tokens=self.cached_tokens + other.cached_tokens,
            cache_write_tokens=self.cache_write_tokens + other.cache_write_tokens,
            thoughts_tokens=self.thoughts_tokens + other.thoughts_tokens,
            count=self.count + other.count,
            cost=self.cost + other.cost,
        )

    def __iadd__(self, other: "UsageStats") -> "UsageStats":
        """Mutate this object by adding stats in-place."""
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cached_tokens += other.cached_tokens
        self.cache_write_tokens += other.cache_write_tokens
        self.thoughts_tokens += other.thoughts_tokens
        self.count += other.count
        self.cost += other.cost
        return self


@dataclass(frozen=True)
class DailyUsageStatistics:
    """Aggregated daily usage statistics and costs."""

    usage_by_model_day: dict[tuple[str, str, date], UsageStats]
    daily_costs: dict[date, float]
    overall_usage: dict[tuple[str, str], UsageStats]
    total_events: int
