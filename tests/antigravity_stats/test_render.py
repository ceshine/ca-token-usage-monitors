"""Unit tests for AntiGravity stats rendering."""

from __future__ import annotations

from datetime import date

from rich.console import Console

from coding_agent_usage_monitors.antigravity_token_usage.stats.render import render_daily_usage_statistics
from coding_agent_usage_monitors.antigravity_token_usage.stats.schemas import UsageStats, DailyUsageStatistics


def test_render_daily_usage_statistics_includes_token_categories() -> None:
    """Rendered report should display model and AntiGravity-specific token fields."""
    report = DailyUsageStatistics(
        usage_by_model_day={
            ("gemini-3.8-flash-high", date(2026, 9, 21)): UsageStats(
                input_tokens=100,
                output_tokens=10,
                reasoning_tokens=5,
                cache_creation_tokens=4,
                cache_read_tokens=20,
                count=1,
                cost=0.1,
            )
        },
        daily_costs={date(2026, 9, 21): 0.1},
        overall_usage={
            "gemini-3.8-flash-high": UsageStats(
                input_tokens=100,
                output_tokens=10,
                reasoning_tokens=5,
                cache_creation_tokens=4,
                cache_read_tokens=20,
                count=1,
                cost=0.1,
            )
        },
        total_events=1,
    )
    console = Console(record=True, width=220)

    render_daily_usage_statistics(report, console)

    output = console.export_text()
    assert "gemini-3.8-flash-high" in output
    assert "Reasoning Tokens" in output
    assert "Cache Write Tokens" in output
