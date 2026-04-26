"""Unit tests for Claude stats rendering."""

from __future__ import annotations

from datetime import date

from rich.console import Console

from coding_agent_usage_monitors.claude_token_usage.stats.render import render_daily_usage_statistics
from coding_agent_usage_monitors.claude_token_usage.stats.schemas import UsageStats, DailyUsageStatistics


def test_render_daily_usage_statistics_includes_model_values() -> None:
    """Rendered usage tables should include model values in model breakdown."""
    report = DailyUsageStatistics(
        usage_by_model_day={
            ("claude-sonnet-4-5-20250514", date(2026, 2, 22)): UsageStats(
                input_tokens=160,
                output_tokens=15,
                cached_tokens=40,
                cache_write_tokens=0,
                count=1,
                cost=0.572,
            )
        },
        daily_costs={date(2026, 2, 22): 0.572},
        overall_usage={
            "claude-sonnet-4-5-20250514": UsageStats(
                input_tokens=160,
                output_tokens=15,
                cached_tokens=40,
                cache_write_tokens=0,
                count=1,
                cost=0.572,
            )
        },
        total_events=1,
    )

    console = Console(record=True, width=220)
    render_daily_usage_statistics(report, console)

    output = console.export_text()
    assert "claude-sonnet-4-5-20250514" in output


def test_render_daily_usage_statistics_prints_no_events_message_when_empty() -> None:
    """Empty report should print a no-events message instead of tables."""
    report = DailyUsageStatistics(
        usage_by_model_day={},
        daily_costs={},
        overall_usage={},
        total_events=0,
    )

    console = Console(record=True, width=220)
    render_daily_usage_statistics(report, console)

    output = console.export_text()
    assert "No token usage events" in output
