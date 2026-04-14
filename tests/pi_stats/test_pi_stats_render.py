"""Unit tests for Pi agent stats rendering."""

from __future__ import annotations

from datetime import date

from rich.console import Console

from coding_agent_usage_monitors.pi_token_usage.stats.render import render_daily_usage_statistics
from coding_agent_usage_monitors.pi_token_usage.stats.schemas import UsageStats, DailyUsageStatistics


def test_render_daily_usage_statistics_includes_provider_and_model_values() -> None:
    """Rendered usage tables should include provider and model values in model breakdown."""
    report = DailyUsageStatistics(
        usage_by_model_day={
            ("openrouter", "qwen/qwen3-coder", date(2026, 4, 13)): UsageStats(
                input_tokens=160,
                output_tokens=15,
                cached_tokens=40,
                cache_write_tokens=0,
                count=1,
                cost=0.572,
            )
        },
        daily_costs={date(2026, 4, 13): 0.572},
        overall_usage={
            ("openrouter", "qwen/qwen3-coder"): UsageStats(
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
    assert "openrouter" in output
    assert "qwen/qwen3-coder" in output


def test_render_daily_usage_statistics_empty_report_prints_placeholder() -> None:
    """Empty report should emit the no-events placeholder, not a table."""
    report = DailyUsageStatistics(
        usage_by_model_day={},
        daily_costs={},
        overall_usage={},
        total_events=0,
    )

    console = Console(record=True, width=220)
    render_daily_usage_statistics(report, console)

    output = console.export_text()
    assert "No token usage events found" in output
