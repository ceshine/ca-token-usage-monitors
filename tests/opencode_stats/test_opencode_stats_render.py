"""Unit tests for OpenCode stats rendering."""

from __future__ import annotations

from datetime import date

from rich.console import Console

from opencode_token_usage.stats.render import render_daily_usage_statistics
from opencode_token_usage.stats.schemas import DailyUsageStatistics, UsageStats


def test_render_daily_usage_statistics_includes_provider_and_model_values() -> None:
    """Rendered usage tables should include provider and model values in model breakdown."""
    report = DailyUsageStatistics(
        usage_by_model_day={
            ("openrouter", "qwen/qwen3-coder", date(2026, 2, 22)): UsageStats(
                input_tokens=160,
                output_tokens=15,
                cached_tokens=40,
                cache_write_tokens=0,
                thoughts_tokens=5,
                count=1,
                cost=0.572,
            )
        },
        daily_costs={date(2026, 2, 22): 0.572},
        overall_usage={
            ("openrouter", "qwen/qwen3-coder"): UsageStats(
                input_tokens=160,
                output_tokens=15,
                cached_tokens=40,
                cache_write_tokens=0,
                thoughts_tokens=5,
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
