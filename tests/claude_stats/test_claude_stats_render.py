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


def test_render_daily_usage_statistics_includes_aggregated_daily_table() -> None:
    """Aggregated daily table should sum requests and tokens across models."""
    report = DailyUsageStatistics(
        usage_by_model_day={
            ("model-a", date(2026, 2, 22)): UsageStats(input_tokens=100, output_tokens=10, count=2, cost=0.1),
            ("model-b", date(2026, 2, 22)): UsageStats(input_tokens=50, output_tokens=5, count=3, cost=0.2),
        },
        daily_costs={date(2026, 2, 22): 0.3},
        overall_usage={},
        total_events=5,
    )

    console = Console(record=True, width=220)
    render_daily_usage_statistics(report, console)

    lines = console.export_text().splitlines()
    assert any("Daily Aggregated Statistics" in line for line in lines)
    day_rows = [line for line in lines if "2026-02-22" in line and "model-" not in line]
    assert len(day_rows) == 1
    assert _split_cells(day_rows[0]) == ["2026-02-22", "5", "150", "15", "0", "0", "0.300000", "165"]


def test_render_daily_usage_statistics_aggregated_table_sorts_days_and_sums_footer() -> None:
    """Aggregated daily table should list days in order and total all days in the footer."""
    report = DailyUsageStatistics(
        usage_by_model_day={
            ("model-a", date(2026, 2, 23)): UsageStats(
                input_tokens=10, output_tokens=2, cached_tokens=3, cache_write_tokens=4, count=1, cost=0.5
            ),
            ("model-a", date(2026, 2, 22)): UsageStats(input_tokens=100, output_tokens=10, count=2, cost=0.1),
        },
        daily_costs={date(2026, 2, 22): 0.1, date(2026, 2, 23): 0.5},
        overall_usage={},
        total_events=3,
    )

    console = Console(record=True, width=220)
    render_daily_usage_statistics(report, console)

    lines = console.export_text().splitlines()
    title_index = next(i for i, line in enumerate(lines) if "Daily Aggregated Statistics" in line)
    table_lines = lines[title_index:]
    rows = [_split_cells(line) for line in table_lines if line.startswith("│")]
    assert rows[0] == ["2026-02-22", "2", "100", "10", "0", "0", "0.100000", "110"]
    assert rows[1] == ["2026-02-23", "1", "10", "2", "3", "4", "0.500000", "19"]
    assert rows[2] == ["Grand Total", "3", "110", "12", "3", "4", "0.600000", "129"]


def _split_cells(line: str) -> list[str]:
    """Split one rendered table row into stripped cell values."""
    return [cell.strip() for cell in line.strip("│").split("│")]


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
