"""Rich rendering helpers for Claude token usage statistics."""

from __future__ import annotations

from datetime import date
from typing import Any

from rich.table import Table
from rich.console import Console

from .schemas import UsageStats, DailyUsageStatistics

TABLE_ROW_STYLES = ["white", "yellow"]


def render_aggregated_daily_stats(
    usage_by_model_day: dict[tuple[str, date], UsageStats],
    console: Console,
) -> None:
    """Aggregate usage across models per day and render a table with totals.

    Args:
        usage_by_model_day: Usage statistics keyed by (model, day).
        console: Rich console to print the table to.
    """
    daily_stats: dict[date, UsageStats] = {}
    for (_model, day), stats in usage_by_model_day.items():
        if day not in daily_stats:
            daily_stats[day] = UsageStats()
        daily_stats[day] += stats

    table = Table(title="Daily Aggregated Statistics", show_footer=True, title_justify="left")
    table.add_column("Date", justify="left")
    table.add_column("Requests", footer_style="bold", justify="right")
    table.add_column("Input Tokens", footer_style="bold", justify="right")
    table.add_column("Output Tokens", footer_style="bold", justify="right")
    table.add_column("Cached Tokens", footer_style="bold", justify="right")
    table.add_column("Cache Write Tokens", footer_style="bold", justify="right")
    table.add_column("Cost ($)", footer_style="bold", justify="right")
    table.add_column("Total Tokens", footer_style="bold", justify="right")

    total_stats = UsageStats()
    for index, day in enumerate(sorted(daily_stats)):
        stats = daily_stats[day]
        total_stats += stats
        total_tokens = stats.input_tokens + stats.output_tokens + stats.cached_tokens + stats.cache_write_tokens
        style = TABLE_ROW_STYLES[index % len(TABLE_ROW_STYLES)]
        table.add_row(
            day.isoformat(),
            str(stats.count),
            f"{stats.input_tokens:,}",
            f"{stats.output_tokens:,}",
            f"{stats.cached_tokens:,}",
            f"{stats.cache_write_tokens:,}",
            f"{stats.cost:,.6f}",
            f"{total_tokens:,}",
            style=style,
        )

    grand_total_tokens = (
        total_stats.input_tokens
        + total_stats.output_tokens
        + total_stats.cached_tokens
        + total_stats.cache_write_tokens
    )
    table.columns[0].footer = "Grand Total"
    table.columns[1].footer = str(total_stats.count)
    table.columns[2].footer = f"{total_stats.input_tokens:,}"
    table.columns[3].footer = f"{total_stats.output_tokens:,}"
    table.columns[4].footer = f"{total_stats.cached_tokens:,}"
    table.columns[5].footer = f"{total_stats.cache_write_tokens:,}"
    table.columns[6].footer = f"{total_stats.cost:,.6f}"
    table.columns[7].footer = f"{grand_total_tokens:,}"
    console.print(table)


def render_daily_usage_statistics(report: DailyUsageStatistics, console: Console) -> None:
    """Render daily and overall statistics tables."""
    if report.total_events == 0:
        console.print("No token usage events found in the database.")
        return

    sorted_keys = sorted(report.usage_by_model_day.keys(), key=lambda item: (item[1], item[0]))
    daily_data = [((day.isoformat(), model), report.usage_by_model_day[(model, day)]) for model, day in sorted_keys]
    _print_usage_table("Daily Token Usage", daily_data, console, show_date=True)
    console.print("\n")

    render_aggregated_daily_stats(report.usage_by_model_day, console)
    console.print("\n")

    overall_data = sorted(report.overall_usage.items(), key=lambda item: item[0])
    _print_usage_table("Overall Token Usage by Model", overall_data, console, show_date=False)


def _print_usage_table(
    title: str,
    data: list[tuple[Any, UsageStats]],
    console: Console,
    show_date: bool = False,
) -> None:
    """Render one usage table with totals."""
    table = Table(
        title=title,
        show_footer=True,
        footer_style="bold",
        title_justify="left",
    )

    if show_date:
        table.add_column("Date", justify="left")
    table.add_column("Model", footer="Grand Total", justify="left")
    table.add_column("Requests", footer_style="bold", justify="right")
    table.add_column("Input Tokens", footer_style="bold", justify="right")
    table.add_column("Output Tokens", footer_style="bold", justify="right")
    table.add_column("Cached Tokens", footer_style="bold", justify="right")
    table.add_column("Cache Write Tokens", footer_style="bold", justify="right")
    table.add_column("Cost ($)", footer_style="bold", justify="right")
    table.add_column("Total Tokens", footer_style="bold", justify="right")

    total_stats = UsageStats()
    last_date: str | None = None
    style_index = 0

    for key, stats in data:
        total_stats += stats
        total_tokens = stats.input_tokens + stats.output_tokens + stats.cached_tokens + stats.cache_write_tokens

        row_args: list[str] = []
        row_style: str | None = None

        if show_date:
            date_str, model_name = key
            if last_date is not None and date_str != last_date:
                style_index = (style_index + 1) % len(TABLE_ROW_STYLES)
            last_date = date_str
            row_style = TABLE_ROW_STYLES[style_index]
            row_args.extend([date_str, model_name])
        else:
            model_name = key
            row_args.extend([model_name])

        row_args.extend(
            [
                str(stats.count),
                f"{stats.input_tokens:,}",
                f"{stats.output_tokens:,}",
                f"{stats.cached_tokens:,}",
                f"{stats.cache_write_tokens:,}",
                f"{stats.cost:,.6f}",
                f"{total_tokens:,}",
            ]
        )
        table.add_row(*row_args, style=row_style)

    col_offset = 1 if show_date else 0
    table.columns[1 + col_offset].footer = str(total_stats.count)
    table.columns[2 + col_offset].footer = f"{total_stats.input_tokens:,}"
    table.columns[3 + col_offset].footer = f"{total_stats.output_tokens:,}"
    table.columns[4 + col_offset].footer = f"{total_stats.cached_tokens:,}"
    table.columns[5 + col_offset].footer = f"{total_stats.cache_write_tokens:,}"
    table.columns[6 + col_offset].footer = f"{total_stats.cost:,.6f}"
    table.columns[
        7 + col_offset
    ].footer = f"{(total_stats.input_tokens + total_stats.output_tokens + total_stats.cached_tokens + total_stats.cache_write_tokens):,}"

    console.print(table)
