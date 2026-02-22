"""Rich rendering helpers for OpenCode token usage statistics."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table

from .schemas import DailyUsageStatistics, UsageStats

TABLE_ROW_STYLES = ["white", "yellow"]


def render_daily_usage_statistics(report: DailyUsageStatistics, console: Console) -> None:
    """Render daily and overall statistics tables."""
    if report.total_events == 0:
        console.print("No token usage events found in the database.")
        return

    sorted_keys = sorted(report.usage_by_model_day.keys(), key=lambda item: (item[2], item[0], item[1]))
    daily_data = [
        ((day.isoformat(), provider, model), report.usage_by_model_day[(provider, model, day)])
        for provider, model, day in sorted_keys
    ]
    _print_usage_table("Daily Token Usage", daily_data, console, show_date=True)
    console.print("\n")

    cost_table = Table(title="Daily Aggregated Costs", show_footer=True, title_justify="left")
    cost_table.add_column("Date", justify="left")
    cost_table.add_column("Cost ($)", justify="right", footer_style="bold")

    total_daily_cost = 0.0
    for index, day in enumerate(sorted(report.daily_costs)):
        day_cost = report.daily_costs[day]
        total_daily_cost += day_cost
        style = TABLE_ROW_STYLES[index % len(TABLE_ROW_STYLES)]
        cost_table.add_row(day.isoformat(), f"{day_cost:,.6f}", style=style)

    cost_table.columns[1].footer = f"{total_daily_cost:,.6f}"
    console.print(cost_table)
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
    table.add_column("Provider", footer="Grand Total", justify="left")
    table.add_column("Model", justify="left")
    table.add_column("Requests", footer_style="bold", justify="right")
    table.add_column("Input Tokens", footer_style="bold", justify="right")
    table.add_column("Output Tokens", footer_style="bold", justify="right")
    table.add_column("Cached Tokens", footer_style="bold", justify="right")
    table.add_column("Cache Write Tokens", footer_style="bold", justify="right")
    table.add_column("Thoughts Tokens", footer_style="bold", justify="right")
    table.add_column("Cost ($)", footer_style="bold", justify="right")
    table.add_column("Total Tokens", footer_style="bold", justify="right")

    total_stats = UsageStats()
    last_date: str | None = None
    style_index = 0

    for key, stats in data:
        total_stats += stats
        total_tokens = (
            stats.input_tokens
            + stats.output_tokens
            + stats.cached_tokens
            + stats.cache_write_tokens
            + stats.thoughts_tokens
        )

        row_args: list[str] = []
        row_style: str | None = None

        if show_date:
            date_str, provider_name, model_name = key
            if last_date is not None and date_str != last_date:
                style_index = (style_index + 1) % len(TABLE_ROW_STYLES)
            last_date = date_str
            row_style = TABLE_ROW_STYLES[style_index]
            row_args.extend([date_str, provider_name, model_name])
        else:
            provider_name, model_name = key
            row_args.extend([provider_name, model_name])

        row_args.extend(
            [
                str(stats.count),
                f"{stats.input_tokens:,}",
                f"{stats.output_tokens:,}",
                f"{stats.cached_tokens:,}",
                f"{stats.cache_write_tokens:,}",
                f"{stats.thoughts_tokens:,}",
                f"{stats.cost:,.6f}",
                f"{total_tokens:,}",
            ]
        )
        table.add_row(*row_args, style=row_style)

    col_offset = 1 if show_date else 0
    table.columns[2 + col_offset].footer = str(total_stats.count)
    table.columns[3 + col_offset].footer = f"{total_stats.input_tokens:,}"
    table.columns[4 + col_offset].footer = f"{total_stats.output_tokens:,}"
    table.columns[5 + col_offset].footer = f"{total_stats.cached_tokens:,}"
    table.columns[6 + col_offset].footer = f"{total_stats.cache_write_tokens:,}"
    table.columns[7 + col_offset].footer = f"{total_stats.thoughts_tokens:,}"
    table.columns[8 + col_offset].footer = f"{total_stats.cost:,.6f}"
    table.columns[
        9 + col_offset
    ].footer = f"{(total_stats.input_tokens + total_stats.output_tokens + total_stats.cached_tokens + total_stats.cache_write_tokens + total_stats.thoughts_tokens):,}"

    console.print(table)
