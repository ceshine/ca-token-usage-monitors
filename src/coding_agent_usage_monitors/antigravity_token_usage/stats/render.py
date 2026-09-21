"""Rich rendering helpers for AntiGravity token usage statistics."""

from __future__ import annotations

from datetime import date
from typing import Any

from rich.table import Table
from rich.console import Console

from .schemas import UsageStats, DailyUsageStatistics

TABLE_ROW_STYLES = ["white", "yellow"]


def render_daily_usage_statistics(report: DailyUsageStatistics, console: Console) -> None:
    """Render daily, daily-aggregate, and overall usage statistics.

    Args:
        report: Aggregated AntiGravity usage statistics.
        console: Rich console receiving the output.
    """
    if report.total_events == 0:
        console.print("No token usage events found in the database.")
        return

    sorted_keys = sorted(report.usage_by_model_day, key=lambda item: (item[1], item[0]))
    daily_data = [((day.isoformat(), model), report.usage_by_model_day[(model, day)]) for model, day in sorted_keys]
    _print_usage_table("Daily Token Usage", daily_data, console, show_date=True)
    console.print()

    _render_daily_aggregates(report, console)
    console.print()

    _print_usage_table("Overall Token Usage by Model", sorted(report.overall_usage.items()), console)


def _render_daily_aggregates(report: DailyUsageStatistics, console: Console) -> None:
    """Render daily totals and estimated costs.

    Args:
        report: Aggregated AntiGravity usage statistics.
        console: Rich console receiving the output.
    """
    daily_stats: dict[date, UsageStats] = {}
    for (_, day), stats in report.usage_by_model_day.items():
        daily_stats[day] = daily_stats.get(day, UsageStats()) + stats

    table = _build_usage_table("Daily Aggregated Usage", show_date=True, include_model=False)
    total_stats = UsageStats()
    for index, day in enumerate(sorted(daily_stats)):
        stats = daily_stats[day]
        total_stats += stats
        table.add_row(day.isoformat(), *_stats_cells(stats), style=TABLE_ROW_STYLES[index % len(TABLE_ROW_STYLES)])
    _set_usage_footers(table, total_stats, show_date=True, include_model=False)
    console.print(table)


def _print_usage_table(
    title: str,
    data: list[tuple[Any, UsageStats]],
    console: Console,
    show_date: bool = False,
) -> None:
    """Render model-level usage data with totals.

    Args:
        title: Table title.
        data: Model usage rows to render.
        console: Rich console receiving the output.
        show_date: Whether each row includes a date field.
    """
    table = _build_usage_table(title, show_date=show_date, include_model=True)
    total_stats = UsageStats()
    last_date: str | None = None
    style_index = 0
    for key, stats in data:
        total_stats += stats
        row_style: str | None = None
        if show_date:
            date_string, model_code = key
            if last_date is not None and date_string != last_date:
                style_index = (style_index + 1) % len(TABLE_ROW_STYLES)
            last_date = date_string
            row_style = TABLE_ROW_STYLES[style_index]
            row = [date_string, model_code]
        else:
            row = [str(key)]
        table.add_row(*row, *_stats_cells(stats), style=row_style)
    _set_usage_footers(table, total_stats, show_date=show_date, include_model=True)
    console.print(table)


def _build_usage_table(title: str, show_date: bool, include_model: bool) -> Table:
    """Create a token usage table with standard columns.

    Args:
        title: Table title.
        show_date: Whether to add a date column.
        include_model: Whether to add a model column.

    Returns:
        Configured Rich table.
    """
    table = Table(title=title, show_footer=True, footer_style="bold", title_justify="left")
    if show_date:
        table.add_column("Date", justify="left")
    if include_model:
        table.add_column("Model", footer="Grand Total", justify="left", overflow="fold")
    table.add_column("Requests", footer_style="bold", justify="right")
    table.add_column("Input Tokens", footer_style="bold", justify="right")
    table.add_column("Output Tokens", footer_style="bold", justify="right")
    table.add_column("Reasoning Tokens", footer_style="bold", justify="right")
    table.add_column("Cache Read Tokens", footer_style="bold", justify="right")
    table.add_column("Cache Write Tokens", footer_style="bold", justify="right")
    table.add_column("Cost ($)", footer_style="bold", justify="right")
    table.add_column("Total Tokens", footer_style="bold", justify="right")
    return table


def _stats_cells(stats: UsageStats) -> list[str]:
    """Format one aggregate as table cells.

    Args:
        stats: Aggregate usage values to format.

    Returns:
        Ordered formatted table cells.
    """
    return [
        str(stats.count),
        f"{stats.input_tokens:,}",
        f"{stats.output_tokens:,}",
        f"{stats.reasoning_tokens:,}",
        f"{stats.cache_read_tokens:,}",
        f"{stats.cache_creation_tokens:,}",
        f"{stats.cost:,.6f}",
        f"{_total_tokens(stats):,}",
    ]


def _set_usage_footers(table: Table, stats: UsageStats, show_date: bool, include_model: bool) -> None:
    """Set total values in a usage table footer.

    Args:
        table: Table receiving footer values.
        stats: Aggregate totals.
        show_date: Whether the table includes a date column.
        include_model: Whether the table includes a model column.
    """
    offset = int(show_date) + int(include_model)
    table.columns[offset].footer = str(stats.count)
    table.columns[offset + 1].footer = f"{stats.input_tokens:,}"
    table.columns[offset + 2].footer = f"{stats.output_tokens:,}"
    table.columns[offset + 3].footer = f"{stats.reasoning_tokens:,}"
    table.columns[offset + 4].footer = f"{stats.cache_read_tokens:,}"
    table.columns[offset + 5].footer = f"{stats.cache_creation_tokens:,}"
    table.columns[offset + 6].footer = f"{stats.cost:,.6f}"
    table.columns[offset + 7].footer = f"{_total_tokens(stats):,}"


def _total_tokens(stats: UsageStats) -> int:
    """Return the total of all separately reported token categories.

    Args:
        stats: Aggregate usage values.

    Returns:
        Total token count.
    """
    return (
        stats.input_tokens
        + stats.output_tokens
        + stats.reasoning_tokens
        + stats.cache_creation_tokens
        + stats.cache_read_tokens
    )
