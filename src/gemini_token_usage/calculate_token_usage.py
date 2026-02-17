# Copyright 2025 Ceshine Lee
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
from pathlib import Path
from collections import defaultdict
from typing import Any
from dataclasses import dataclass
from datetime import datetime, date
from zoneinfo import ZoneInfo

import orjsonl
import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .price_spec import get_price_spec
from .convert_logs import run_log_conversion

LOGGER = logging.getLogger(__name__)
TYPER_APP = typer.Typer()
TABLE_ROW_STYLES = ["white", "yellow"]

# Use a sensible default for this CLI tool
os.environ.setdefault("PRICE_CACHE_PATH", str(Path("~/.gemini/prices.json").expanduser()))


@dataclass
class UsageStats:
    """Accumulates token usage and cost statistics."""

    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    thoughts_tokens: int = 0
    count: int = 0
    cost: float = 0.0

    def __add__(self, other: "UsageStats") -> "UsageStats":
        return UsageStats(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            cached_tokens=self.cached_tokens + other.cached_tokens,
            thoughts_tokens=self.thoughts_tokens + other.thoughts_tokens,
            count=self.count + other.count,
            cost=self.cost + other.cost,
        )

    def __iadd__(self, other: "UsageStats") -> "UsageStats":
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cached_tokens += other.cached_tokens
        self.thoughts_tokens += other.thoughts_tokens
        self.count += other.count
        self.cost += other.cost
        return self


def calculate_cost(attributes: dict[str, Any], price_spec: dict[str, Any]) -> float:
    """Calculates the cost of an inference event based on token usage and price specification.

    Args:
        attributes (dict[str, Any]): The attributes dictionary from the log entry containing token counts and model info.
        price_spec (dict[str, Any]): The entire price specification dictionary mapping model names to their pricing details.

    Returns:
        The calculated cost (float) in USD.
    """
    model = attributes.get("model", "unknown")
    input_tokens = attributes.get("input_token_count") or 0
    output_tokens = attributes.get("output_token_count") or 0
    cached_tokens = attributes.get("cached_content_token_count") or 0
    thoughts_tokens = attributes.get("thoughts_token_count") or 0

    # Get price information for the model
    model_price_spec = price_spec.get(model, {})

    input_cost_per_token = model_price_spec.get("input_cost_per_token", 0)
    output_cost_per_token = model_price_spec.get("output_cost_per_token", 0)
    # Use cache_read_input_token_cost for cached tokens if available
    cached_cost_per_token = model_price_spec.get("cache_read_input_token_cost", 0)

    # Check for tiered pricing based on context length
    # Assuming input_tokens represents the total context size (including cached)
    if input_tokens > 200000:
        input_cost_per_token = model_price_spec.get("input_cost_per_token_above_200k_tokens", input_cost_per_token)
        output_cost_per_token = model_price_spec.get("output_cost_per_token_above_200k_tokens", output_cost_per_token)
        cached_cost_per_token = model_price_spec.get(
            "cache_read_input_token_cost_above_200k_tokens", cached_cost_per_token
        )

    # Calculate cost for this entry
    cost = (
        ((input_tokens - cached_tokens) * input_cost_per_token)
        + ((output_tokens + thoughts_tokens) * output_cost_per_token)
        + (cached_tokens * cached_cost_per_token)
    )
    return cost


def process_log_file(
    log_file_path: Path,
    price_spec: dict[str, Any],
    timezone: ZoneInfo | None = None,
) -> tuple[dict[tuple[str, date], UsageStats], int, bool]:
    """Processes a log file to aggregate token usage and cost by model and date.

    Args:
        log_file_path (Path): The path to the log file.
        price_spec (dict[str, Any]): The price specification dictionary.
        timezone (ZoneInfo | None): The timezone to use for determining the date.
            If None, the local system timezone is used.

    Returns:
        A tuple containing:
            - A dictionary mapping (model, date) tuples to UsageStats objects.
            - The total count (int) of processed inference events.
            - A boolean (bool) indicating if any errors were encountered during processing.
    """
    usage_by_model_day: dict[tuple[str, date], UsageStats] = defaultdict(UsageStats)
    count = 0
    encountered_errors = False

    try:
        for entry in orjsonl.stream(log_file_path):
            assert isinstance(entry, dict), f"Got an unexpected entry that is not a dict {entry}"
            attributes = entry.get("attributes", {})
            event_name = attributes.get("event.name")

            if event_name == "gemini_cli.api_response":
                model = attributes.get("model", "unknown")
                input_tokens = int(attributes.get("input_token_count") or 0)
                output_tokens = int(attributes.get("output_token_count") or 0)
                cached_tokens = int(attributes.get("cached_content_token_count") or 0)
                thoughts_tokens = int(attributes.get("thoughts_token_count") or 0)
                timestamp_str = attributes.get("event.timestamp")

                # Determine date
                event_date = date.min  # Default if timestamp is missing
                if timestamp_str:
                    try:
                        dt = datetime.fromisoformat(timestamp_str)
                        # If timezone is provided, convert to it.
                        # If timezone is None, astimezone(None) converts to local time.
                        dt = dt.astimezone(timezone)
                        event_date = dt.date()
                    except ValueError:
                        LOGGER.warning("Invalid timestamp format: %s", timestamp_str)

                cost = calculate_cost(attributes, price_spec)

                stats = usage_by_model_day[(model, event_date)]
                stats.input_tokens += input_tokens
                stats.output_tokens += output_tokens
                stats.cached_tokens += cached_tokens
                stats.thoughts_tokens += thoughts_tokens
                stats.count += 1
                stats.cost += cost
                count += 1
    except Exception as e:
        LOGGER.error("Error processing the JSONL data: %s", e)
        encountered_errors = True

    return usage_by_model_day, count, encountered_errors


def print_usage_table(
    title: str,
    data: list[tuple[Any, UsageStats]],
    console: Console,
    show_date: bool = False,
):
    """Helper to print usage statistics table."""
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
    table.add_column("Thoughts Tokens", footer_style="bold", justify="right")
    table.add_column("Cost ($)", footer_style="bold", justify="right")
    table.add_column("Total Tokens", footer_style="bold", justify="right")

    total_stats = UsageStats()

    # Logic for alternating row styles based on date
    last_date_str = None
    style_index = 0
    # Alternating styles for different days
    styles = TABLE_ROW_STYLES

    for key, stats in data:
        total_stats += stats
        total_tokens = (
            # Pecularity of Gemini CLI's logs: `input_tokens` includes `cached_tokens`, but `output_tokens` does not include `thoughts_tokens`
            stats.input_tokens + stats.output_tokens + stats.thoughts_tokens
        )
        row_args = []
        row_style = None

        if show_date:
            # Unpack key assuming it is (date_str, model)
            date_str, model_name = key

            if last_date_str is not None and date_str != last_date_str:
                style_index = (style_index + 1) % len(styles)
            last_date_str = date_str
            row_style = styles[style_index]

            row_args.append(date_str)
            row_args.append(model_name)
        else:
            # key is model
            row_args.append(key)

        row_args.extend(
            [
                str(stats.count),
                f"{stats.input_tokens:,}",
                f"{stats.output_tokens:,}",
                f"{stats.cached_tokens:,}",
                f"{stats.thoughts_tokens:,}",
                f"{stats.cost:,.6f}",
                f"{total_tokens:,}",
            ]
        )
        table.add_row(*row_args, style=row_style)

    # Set footers
    # Adjust column indices based on show_date
    col_offset = 1 if show_date else 0

    table.columns[1 + col_offset].footer = str(total_stats.count)
    table.columns[2 + col_offset].footer = f"{total_stats.input_tokens:,}"
    table.columns[3 + col_offset].footer = f"{total_stats.output_tokens:,}"
    table.columns[4 + col_offset].footer = f"{total_stats.cached_tokens:,}"
    table.columns[5 + col_offset].footer = f"{total_stats.thoughts_tokens:,}"
    table.columns[6 + col_offset].footer = f"{total_stats.cost:,.6f}"
    table.columns[
        7 + col_offset
    ].footer = f"{(total_stats.input_tokens + total_stats.output_tokens + total_stats.thoughts_tokens):,}"

    console.print(table)


def analyze_token_usage(
    log_file_path: Path,
    enable_archiving: bool = False,
    log_simplify_level: int = 1,
    timezone: str | None = None,
) -> int:
    """Calculates and displays token usage and cost from a Gemini CLI log file.

    Args:
        log_file_path (Path): Path to the JSONL log file to analyze.
          - If a folder is provided, it attempts to find `telemetry.log` (or `.gemini/telemetry.log`) to convert.
          - It also checks for `telemetry.jsonl` (or `gemini/telemetry.jsonl`) to use directly if no log file is found.
          - If a .jsonl file is provided, it will be processed directly.
        enable_archiving (bool): Enable archiving (moving to `/tmp`) when a folder is provided as `log_file_path`. Only enable it if no Gemini CLI is currently running.
        log_simplify_level (int): Level of simplification for the JSONL log file. Available levels: 0 (no simplification), 1 (default), 2 (trim fields), and 3 (trim attributes)
        timezone (str | None): Timezone string for daily aggregation.

    Returns:
        int: Exit code (0 for success, non-zero for failure).
    """
    console = Console()

    console.print(f"[bold]Gemini CLI Token Usage and Cost Analysis[/bold] version `{__version__}` \n")

    if log_file_path.is_dir():
        source_log_file, jsonl_file = None, None
        # Try to find the the default file in the specified folder
        if (log_file_path / "telemetry.log").exists():
            source_log_file = log_file_path / "telemetry.log"
        elif (log_file_path / ".gemini" / "telemetry.log").exists():
            source_log_file = log_file_path / ".gemini" / "telemetry.log"
        elif (log_file_path / "telemetry.jsonl").exists():
            jsonl_file = log_file_path / "telemetry.jsonl"
        elif (log_file_path / ".gemini" / "telemetry.jsonl").exists():
            jsonl_file = log_file_path / ".gemini" / "telemetry.jsonl"
        else:
            raise FileNotFoundError(
                f"Could not find telemetry.log or telemetry.jsonl in {log_file_path} nor in its '.gemini' or 'gemini' subdirectories."
            )
        if source_log_file is not None and jsonl_file is None:
            log_file_path = run_log_conversion(
                source_log_file,
                source_log_file.parent / "telemetry.jsonl",
                simplify_level=log_simplify_level,
                archiving_enabled=enable_archiving,
                archive_folder_path=Path("/tmp"),
            )
            console.print(
                f"Converted {source_log_file} to {log_file_path} with archiving [bold]{'ENABLED' if enable_archiving else 'DISABLED'}[/bold]",
                style="green",
            )
        elif jsonl_file is not None:
            console.print(f"Using [bold]{jsonl_file}[/bold] as the JSONL log file", style="green")
            log_file_path = jsonl_file
        else:
            raise RuntimeError("Incorrect combination of the `source_log_file` and the `jsonl_file` values.")
    elif log_file_path.suffix != ".jsonl":
        raise ValueError(f"Log file must be a .jsonl file, got {log_file_path}")

    if not log_file_path.exists():
        console.print(f"Error: {log_file_path} not found.", style="bold red")
        return 1

    # Determine timezone
    tz = None
    if timezone:
        try:
            tz = ZoneInfo(timezone)
        except Exception:
            console.print(f"Invalid timezone: {timezone}. Using local time.", style="bold yellow")

    price_spec = get_price_spec()

    console.print(f"Reading {log_file_path}...")
    usage_by_model_day, count, encountered_errors = process_log_file(log_file_path, price_spec, timezone=tz)

    if encountered_errors:
        console.print("Displaying results processed so far...", style="bold yellow")

    if count == 0:
        console.print("No inference events found in the log.")
        return 0

    console.print(f"\nFound {count} inference events.\n")

    # Prepare Daily Stats
    # Group keys by date, then model
    sorted_keys = sorted(usage_by_model_day.keys(), key=lambda x: (x[1], x[0]))
    daily_data = [((key[1].isoformat(), key[0]), usage_by_model_day[key]) for key in sorted_keys]
    print_usage_table("Daily Token Usage", daily_data, console, show_date=True)

    console.print("\n")

    # Prepare Aggregated Daily Costs
    daily_costs: dict[date, float] = defaultdict(float)
    for (_, day), stats in usage_by_model_day.items():
        daily_costs[day] += stats.cost

    cost_table = Table(title="Daily Aggregated Costs", show_footer=True, title_justify="left")
    cost_table.add_column("Date", justify="left")
    cost_table.add_column("Cost ($)", justify="right", footer_style="bold")

    total_daily_cost = 0.0

    # Alternating styles for different days
    styles = TABLE_ROW_STYLES
    style_index = 0

    for day in sorted(daily_costs.keys()):
        cost = daily_costs[day]
        total_daily_cost += cost
        row_style = styles[style_index % len(styles)]
        cost_table.add_row(day.isoformat(), f"{cost:,.6f}", style=row_style)
        style_index += 1  # Increment style_index for the next date

    cost_table.columns[1].footer = f"{total_daily_cost:,.6f}"
    console.print(cost_table)
    console.print("\n")

    # Prepare Overall Stats
    overall_usage: dict[str, UsageStats] = defaultdict(UsageStats)
    for (model, _), stats in usage_by_model_day.items():
        overall_usage[model] += stats

    overall_data = sorted(overall_usage.items())
    print_usage_table("Overall Token Usage by Model", overall_data, console, show_date=False)

    if encountered_errors:
        return 1
    return 0


@TYPER_APP.command()
def main(
    log_file_path: Path,
    enable_archiving: bool = False,
    log_simplify_level: int = 1,
    timezone: str = typer.Option(
        None,
        "--timezone",
        "-tz",
        help="Timezone to use for daily stats (e.g., 'UTC', 'America/New_York'). Defaults to local system time.",
    ),
):
    """Calculates and displays token usage and cost from a Gemini CLI log file.

    Args:
        log_file_path (Path): Path to the JSONL log file to analyze.
          - If a folder is provided, it attempts to find `telemetry.log` (or `.gemini/telemetry.log`) to convert.
          - It also checks for `telemetry.jsonl` (or `gemini/telemetry.jsonl`) to use directly if no log file is found.
          - If a .jsonl file is provided, it will be processed directly.
        enable_archiving (bool): Enable archiving (moving to `/tmp`) when a folder is provided as `log_file_path`. Only enable it if no Gemini CLI is currently running.
        log_simplify_level (int): Level of simplification for the JSONL log file. Available levels: 0 (no simplification), 1 (default), 2 (trim fields), and 3 (trim attributes)
        timezone (str): Timezone string for daily aggregation.
    """
    try:
        exit_code = analyze_token_usage(log_file_path, enable_archiving, log_simplify_level, timezone)
        if exit_code != 0:
            raise typer.Exit(exit_code)
    except (FileNotFoundError, ValueError) as e:
        raise typer.BadParameter(str(e))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    TYPER_APP()
