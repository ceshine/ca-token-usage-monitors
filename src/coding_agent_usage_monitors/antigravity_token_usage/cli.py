"""CLI entrypoints for AntiGravity token usage tooling."""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import typer
from rich.console import Console

from .stats.render import render_daily_usage_statistics
from .stats.schemas import DailyUsageStatistics
from .stats.service import StatsService
from .stats.repository import StatsRepository, StatsRepositoryError
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .ingestion.repository import IngestionRepository
from ..common.paths import get_default_database_path
from ..common.cli_utils import parse_since_date, parse_until_date
from ..common.model_pricing import PriceSpecError

LOGGER = logging.getLogger(__name__)
DEFAULT_DATABASE_PATH = get_default_database_path()

TYPER_APP = typer.Typer(help="AntiGravity token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


@TYPER_APP.command("ingest")
def ingest_command(
    database_path: Path = typer.Option(  # noqa: B008
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and usage events.",
    ),
    data_dir: str | None = typer.Option(
        None,
        "--data-dir",
        "-s",
        help="Custom root directory or comma-separated list of roots for AntiGravity databases.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Ingest AntiGravity SQLite conversation databases into DuckDB."""
    _configure_logging(verbose)
    counters = _run_ingestion(database_path=database_path, data_dir=data_dir)
    _emit_summary(counters)
    if counters.databases_failed > 0:
        raise typer.Exit(code=1)
    _emit_last_7_days_stats(database_path=database_path, console=Console())


@TYPER_APP.command("stats")
def stats_command(
    database_path: Path = typer.Option(  # noqa: B008
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and usage events.",
    ),
    timezone: str | None = typer.Option(
        None,
        "--timezone",
        "-tz",
        help="Timezone to use for daily stats (e.g., 'UTC', 'America/New_York'). Defaults to local system time.",
    ),
    since: str | None = typer.Option(
        None,
        "--since",
        help="Include only usage on/after this date (YYYY-MM-DD).",
    ),
    until: str | None = typer.Option(
        None,
        "--until",
        help="Include only usage before this date, exclusive (YYYY-MM-DD).",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Aggregate and print daily token usage and estimated costs from DuckDB."""
    _configure_logging(verbose)
    if not database_path.exists():
        raise typer.BadParameter(f"Database file not found: {database_path}")

    report = _collect_stats_report(
        database_path=database_path,
        timezone=_parse_timezone(timezone),
        since=parse_since_date(since),
        until=parse_until_date(until),
    )
    render_daily_usage_statistics(report, Console())


def _run_ingestion(database_path: Path, data_dir: str | None = None) -> IngestionCounters:
    """Run ingestion workflow and return execution counters.

    Args:
        database_path (Path): Path to output DuckDB database.
        data_dir (str | None, optional): Optional override root or comma-separated roots. Defaults to None.

    Returns:
        IngestionCounters: Ingestion performance and error counters.
    """
    database_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Start ingesting AntiGravity conversation databases.")
    repository = IngestionRepository(database_path)
    try:
        service = IngestionService(repository=repository)
        override_env = {"ANTIGRAVITY_DATA_DIR": data_dir} if data_dir is not None else None
        counters = service.ingest(override_env=override_env)
        LOGGER.info("Finished ingesting AntiGravity conversation databases.")
        return counters
    finally:
        repository.close()


def _configure_logging(verbose: bool) -> None:
    """Initialize default logging for CLI usage.

    Args:
        verbose (bool): Whether to enable INFO-level logging output.
    """
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    )


def _emit_last_7_days_stats(database_path: Path, console: Console) -> None:
    """Render usage statistics from events ingested in the last seven days.

    Args:
        database_path: Path to the DuckDB usage database.
        console: Rich console receiving the output.
    """
    since_date = datetime.now(UTC).astimezone().date() - timedelta(days=6)
    report = _collect_stats_report(database_path=database_path, timezone=None, since=since_date)
    typer.echo("\nStatistics (last 7 days):")
    render_daily_usage_statistics(report, console)


def _collect_stats_report(
    database_path: Path,
    timezone: ZoneInfo | None,
    since: date | None,
    until: date | None = None,
) -> DailyUsageStatistics:
    """Collect a daily usage report with optional date filtering.

    Args:
        database_path: Path to the DuckDB usage database.
        timezone: Timezone used to group events into calendar dates.
        since: Inclusive lower date bound.
        until: Exclusive upper date bound.

    Returns:
        Aggregated daily usage report.

    Raises:
        typer.BadParameter: If the stored usage table cannot be read or model pricing cannot be loaded.
    """
    repository: StatsRepository | None = None
    try:
        repository = StatsRepository(database_path)
        return StatsService(
            repository=repository, timezone=timezone, since=since, until=until
        ).collect_daily_statistics()
    except StatsRepositoryError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except PriceSpecError as exc:
        raise typer.BadParameter(f"Failed to load model pricing: {exc}") from exc
    finally:
        if repository is not None:
            repository.close()


def _parse_timezone(timezone: str | None) -> ZoneInfo | None:
    """Parse a timezone option into a ZoneInfo instance.

    Args:
        timezone: IANA timezone name, or None for the system local timezone.

    Returns:
        Parsed timezone, or None.

    Raises:
        typer.BadParameter: If the timezone name is invalid.
    """
    if timezone is None:
        return None
    try:
        return ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise typer.BadParameter(f"Invalid timezone: {timezone}.") from exc


def _emit_summary(counters: IngestionCounters) -> None:
    """Print ingestion counters to stdout.

    Args:
        counters (IngestionCounters): Ingestion counters to format and display.
    """
    typer.echo("\nSummary:")
    summary_lines = [
        f"databases_discovered={counters.databases_discovered}",
        f"databases_parsed={counters.databases_parsed}",
        f"databases_failed={counters.databases_failed}",
        f"raw_events_extracted={counters.raw_events_extracted}",
        f"canonical_events_written={counters.canonical_events_written}",
        f"duplicate_events_merged={counters.duplicate_events_merged}",
        f"missing_optional_tables={counters.missing_optional_tables}",
    ]
    for line in summary_lines:
        typer.echo(line)

    for failed_database in counters.failed_databases:
        typer.echo(f"failed_database={failed_database}")


def module_cli_entry_point() -> None:
    """Console script entrypoint."""
    TYPER_APP()
