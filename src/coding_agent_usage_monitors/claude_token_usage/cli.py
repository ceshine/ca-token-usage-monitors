"""CLI entrypoints for Claude Code token usage tools."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import typer
from rich.console import Console

from coding_agent_usage_monitors.common.cli_utils import parse_since_date, parse_until_date
from coding_agent_usage_monitors.common.paths import get_default_database_path

from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .stats.render import render_daily_usage_statistics
from .stats.repository import StatsRepository, StatsRepositoryError
from .stats.service import StatsService

LOGGER = logging.getLogger(__name__)
DEFAULT_DATABASE_PATH = get_default_database_path()

TYPER_APP = typer.Typer(help="Claude Code token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


def _run_ingestion(database_path: Path, session_roots: list[Path] | None) -> IngestionCounters:
    """Run ingestion and return operation counters.

    Args:
        database_path: Path to DuckDB database.
        session_roots: Optional list of session root directories.

    Returns:
        IngestionCounters from the ingestion run.
    """
    database_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Start ingesting Claude Code session files.")
    repository = IngestionRepository(database_path)
    try:
        service = IngestionService(repository=repository, session_roots=session_roots)
        ingest_stats = service.ingest()
        LOGGER.info("Finished ingesting Claude Code session files.")
        return ingest_stats
    finally:
        repository.close()


@TYPER_APP.command("ingest")
def ingest_command(
    database_path: Path = typer.Option(
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and token details.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Ingest Claude Code session token usage into DuckDB."""
    _configure_logging(verbose)
    counters = _run_ingestion(database_path=database_path, session_roots=None)
    _emit_summary(counters)
    _emit_last_7_days_stats(database_path=database_path, console=Console())
    if counters.failed_files:
        raise typer.Exit(code=1)


def _configure_logging(verbose: bool) -> None:
    """Initialize default logging for CLI usage.

    Args:
        verbose: When True, set log level to INFO; otherwise WARNING.
    """
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    )


def _emit_summary(counters: IngestionCounters) -> None:
    """Print ingestion counters to stdout.

    Args:
        counters: Ingestion counters to display.
    """
    typer.echo("\nSummary:")
    summary_lines = [
        f"files_scanned={counters.files_scanned}",
        f"files_ingested={counters.files_ingested}",
        f"files_skipped_unchanged={counters.files_skipped_unchanged}",
        f"sessions_ingested={counters.sessions_ingested}",
        f"usage_rows_raw={counters.usage_rows_raw}",
        f"usage_rows_deduped={counters.usage_rows_deduped}",
        f"usage_rows_skipped_synthetic={counters.usage_rows_skipped_synthetic}",
        f"usage_rows_skipped_before_checkpoint={counters.usage_rows_skipped_before_checkpoint}",
        f"duplicate_rows_skipped={counters.duplicate_rows_skipped}",
        f"parse_errors={counters.parse_errors}",
    ]
    for line in summary_lines:
        typer.echo(line)

    for failed_file in counters.failed_files:
        typer.echo(f"failed_file={failed_file}")


@TYPER_APP.command("stats")
def stats_command(
    database_path: Path = typer.Option(
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and token details.",
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
    """Aggregate and print daily token usage and costs from DuckDB."""
    _configure_logging(verbose)
    if not database_path.exists():
        raise typer.BadParameter(f"Database file not found: {database_path}")

    resolved_timezone = _parse_timezone(timezone)
    since_date = parse_since_date(since)
    until_date = parse_until_date(until)
    report = _collect_stats_report(
        database_path=database_path, timezone=resolved_timezone, since=since_date, until=until_date
    )

    render_daily_usage_statistics(report, Console())


def _emit_last_7_days_stats(database_path: Path, console: Console) -> None:
    """Render usage statistics from ingested events over the last seven days."""
    today = datetime.now().date()
    since_date = today - timedelta(days=6)
    report = _collect_stats_report(database_path=database_path, timezone=None, since=since_date)
    typer.echo("\nStatistics (last 7 days):")
    render_daily_usage_statistics(report=report, console=console)


def _collect_stats_report(
    database_path: Path,
    timezone: ZoneInfo | None,
    since: date | None,
    until: date | None = None,
):
    """Collect daily stats report from database events with optional date filtering."""
    repository: StatsRepository | None = None
    try:
        repository = StatsRepository(database_path)
        service = StatsService(repository=repository, timezone=timezone, since=since, until=until)
        return service.collect_daily_statistics()
    except (StatsRepositoryError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        if repository is not None:
            repository.close()


def _parse_timezone(timezone: str | None) -> ZoneInfo | None:
    """Parse timezone option into a ZoneInfo instance."""
    if timezone is None:
        return None
    try:
        return ZoneInfo(timezone)
    except Exception as exc:
        raise typer.BadParameter(f"Invalid timezone: {timezone}.") from exc


def module_cli_entry_point():
    """Entry point for the CLI."""
    TYPER_APP()
