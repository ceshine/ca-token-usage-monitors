"""CLI entrypoints for Pi agent token usage tools."""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import typer
from rich.console import Console

from .stats.render import render_daily_usage_statistics
from .stats.schemas import DailyUsageStatistics
from .stats.service import StatsService
from .stats.repository import StatsRepository, StatsRepositoryError
from .ingestion.parser import discover_session_root
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .ingestion.repository import IngestionRepository
from ..common.paths import get_default_database_path
from ..common.cli_utils import parse_since_date, parse_until_date

LOGGER = logging.getLogger(__name__)
DEFAULT_DATABASE_PATH = get_default_database_path()

TYPER_APP = typer.Typer(help="Pi agent token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


@TYPER_APP.command("ingest")
def ingest_command(
    source_dir: Path | None = typer.Option(
        None,
        "--source-dir",
        "-s",
        help="Pi agent sessions root (default: ~/.pi/agent/sessions).",
    ),
    database_path: Path = typer.Option(
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and token details.",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Bypass file-state cache and per-session checkpoint; re-upsert every row.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Ingest Pi agent session token usage into DuckDB."""
    _configure_logging(verbose)
    counters = _run_ingestion(
        database_path=database_path,
        session_root=discover_session_root(source_dir),
        full_refresh=full_refresh,
    )
    _emit_summary(counters)
    _emit_last_7_days_stats(database_path=database_path, console=Console())
    if counters.failed_files:
        raise typer.Exit(code=1)


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
        database_path=database_path,
        timezone=resolved_timezone,
        since=since_date,
        until=until_date,
    )
    render_daily_usage_statistics(report, Console())


def _run_ingestion(
    database_path: Path,
    session_root: Path,
    full_refresh: bool,
) -> IngestionCounters:
    """Run ingestion and return operation counters."""
    database_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Start ingesting Pi agent session files.")
    repository = IngestionRepository(database_path)
    try:
        service = IngestionService(repository=repository, session_root=session_root)
        counters = service.ingest(full_refresh=full_refresh)
        LOGGER.info("Finished ingesting Pi agent session files.")
        return counters
    finally:
        repository.close()


def _configure_logging(verbose: bool) -> None:
    """Initialize default logging for CLI usage."""
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    )


def _emit_summary(counters: IngestionCounters) -> None:
    """Print ingestion counters to stdout."""
    typer.echo("\nSummary:")
    summary_lines = [
        f"files_scanned={counters.files_scanned}",
        f"files_ingested={counters.files_ingested}",
        f"files_skipped_unchanged={counters.files_skipped_unchanged}",
        f"sessions_ingested={counters.sessions_ingested}",
        f"usage_rows_raw={counters.usage_rows_raw}",
        f"usage_rows_persisted={counters.usage_rows_persisted}",
        f"usage_rows_skipped_before_checkpoint={counters.usage_rows_skipped_before_checkpoint}",
        f"sessions_cwd_recovered_from_path={counters.sessions_cwd_recovered_from_path}",
        f"parse_errors={counters.parse_errors}",
    ]
    for line in summary_lines:
        typer.echo(line)

    for failed_file in counters.failed_files:
        typer.echo(f"failed_file={failed_file}")


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
) -> DailyUsageStatistics:
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


def module_cli_entry_point() -> None:
    """Entry point for the CLI."""
    TYPER_APP()
