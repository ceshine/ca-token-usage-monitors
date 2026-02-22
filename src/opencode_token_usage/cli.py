"""CLI entrypoints for OpenCode token usage tooling."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import typer
from rich.console import Console

from .ingestion.errors import IngestionError
from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .ingestion.source_reader import SourceReader
from .stats.render import render_daily_usage_statistics
from .stats.repository import StatsRepository, StatsRepositoryError
from .stats.service import StatsService

LOGGER = logging.getLogger(__name__)
DEFAULT_SOURCE_DB = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
DEFAULT_DATABASE_PATH = Path("data/token_usage.duckdb")

TYPER_APP = typer.Typer(help="OpenCode token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


@TYPER_APP.command("ingest")
def ingest_command(
    source_db: Path = typer.Option(
        DEFAULT_SOURCE_DB,
        "--source-db",
        "-s",
        help="OpenCode SQLite database path.",
    ),
    database_path: Path = typer.Option(
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and usage events.",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Ignore checkpoint and re-upsert all assistant rows.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Ingest OpenCode assistant message usage from SQLite into DuckDB."""
    _configure_logging(verbose)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    repository = IngestionRepository(database_path)
    source_reader = SourceReader(source_db)
    try:
        service = IngestionService(
            repository=repository,
            source_reader=source_reader,
        )
        counters = service.ingest(full_refresh=full_refresh)
    except IngestionError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        source_reader.close()
        repository.close()

    _emit_summary(counters)
    _emit_last_7_days_stats(database_path=database_path, console=Console())


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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Aggregate and print daily token usage and costs from DuckDB."""
    _configure_logging(verbose)
    if not database_path.exists():
        raise typer.BadParameter(f"Database file not found: {database_path}")

    resolved_timezone = _parse_timezone(timezone)
    since_date = _parse_since_date(since)
    report = _collect_stats_report(database_path=database_path, timezone=resolved_timezone, since=since_date)

    render_daily_usage_statistics(report, Console())


def _configure_logging(verbose: bool) -> None:
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    )


def _emit_summary(counters: IngestionCounters) -> None:
    typer.echo("\nSummary:")
    typer.echo(f"messages_scanned={counters.messages_scanned}")
    typer.echo(f"messages_ingested={counters.messages_ingested}")
    typer.echo(f"sessions_upserted={counters.sessions_upserted}")
    typer.echo(f"batches_flushed={counters.batches_flushed}")
    typer.echo(f"skipped_no_source_changes={int(counters.skipped_no_source_changes)}")


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
):
    """Collect daily stats report from database events with optional date filtering."""
    repository: StatsRepository | None = None
    try:
        repository = StatsRepository(database_path)
        service = StatsService(repository=repository, timezone=timezone, since=since)
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


def _parse_since_date(since: str | None) -> date | None:
    """Parse `--since` value into a date."""
    if since is None:
        return None
    try:
        return date.fromisoformat(since)
    except ValueError as exc:
        raise typer.BadParameter(f"Invalid --since value: {since}. Expected YYYY-MM-DD.") from exc


def module_cli_entry_point() -> None:
    TYPER_APP()
