"""CLI entrypoints for codex token usage tools."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import typer
from rich.console import Console

from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .stats.render import render_daily_usage_statistics
from .stats.repository import StatsRepository, StatsRepositoryError
from .stats.service import StatsService

LOGGER = logging.getLogger(__name__)
DEFAULT_SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
DEFAULT_DATABASE_PATH = Path("data/token_usage.duckdb")

TYPER_APP = typer.Typer(help="Codex token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


def _run_ingestion(database_path: Path, sessions_root: Path) -> IngestionCounters:
    """Run ingestion and return operation counters."""
    database_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Start ingesting Codex session files.")
    repository = IngestionRepository(database_path)
    try:
        service = IngestionService(repository=repository, sessions_root=sessions_root)
        ingest_stats = service.ingest()
        LOGGER.info("Finished ingesting Codex session files.")
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
    sessions_root: Path = typer.Option(
        DEFAULT_SESSIONS_ROOT,
        "--sessions-root",
        "-s",
        help="Root directory containing Codex JSONL session files.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable info-level logging."),
) -> None:
    """Ingest Codex session token usage into DuckDB.

    Note: this command is mostly for internal debugging purposes.
    """
    _configure_logging(verbose)
    counters = _run_ingestion(database_path=database_path, sessions_root=sessions_root)
    _emit_summary(counters)
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
    ingest: bool = typer.Option(
        False,
        "--ingest/--no-ingest",
        help="Ingest session logs into DuckDB before computing statistics.",
    ),
    sessions_root: Path = typer.Option(
        DEFAULT_SESSIONS_ROOT,
        "--sessions-root",
        "-s",
        help="Root directory containing Codex JSONL session files (used with --ingest).",
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
    if ingest:
        counters = _run_ingestion(database_path=database_path, sessions_root=sessions_root)
        if verbose:
            _emit_summary(counters)
        if counters.failed_files:
            raise typer.Exit(code=1)
    elif not database_path.exists():
        raise typer.BadParameter(f"Database file not found: {database_path}")

    resolved_timezone = _parse_timezone(timezone)
    since_date = _parse_since_date(since)
    repository: StatsRepository | None = None
    try:
        repository = StatsRepository(database_path)
        service = StatsService(repository=repository, timezone=resolved_timezone, since=since_date)
        report = service.collect_daily_statistics()
    except (StatsRepositoryError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        if repository is not None:
            repository.close()

    render_daily_usage_statistics(report, Console())


def _configure_logging(verbose: bool) -> None:
    """Initialize default logging for CLI usage."""
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    )


def _emit_summary(counters: IngestionCounters) -> None:
    """Print ingestion counters to stdout."""
    summary_lines = [
        f"files_scanned={counters.files_scanned}",
        f"files_ingested={counters.files_ingested}",
        f"files_skipped_unchanged={counters.files_skipped_unchanged}",
        f"sessions_ingested={counters.sessions_ingested}",
        f"token_rows_raw={counters.token_rows_raw}",
        f"token_rows_deduped={counters.token_rows_deduped}",
        f"token_rows_skipped_info_null={counters.token_rows_skipped_info_null}",
        f"token_rows_skipped_before_checkpoint={counters.token_rows_skipped_before_checkpoint}",
        f"duplicate_rows_skipped={counters.duplicate_rows_skipped}",
        f"monotonicity_errors={counters.monotonicity_errors}",
        f"delta_consistency_errors={counters.delta_consistency_errors}",
        f"parse_errors={counters.parse_errors}",
    ]
    for line in summary_lines:
        typer.echo(line)

    for failed_file in counters.failed_files:
        typer.echo(f"failed_file={failed_file}")


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


def module_cli_entry_point():
    TYPER_APP()
