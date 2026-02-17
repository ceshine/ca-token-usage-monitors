"""CLI entrypoints for Gemini token usage tooling."""

from __future__ import annotations

import logging
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import UTC, date, datetime, timedelta

import typer
from rich.console import Console

from .ingestion.errors import ConfirmationDeclinedError, IngestionError
from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters, IngestionSourceRow
from .ingestion.service import IngestionService
from .preprocessing.convert import run_log_conversion
from .preprocessing.metadata import ensure_project_metadata_line
from .preprocessing.resolve_input import resolve_preprocess_input
from .preprocessing.simplify import run_log_simplification
from .stats.repository import StatsRepository, StatsRepositoryError
from .stats.render import render_daily_usage_statistics
from .stats.service import StatsService

LOGGER = logging.getLogger(__name__)
DEFAULT_ARCHIVE_FOLDER = Path("/tmp")
DEFAULT_DATABASE_PATH = Path("data/token_usage.duckdb")

TYPER_APP = typer.Typer(help="Gemini token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


@TYPER_APP.command("preprocess")
def preprocess_command(
    log_file_path: Path,
    enable_archiving: bool = False,
    log_simplify_level: int = 1,
    stats: bool = typer.Option(
        False,
        "--stats",
        help="Print token usage statistics for the processed JSONL file.",
    ),
    timezone: str | None = typer.Option(
        None,
        "--timezone",
        "-tz",
        help="Timezone to use for daily stats (e.g., 'UTC', 'America/New_York'). Defaults to local system time.",
    ),
) -> None:
    """Preprocess Gemini telemetry logs and optionally print token usage statistics."""
    _configure_logging()
    console = Console()

    try:
        processed_jsonl_path = _run_preprocessing(
            log_file_path=log_file_path,
            enable_archiving=enable_archiving,
            log_simplify_level=log_simplify_level,
            console=console,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    if not stats:
        return

    timezone_info = _parse_timezone(timezone)
    report = StatsService().collect_daily_statistics(processed_jsonl_path, timezone=timezone_info)
    render_daily_usage_statistics(report=report, console=console)
    if report.encountered_errors:
        raise typer.Exit(code=1)


@TYPER_APP.command("simplify")
def simplify_command(
    input_file_path: Path = typer.Argument(
        ...,
        help="The path to the input JSONL file.",
        exists=True,
        dir_okay=True,
        readable=True,
    ),
    level: int = typer.Option(
        1,
        "--level",
        "-l",
        help="The simplification level to apply to records (0, 1, 2, or 3).",
        min=0,
        max=3,
    ),
    archive_folder: Path = typer.Option(
        DEFAULT_ARCHIVE_FOLDER,
        "--archive-folder",
        "-a",
        help="Folder where the original file will be archived before simplification.",
    ),
    disable_archiving: bool = typer.Option(
        False,
        "--disable-archiving",
        "-d",
        help="If set, remove the original file instead of archiving it.",
    ),
) -> None:
    """Simplify an existing Gemini telemetry JSONL file."""
    _configure_logging()
    try:
        simplified_path = run_log_simplification(
            input_file_path=input_file_path,
            level=level,
            archive_folder=archive_folder,
            disable_archiving=disable_archiving,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    typer.echo(f"simplified_file={simplified_path}")


@TYPER_APP.command("ingest")
def ingest_command(
    input_paths: list[Path] = typer.Argument(
        None,
        help="Directories or telemetry.jsonl files to ingest.",
    ),
    all_active: bool = typer.Option(
        False,
        "--all-active",
        help="Include all currently active tracked sources from the database.",
    ),
    auto_deactivate: bool = typer.Option(
        False,
        "--auto-deactivate",
        help="With --all-active, mark missing active sources as inactive automatically.",
    ),
    database_path: Path = typer.Option(
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path for ingestion state and usage events.",
    ),
    enable_archiving: bool = typer.Option(
        False,
        "--enable-archiving",
        help="Archive raw telemetry.log files when preprocessing selected input paths.",
    ),
    log_simplify_level: int = typer.Option(
        1,
        "--log-simplify-level",
        min=0,
        max=3,
        help="Simplification level used when preprocessing selected telemetry.log files.",
    ),
) -> None:
    """Ingest Gemini usage events from preprocessed telemetry.jsonl files into DuckDB."""
    _configure_logging()
    console = Console()
    selected_paths = list(input_paths) if input_paths is not None else []
    preprocessed_input_paths = _preprocess_ingest_input_paths(
        input_paths=selected_paths,
        enable_archiving=enable_archiving,
        log_simplify_level=log_simplify_level,
        console=console,
    )
    database_path.parent.mkdir(parents=True, exist_ok=True)

    repository = IngestionRepository(database_path)
    try:
        service = IngestionService(
            repository=repository,
            confirm_new_source=_confirm_new_source_registration,
            confirm_reactivate=_confirm_source_reactivation,
            confirm_project_path_move=_confirm_source_path_update,
        )
        counters = service.ingest(
            input_paths=preprocessed_input_paths,
            include_all_active=all_active,
            auto_deactivate=auto_deactivate,
        )
    except ConfirmationDeclinedError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1) from exc
    except IngestionError as exc:
        raise typer.BadParameter(str(exc)) from exc
    finally:
        repository.close()

    _emit_ingest_summary(counters)
    _emit_last_7_days_stats(database_path=database_path, console=console)


@TYPER_APP.command("stats")
def stats_command(
    database_path: Path = typer.Option(
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
) -> None:
    """Aggregate and print daily token usage and costs from DuckDB."""
    _configure_logging()
    if not database_path.exists():
        raise typer.BadParameter(f"Database file not found: {database_path}")

    timezone_info = _parse_timezone(timezone)
    since_date = _parse_since_date(since)
    report = _collect_stats_report(database_path=database_path, timezone=timezone_info, since_date=since_date)
    render_daily_usage_statistics(report=report, console=Console())


def _run_preprocessing(
    log_file_path: Path,
    enable_archiving: bool,
    log_simplify_level: int,
    console: Console,
) -> Path:
    """Resolve and preprocess input logs, returning the processed JSONL file path."""
    resolved = resolve_preprocess_input(log_file_path)

    if resolved.source_log_file is not None:
        output_file_path = resolved.source_log_file.parent / "telemetry.jsonl"
        processed = run_log_conversion(
            input_file_path=resolved.source_log_file,
            output_file_path=output_file_path,
            simplify_level=log_simplify_level,
            archiving_enabled=enable_archiving,
            archive_folder_path=DEFAULT_ARCHIVE_FOLDER,
        )
        console.print(
            (
                f"Converted {resolved.source_log_file} to {processed} "
                f"with archiving [bold]{'ENABLED' if enable_archiving else 'DISABLED'}[/bold]"
            ),
            style="green",
        )
        return processed

    assert resolved.jsonl_file is not None, "A JSONL file should always be resolved when source_log_file is absent."
    _ = ensure_project_metadata_line(resolved.jsonl_file)
    console.print(f"Using [bold]{resolved.jsonl_file}[/bold] as the JSONL log file", style="green")
    return resolved.jsonl_file


def _configure_logging() -> None:
    """Initialize default logging for CLI usage."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def _preprocess_ingest_input_paths(
    input_paths: list[Path],
    enable_archiving: bool,
    log_simplify_level: int,
    console: Console,
) -> list[Path]:
    """Preprocess selected paths before ingestion and return resolved JSONL paths."""
    processed_paths: list[Path] = []
    for input_path in input_paths:
        try:
            processed_paths.append(
                _run_preprocessing(
                    log_file_path=input_path,
                    enable_archiving=enable_archiving,
                    log_simplify_level=log_simplify_level,
                    console=console,
                )
            )
        except (FileNotFoundError, ValueError, RuntimeError) as exc:
            raise typer.BadParameter(str(exc)) from exc
    return processed_paths


def _emit_last_7_days_stats(database_path: Path, console: Console) -> None:
    """Render 7-day usage statistics from ingested database events."""
    today = datetime.now().date()
    since_date = today - timedelta(days=6)
    report = _collect_stats_report(database_path=database_path, timezone=None, since_date=since_date)
    typer.echo("\nStatistics (last 7 days):")
    render_daily_usage_statistics(report=report, console=console)


def _collect_stats_report(
    database_path: Path,
    timezone: ZoneInfo | None,
    since_date: date | None,
):
    """Collect daily stats report from database events with optional date filtering."""
    repository: StatsRepository | None = None
    try:
        repository = StatsRepository(database_path)
        events = repository.fetch_token_events()
        if since_date is not None:
            events = [event for event in events if _resolve_event_date(event.event_timestamp, timezone) >= since_date]
        return StatsService().collect_daily_statistics_from_events(events=events, timezone=timezone)
    except StatsRepositoryError as exc:
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


def _resolve_event_date(event_timestamp: datetime, timezone: ZoneInfo | None) -> date:
    """Resolve event date in selected timezone (or local system timezone)."""
    normalized = event_timestamp if event_timestamp.tzinfo is not None else event_timestamp.replace(tzinfo=UTC)
    return normalized.astimezone(timezone).date()


def _emit_ingest_summary(counters: IngestionCounters) -> None:
    """Print ingestion counters to stdout."""
    typer.echo("\nSummary:")
    lines = [
        f"sources_scanned={counters.sources_scanned}",
        f"sources_ingested={counters.sources_ingested}",
        f"sources_skipped_unchanged={counters.sources_skipped_unchanged}",
        f"sources_missing={counters.sources_missing}",
        f"sources_auto_deactivated={counters.sources_auto_deactivated}",
        f"usage_events_total={counters.usage_events_total}",
        f"usage_events_skipped_before_checkpoint={counters.usage_events_skipped_before_checkpoint}",
        f"usage_rows_attempted_insert={counters.usage_rows_attempted_insert}",
    ]
    for line in lines:
        typer.echo(line)


def _confirm_new_source_registration(jsonl_file_path: Path, project_id) -> bool:
    """Prompt for new source path registration."""
    return typer.confirm(
        f"Register new source path {jsonl_file_path} for project_id {project_id}?",
        default=False,
    )


def _confirm_source_reactivation(source: IngestionSourceRow) -> bool:
    """Prompt for source reactivation."""
    return typer.confirm(
        f"Source {source.jsonl_file_path} is inactive. Reactivate and ingest it?",
        default=False,
    )


def _confirm_source_path_update(source: IngestionSourceRow, new_path: Path) -> bool:
    """Prompt for in-place source path update for an existing project_id."""
    return typer.confirm(
        (f"Project {source.project_id} is currently tracked at {source.jsonl_file_path}. Update it to {new_path}?"),
        default=False,
    )


def module_cli_entry_point() -> None:
    """Console script entrypoint."""
    TYPER_APP()
