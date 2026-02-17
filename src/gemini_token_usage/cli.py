"""CLI entrypoints for Gemini token usage tooling."""

from __future__ import annotations

import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import typer
from rich.console import Console

from .preprocessing.convert import run_log_conversion
from .preprocessing.resolve_input import resolve_preprocess_input
from .preprocessing.simplify import run_log_simplification
from .stats.render import render_daily_usage_statistics
from .stats.service import StatsService

LOGGER = logging.getLogger(__name__)
DEFAULT_ARCHIVE_FOLDER = Path("/tmp")

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
    console.print(f"Using [bold]{resolved.jsonl_file}[/bold] as the JSONL log file", style="green")
    return resolved.jsonl_file


def _configure_logging() -> None:
    """Initialize default logging for CLI usage."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def _parse_timezone(timezone: str | None) -> ZoneInfo | None:
    """Parse timezone option into a ZoneInfo instance."""
    if timezone is None:
        return None
    try:
        return ZoneInfo(timezone)
    except Exception as exc:
        raise typer.BadParameter(f"Invalid timezone: {timezone}.") from exc


def module_cli_entry_point() -> None:
    """Console script entrypoint."""
    TYPER_APP()
