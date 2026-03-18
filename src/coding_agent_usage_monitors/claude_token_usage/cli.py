"""CLI entrypoints for Claude Code token usage tools."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from coding_agent_usage_monitors.common.paths import get_default_database_path
from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService

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


def module_cli_entry_point():
    """Entry point for the CLI."""
    TYPER_APP()
