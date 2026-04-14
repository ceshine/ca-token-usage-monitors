"""CLI entrypoints for Pi agent token usage tools."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from coding_agent_usage_monitors.common.paths import get_default_database_path

from .ingestion.parser import discover_session_root
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .ingestion.repository import IngestionRepository

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
    if counters.failed_files:
        raise typer.Exit(code=1)


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


def module_cli_entry_point() -> None:
    """Entry point for the CLI."""
    TYPER_APP()
