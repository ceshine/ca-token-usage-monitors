"""CLI entrypoints for codex token usage tools."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService

LOGGER = logging.getLogger(__name__)
DEFAULT_SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
DEFAULT_DATABASE_PATH = Path("data/token_usage.duckdb")

TYPER_APP = typer.Typer(help="Codex token usage tooling.")


@TYPER_APP.callback()
def main() -> None:
    """Root CLI callback."""


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
    """Ingest Codex session token usage into DuckDB."""
    _configure_logging(verbose)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    repository = IngestionRepository(database_path)
    try:
        service = IngestionService(repository=repository, sessions_root=sessions_root)
        counters = service.ingest()
    finally:
        repository.close()

    _emit_summary(counters)
    if counters.failed_files:
        raise typer.Exit(code=1)


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


def module_cli_entry_point():
    TYPER_APP()
