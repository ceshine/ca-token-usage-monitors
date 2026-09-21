"""CLI entrypoints for AntiGravity token usage tooling."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .ingestion.repository import IngestionRepository
from ..common.paths import get_default_database_path

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
