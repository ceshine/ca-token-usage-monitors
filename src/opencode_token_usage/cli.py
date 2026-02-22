"""CLI entrypoints for OpenCode token usage tooling."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from .ingestion.errors import IngestionError
from .ingestion.repository import IngestionRepository
from .ingestion.schemas import IngestionCounters
from .ingestion.service import IngestionService
from .ingestion.source_reader import SourceReader

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


def module_cli_entry_point() -> None:
    TYPER_APP()
