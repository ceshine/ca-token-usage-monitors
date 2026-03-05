#!/usr/bin/env python3
"""Compact a DuckDB database by copying it into a fresh database file.

This follows the DuckDB-recommended workflow:

1. ATTACH source database.
2. ATTACH destination database.
3. COPY FROM DATABASE source TO destination.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Annotated

import typer
import duckdb


APP = typer.Typer(add_completion=False, help=__doc__)


def _format_size(size_bytes: int) -> str:
    """Return a human-readable file size string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size_bytes)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def _sql_quote_path(path: Path) -> str:
    """Return SQL-safe single-quoted path literal for DuckDB SQL text."""
    return str(path).replace("'", "''")


def _make_temp_path(original_path: Path) -> Path:
    """Generate a non-existent sibling path for compacted output."""
    candidate = original_path.with_name(f"{original_path.name}.compacted")
    counter = 1
    while candidate.exists():
        candidate = original_path.with_name(f"{original_path.name}.compacted.{counter}")
        counter += 1
    return candidate


def _compact_database(source_path: Path, target_path: Path) -> None:
    """Copy a DuckDB database into a fresh file using DuckDB SQL commands."""
    connection = duckdb.connect(":memory:")
    try:
        source_sql = _sql_quote_path(source_path)
        target_sql = _sql_quote_path(target_path)
        connection.execute(f"ATTACH '{source_sql}' AS source_db")
        connection.execute(f"ATTACH '{target_sql}' AS target_db")
        connection.execute("COPY FROM DATABASE source_db TO target_db")
    finally:
        connection.close()


@APP.command()
def main(
    database_path: Annotated[
        Path,
        typer.Argument(help="Path to the DuckDB database file to compact."),
    ],
) -> None:
    """Compact a DuckDB database and replace the original file via trash."""
    source_path = database_path.expanduser().resolve()
    if not source_path.exists() or not source_path.is_file():
        raise typer.BadParameter(f"Database file not found: {source_path}")

    before_size = source_path.stat().st_size
    temp_path = _make_temp_path(source_path)

    try:
        _compact_database(source_path, temp_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise

    after_size = temp_path.stat().st_size

    try:
        # Trash the original file
        _ = subprocess.run(["trash-put", str(source_path)], check=True)
    except FileNotFoundError as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError("`trash-put` command not found. Please install trash-cli.") from exc
    except subprocess.CalledProcessError as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to move original database to trash: {source_path}") from exc

    _ = temp_path.rename(source_path)

    typer.echo(f"Before compaction: {_format_size(before_size)} ({before_size} bytes)")
    typer.echo(f"After compaction:  {_format_size(after_size)} ({after_size} bytes)")


if __name__ == "__main__":
    APP()
