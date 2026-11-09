"""One-time migration script to backfill model_id and re-normalize model_code in DuckDB."""

from __future__ import annotations

import re
import logging
from pathlib import Path
from collections import Counter
from dataclasses import dataclass

import typer
import duckdb
from rich.table import Table
from rich.console import Console

from coding_agent_usage_monitors.common.paths import get_default_database_path
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.parser import (
    BASE_ALIASES,
    NUMERIC_MODEL_MAP,
    resolve_model_code,
    normalize_antigravity_model,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_DATABASE_PATH = get_default_database_path()

PLACEHOLDER_M_REGEX: re.Pattern[str] = re.compile(r"^model_placeholder_m(\d+)$", re.IGNORECASE)
LEGACY_MODEL_REGEX: re.Pattern[str] = re.compile(r"^antigravity-model-(\d+)$", re.IGNORECASE)

TYPER_APP = typer.Typer(help="Backfill raw model_id and re-normalize model_code for AntiGravity usage events.")


@dataclass(frozen=True)
class BackfillStats:
    """Summary statistics from running the model_id backfill and model_code re-normalization."""

    total_rows: int
    model_id_backfilled: int
    model_code_updated: int
    dry_run: bool
    breakdown: list[tuple[str, int | None, int]]


def build_primary_model_id_by_code() -> dict[str, int]:
    """Build reverse mapping from canonical model_code to primary (lowest) numeric model_id.

    Returns:
        dict[str, int]: Mapping from normalized model_code to primary numeric model_id.
    """
    primary_map: dict[str, int] = {}

    for model_id, model_code in sorted(NUMERIC_MODEL_MAP.items()):
        normalized = normalize_antigravity_model(model_code) or model_code
        primary_map.setdefault(normalized, model_id)

    placeholder_entries: list[tuple[int, str]] = []
    for raw_alias, canonical_code in BASE_ALIASES.items():
        match = PLACEHOLDER_M_REGEX.match(raw_alias.strip())
        if match:
            numeric_id = 1000 + int(match.group(1))
            normalized = normalize_antigravity_model(canonical_code) or canonical_code
            placeholder_entries.append((numeric_id, normalized))

    for numeric_id, normalized in sorted(placeholder_entries):
        primary_map.setdefault(normalized, numeric_id)

    return primary_map


PRIMARY_MODEL_ID_BY_CODE: dict[str, int] = build_primary_model_id_by_code()


def infer_model_id_from_code(
    model_code: str,
    primary_map: dict[str, int] | None = None,
) -> int | None:
    """Infer raw numeric model_id from a stored model_code string.

    Args:
        model_code (str): Stored model_code value in DuckDB.
        primary_map (dict[str, int] | None, optional): Reverse mapping override. Defaults to None.

    Returns:
        int | None: Inferred numeric model_id, or None if no numeric ID corresponds to the code.
    """
    trimmed = model_code.strip()
    if not trimmed:
        return None

    m_match = PLACEHOLDER_M_REGEX.match(trimmed)
    if m_match:
        return 1000 + int(m_match.group(1))

    legacy_match = LEGACY_MODEL_REGEX.match(trimmed)
    if legacy_match:
        return int(legacy_match.group(1))

    lookup = primary_map if primary_map is not None else PRIMARY_MODEL_ID_BY_CODE
    if trimmed in lookup:
        return lookup[trimmed]

    normalized = normalize_antigravity_model(trimmed)
    if normalized and normalized in lookup:
        return lookup[normalized]

    return None


def backfill_antigravity_model_ids(
    connection: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
) -> BackfillStats:
    """Backfill model_id and re-normalize model_code in antigravity_usage_events.

    Args:
        connection (duckdb.DuckDBPyConnection): Active DuckDB connection.
        dry_run (bool, optional): If True, roll back changes after computing stats. Defaults to False.

    Returns:
        BackfillStats: Summary statistics and (model_code, model_id) breakdown.
    """
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute("ALTER TABLE antigravity_usage_events ADD COLUMN IF NOT EXISTS model_id BIGINT;")

        rows: list[tuple[str, str, int | None]] = connection.execute(
            """
SELECT event_key, model_code, model_id
FROM antigravity_usage_events
ORDER BY event_key
            """
        ).fetchall()

        model_id_backfilled = 0
        model_code_updated = 0
        updates: list[list[object]] = []
        breakdown_counter: Counter[tuple[str, int | None]] = Counter()

        for event_key, stored_model_code, stored_model_id in rows:
            new_model_id = (
                stored_model_id if stored_model_id is not None else infer_model_id_from_code(stored_model_code)
            )
            new_model_code = resolve_model_code(
                usage_model_id=new_model_id,
                raw_model_name=stored_model_code,
                meta_model_id=None,
                inherited_model=None,
            )

            id_changed = stored_model_id is None and new_model_id is not None
            code_changed = new_model_code != stored_model_code

            if id_changed:
                model_id_backfilled += 1
            if code_changed:
                model_code_updated += 1
            if id_changed or code_changed:
                updates.append([new_model_id, new_model_code, event_key])

            breakdown_counter[(new_model_code, new_model_id)] += 1

        if updates:
            connection.executemany(
                """
UPDATE antigravity_usage_events
SET model_id = ?, model_code = ?
WHERE event_key = ?
                """,
                updates,
            )

        if dry_run:
            connection.execute("ROLLBACK")
        else:
            connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    sorted_breakdown = sorted(
        ((code, mid, count) for (code, mid), count in breakdown_counter.items()),
        key=lambda item: (item[0], item[1] if item[1] is not None else -1),
    )

    return BackfillStats(
        total_rows=len(rows),
        model_id_backfilled=model_id_backfilled,
        model_code_updated=model_code_updated,
        dry_run=dry_run,
        breakdown=sorted_breakdown,
    )


def render_backfill_summary(stats: BackfillStats, console: Console) -> None:
    """Render Rich summary and breakdown tables for the backfill run.

    Args:
        stats (BackfillStats): Computed backfill statistics.
        console (Console): Rich console instance for output.
    """
    mode_label = "DRY RUN (rolled back)" if stats.dry_run else "COMMITTED"
    summary_table = Table(title=f"AntiGravity Model ID Backfill Summary [{mode_label}]")
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Count", justify="right", style="magenta")

    summary_table.add_row("Total Rows", str(stats.total_rows))
    summary_table.add_row("Rows with model_id Backfilled", str(stats.model_id_backfilled))
    summary_table.add_row("Rows with model_code Updated", str(stats.model_code_updated))
    console.print(summary_table)

    breakdown_table = Table(title="Breakdown by (model_code, model_id)")
    breakdown_table.add_column("model_code", style="green")
    breakdown_table.add_column("model_id", justify="right", style="yellow")
    breakdown_table.add_column("Rows", justify="right", style="cyan")

    for model_code, model_id, count in stats.breakdown:
        breakdown_table.add_row(
            model_code,
            str(model_id) if model_id is not None else "NULL",
            str(count),
        )
    console.print(breakdown_table)


@TYPER_APP.command()
def main(
    database_path: Path = typer.Option(
        DEFAULT_DATABASE_PATH,
        "--database-path",
        "-d",
        help="DuckDB file path containing antigravity_usage_events.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Compute and display changes without committing them to DuckDB.",
    ),
) -> None:
    """Backfill raw model_id and re-normalize placeholder model_code values in DuckDB."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    )
    if not database_path.exists():
        raise typer.BadParameter(f"Database file not found: {database_path}")

    LOGGER.info("Opening DuckDB database at %s (dry_run=%s)", database_path, dry_run)
    connection = duckdb.connect(str(database_path))
    try:
        stats = backfill_antigravity_model_ids(connection=connection, dry_run=dry_run)
    finally:
        connection.close()

    render_backfill_summary(stats=stats, console=Console())


if __name__ == "__main__":
    TYPER_APP()
