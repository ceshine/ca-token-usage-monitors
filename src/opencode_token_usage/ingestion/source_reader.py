"""SQLite source reader for OpenCode assistant message usage."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .errors import SourceDatabaseError, SourceSchemaError
from .schemas import SourceCheckpoint, SourceMessageRow

REQUIRED_TABLES: tuple[str, ...] = ("message", "session", "project")


class SourceReader:
    """Read assistant messages from OpenCode SQLite storage."""

    def __init__(self, source_db_path: Path) -> None:
        self._source_db_path = source_db_path
        self._connection = _connect_read_only(source_db_path)

    def close(self) -> None:
        """Close SQLite connection."""
        self._connection.close()

    def ensure_schema(self) -> None:
        """Validate required source tables exist."""
        rows = self._connection.execute(
            """
SELECT name
FROM sqlite_master
WHERE type = 'table'
  AND name IN (?, ?, ?)
            """,
            list(REQUIRED_TABLES),
        ).fetchall()
        existing = {str(row[0]) for row in rows}
        missing = [name for name in REQUIRED_TABLES if name not in existing]
        if missing:
            missing_csv = ", ".join(missing)
            raise SourceSchemaError(f"Missing required table(s) in source database: {missing_csv}")

    def get_latest_assistant_time_updated_ms(self) -> int | None:
        """Return latest assistant message `time_updated` value."""
        row = self._connection.execute(
            """
SELECT MAX(m.time_updated)
FROM message m
WHERE json_extract(m.data, '$.role') = 'assistant'
            """
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def iter_assistant_rows(self, checkpoint: SourceCheckpoint | None) -> Iterator[SourceMessageRow]:
        """Yield assistant rows ordered by `(time_updated, id)` and filtered by checkpoint."""
        params: dict[str, Any] = {
            "last_time": checkpoint.last_time_updated_ms if checkpoint else None,
            "last_id": checkpoint.last_message_id if checkpoint else None,
        }

        cursor = self._connection.execute(
            """
SELECT
    m.id AS message_id,
    m.session_id,
    m.time_created,
    m.time_updated,
    m.data,
    s.project_id,
    s.title,
    s.directory,
    s.version,
    p.worktree
FROM message m
JOIN session s ON s.id = m.session_id
JOIN project p ON p.id = s.project_id
WHERE json_extract(m.data, '$.role') = 'assistant'
  AND (
    :last_time IS NULL
    OR m.time_updated > :last_time
    OR (m.time_updated = :last_time AND m.id > :last_id)
  )
ORDER BY m.time_updated ASC, m.id ASC
            """,
            params,
        )

        for row in cursor:
            yield _row_to_source_message(row)


def _connect_read_only(source_db_path: Path) -> sqlite3.Connection:
    if not source_db_path.exists():
        raise SourceDatabaseError(f"Source database not found: {source_db_path}")

    uri = f"file:{source_db_path.expanduser()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise SourceDatabaseError(f"Failed to open source database {source_db_path}: {exc}") from exc

    connection.row_factory = sqlite3.Row
    return connection


def _row_to_source_message(row: sqlite3.Row | tuple[Any, ...]) -> SourceMessageRow:
    return SourceMessageRow(
        message_id=str(row[0]),
        session_id=str(row[1]),
        time_created_ms=int(row[2]),
        time_updated_ms=int(row[3]),
        data_json=str(row[4]),
        project_id=str(row[5]),
        session_title=str(row[6]),
        session_directory=str(row[7]),
        session_version=str(row[8]),
        project_worktree=str(row[9]) if row[9] is not None else None,
    )
