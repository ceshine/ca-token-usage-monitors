"""DuckDB repository for Gemini ingestion persistence."""

from __future__ import annotations

from uuid import UUID
from pathlib import Path
from datetime import UTC, datetime
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

import duckdb

from .schemas import IngestionSourceRow, JsonlFileState, SourceCheckpoint, UsageEventRow


class IngestionRepository:
    """DuckDB-backed repository for Gemini ingestion state and usage events."""

    def __init__(self, database_path: Path) -> None:
        self._connection = duckdb.connect(str(database_path))

    def close(self) -> None:
        """Close DuckDB connection."""
        self._connection.close()

    def ensure_schema(self) -> None:
        """Create ingestion tables when missing."""
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS gemini_ingestion_sources (
    project_id UUID PRIMARY KEY,
    jsonl_file_path VARCHAR NOT NULL UNIQUE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    file_size_bytes BIGINT,
    file_mtime TIMESTAMPTZ,
    last_ingested_event_timestamp TIMESTAMPTZ,
    last_ingested_model_code VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
            """
        )
        _ = self._connection.execute(
            """
CREATE TABLE IF NOT EXISTS gemini_usage_events (
    project_id UUID NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    model_code VARCHAR NOT NULL,
    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    thoughts_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, event_timestamp, model_code)
)
            """
        )

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Open a DB transaction scope."""
        _ = self._connection.execute("BEGIN TRANSACTION")
        try:
            yield
        except Exception:
            _ = self._connection.execute("ROLLBACK")
            raise
        else:
            _ = self._connection.execute("COMMIT")

    def get_source_by_path(self, jsonl_file_path: str) -> IngestionSourceRow | None:
        """Fetch one tracked source by canonical JSONL file path."""
        row = self._connection.execute(
            """
SELECT
    project_id::VARCHAR,
    jsonl_file_path,
    active,
    file_size_bytes,
    CAST(file_mtime AS VARCHAR),
    CAST(last_ingested_event_timestamp AS VARCHAR),
    last_ingested_model_code
FROM gemini_ingestion_sources
WHERE jsonl_file_path = ?
            """,
            [jsonl_file_path],
        ).fetchone()
        return _row_to_source(row)

    def get_source_by_project_id(self, project_id: UUID) -> IngestionSourceRow | None:
        """Fetch one tracked source by project id."""
        row = self._connection.execute(
            """
SELECT
    project_id::VARCHAR,
    jsonl_file_path,
    active,
    file_size_bytes,
    CAST(file_mtime AS VARCHAR),
    CAST(last_ingested_event_timestamp AS VARCHAR),
    last_ingested_model_code
FROM gemini_ingestion_sources
WHERE project_id = ?
            """,
            [str(project_id)],
        ).fetchone()
        return _row_to_source(row)

    def list_active_sources(self) -> list[IngestionSourceRow]:
        """List all active ingestion source rows."""
        rows = self._connection.execute(
            """
SELECT
    project_id::VARCHAR,
    jsonl_file_path,
    active,
    file_size_bytes,
    CAST(file_mtime AS VARCHAR),
    CAST(last_ingested_event_timestamp AS VARCHAR),
    last_ingested_model_code
FROM gemini_ingestion_sources
WHERE active = TRUE
ORDER BY jsonl_file_path
            """
        ).fetchall()
        sources: list[IngestionSourceRow] = []
        for row in rows:
            source = _row_to_source(row)
            if source is not None:
                sources.append(source)
        return sources

    def detect_active_project_collisions(self) -> list[UUID]:
        """Return project IDs that have more than one active source row."""
        rows = self._connection.execute(
            """
SELECT project_id::VARCHAR
FROM gemini_ingestion_sources
WHERE active = TRUE
GROUP BY project_id
HAVING COUNT(*) > 1
            """
        ).fetchall()
        return [UUID(row[0]) for row in rows]

    def insert_source(self, project_id: UUID, jsonl_file_path: str, active: bool = True) -> None:
        """Insert a new tracked source row."""
        _ = self._connection.execute(
            """
INSERT INTO gemini_ingestion_sources (project_id, jsonl_file_path, active)
VALUES (?, ?, ?)
            """,
            [str(project_id), jsonl_file_path, active],
        )

    def set_source_active(self, project_id: UUID, active: bool) -> None:
        """Set source active flag."""
        _ = self._connection.execute(
            """
UPDATE gemini_ingestion_sources
SET active = ?, updated_at = NOW()
WHERE project_id = ?
            """,
            [active, str(project_id)],
        )

    def update_source_path(self, project_id: UUID, jsonl_file_path: str) -> None:
        """Update source path in-place for an existing project ID row."""
        _ = self._connection.execute(
            """
UPDATE gemini_ingestion_sources
SET jsonl_file_path = ?, updated_at = NOW()
WHERE project_id = ?
            """,
            [jsonl_file_path, str(project_id)],
        )

    def insert_usage_events(self, rows: list[UsageEventRow]) -> None:
        """Insert usage event rows using conflict-ignore semantics."""
        if not rows:
            return
        _ = self._connection.executemany(
            """
INSERT INTO gemini_usage_events (
    project_id,
    event_timestamp,
    model_code,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    thoughts_tokens,
    total_tokens
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (project_id, event_timestamp, model_code) DO NOTHING
            """,
            [
                [
                    str(row.project_id),
                    row.event_timestamp,
                    row.model_code,
                    row.input_tokens,
                    row.cached_input_tokens,
                    row.output_tokens,
                    row.thoughts_tokens,
                    row.total_tokens,
                ]
                for row in rows
            ],
        )

    def update_source_bookkeeping(
        self,
        project_id: UUID,
        file_state: JsonlFileState,
        checkpoint: SourceCheckpoint | None,
    ) -> None:
        """Update source file metadata and checkpoint tuple."""
        _ = self._connection.execute(
            """
UPDATE gemini_ingestion_sources
SET
    file_size_bytes = ?,
    file_mtime = ?,
    last_ingested_event_timestamp = ?,
    last_ingested_model_code = ?,
    updated_at = NOW()
WHERE project_id = ?
            """,
            [
                file_state.file_size_bytes,
                file_state.file_mtime,
                checkpoint.last_event_timestamp if checkpoint is not None else None,
                checkpoint.last_model_code if checkpoint is not None else None,
                str(project_id),
            ],
        )


def _row_to_source(row: tuple[Any, ...] | None) -> IngestionSourceRow | None:
    if row is None:
        return None

    checkpoint: SourceCheckpoint | None = None
    checkpoint_ts = _parse_db_timestamp(row[5])
    checkpoint_model = row[6]
    if checkpoint_ts is not None or checkpoint_model is not None:
        if checkpoint_ts is None or not isinstance(checkpoint_model, str) or not checkpoint_model:
            raise ValueError(
                "Invalid checkpoint tuple in gemini_ingestion_sources: both timestamp and model are required."
            )
        checkpoint = SourceCheckpoint(last_event_timestamp=checkpoint_ts, last_model_code=checkpoint_model)

    return IngestionSourceRow(
        project_id=UUID(row[0]),
        jsonl_file_path=str(row[1]),
        active=bool(row[2]),
        file_size_bytes=int(row[3]) if row[3] is not None else None,
        file_mtime=_parse_db_timestamp(row[4]),
        checkpoint=checkpoint,
    )


def _parse_db_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.replace(" ", "T")
    if normalized.endswith("+00"):
        normalized = f"{normalized}:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
