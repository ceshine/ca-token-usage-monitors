"""Typed schemas used by Gemini ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID


@dataclass(frozen=True)
class SourceCheckpoint:
    """Ingestion checkpoint tuple for one project source."""

    last_event_timestamp: datetime
    last_model_code: str


@dataclass(frozen=True)
class IngestionSourceRow:
    """Tracked ingestion source row from DuckDB."""

    project_id: UUID
    jsonl_file_path: str
    active: bool
    file_size_bytes: int | None
    file_mtime: datetime | None
    checkpoint: SourceCheckpoint | None


@dataclass(frozen=True)
class JsonlFileState:
    """Filesystem metadata for one JSONL file."""

    jsonl_file_path: str
    file_size_bytes: int
    file_mtime: datetime


@dataclass(frozen=True)
class UsageEventRow:
    """One parsed usage event row for persistence."""

    project_id: UUID
    event_timestamp: datetime
    model_code: str
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    thoughts_tokens: int
    total_tokens: int


@dataclass(frozen=True)
class ParsedJsonlFile:
    """Parser output for one preprocessed telemetry JSONL."""

    project_id: UUID
    usage_rows: list[UsageEventRow]
    usage_events_total: int
    usage_events_skipped_before_checkpoint: int
    max_event_key: tuple[datetime, str] | None


@dataclass
class IngestionCounters:
    """Aggregate counters emitted by Gemini ingestion service."""

    sources_scanned: int = 0
    sources_ingested: int = 0
    sources_skipped_unchanged: int = 0
    sources_missing: int = 0
    sources_auto_deactivated: int = 0
    usage_events_total: int = 0
    usage_events_skipped_before_checkpoint: int = 0
    usage_rows_attempted_insert: int = 0


@dataclass(frozen=True)
class ResolvedInputPath:
    """Resolved ingest input with original user input for error messaging."""

    original_path: Path
    jsonl_file_path: Path
