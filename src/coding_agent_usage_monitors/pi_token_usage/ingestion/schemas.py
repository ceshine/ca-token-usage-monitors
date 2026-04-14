"""Typed schemas used by the Pi agent ingestion pipeline."""

from __future__ import annotations

from datetime import datetime
from dataclasses import field, dataclass


@dataclass(frozen=True)
class PiReportedCost:
    """Pi-reported per-message cost breakdown (reference-only)."""

    input_usd: float | None
    output_usd: float | None
    cache_read_usd: float | None
    cache_write_usd: float | None
    total_usd: float | None


@dataclass(frozen=True)
class SessionMetadataRow:
    """One row persisted in pi_session_metadata."""

    session_id: str
    session_version: int
    cwd: str
    session_started_at: datetime
    session_file_path: str


@dataclass(frozen=True)
class UsageEventRow:
    """One row persisted in pi_usage_events."""

    session_id: str
    message_id: str
    parent_id: str | None
    event_timestamp: datetime
    event_line_number: int
    provider_code: str | None
    model_code: str | None
    stop_reason: str | None
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    total_tokens: int | None
    reported_cost: PiReportedCost


@dataclass(frozen=True)
class SessionCheckpoint:
    """Tail-ingestion checkpoint loaded from pi_usage_events."""

    last_ts: datetime
    last_message_id: str


@dataclass(frozen=True)
class IngestionFileState:
    """File bookkeeping state persisted in pi_ingestion_files."""

    session_file_path: str
    file_size_bytes: int
    file_mtime: datetime


@dataclass(frozen=True)
class ParsedSessionFile:
    """Parser output for one Pi session file."""

    metadata: SessionMetadataRow
    usage_rows: list[UsageEventRow]
    usage_rows_raw: int
    usage_rows_skipped_before_checkpoint: int
    cwd_recovered_from_path: bool


@dataclass
class IngestionCounters:
    """Ingestion counters emitted by service.ingest()."""

    files_scanned: int = 0
    files_ingested: int = 0
    files_skipped_unchanged: int = 0
    sessions_ingested: int = 0
    usage_rows_raw: int = 0
    usage_rows_persisted: int = 0
    usage_rows_skipped_before_checkpoint: int = 0
    sessions_cwd_recovered_from_path: int = 0
    parse_errors: int = 0
    failed_files: list[str] = field(default_factory=list)
