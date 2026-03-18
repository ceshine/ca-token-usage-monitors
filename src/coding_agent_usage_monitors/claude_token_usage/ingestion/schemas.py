"""Typed schemas used by the Claude Code ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class TokenUsageValues:
    """Token usage counters for one assistant message."""

    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int


@dataclass(frozen=True)
class SessionMetadataRow:
    """One row persisted in claude_session_metadata."""

    session_id: str
    project_name: str | None
    slug: str | None
    cwd: str | None
    version: str | None
    session_file_path: str


@dataclass(frozen=True)
class UsageEventRow:
    """One row persisted in claude_usage_events."""

    session_id: str
    message_id: str
    request_id: str
    event_timestamp: datetime
    event_line_number: int
    model_code: str
    is_sidechain: bool
    agent_id: str | None
    usage: TokenUsageValues


@dataclass(frozen=True)
class SessionCheckpoint:
    """Tail-ingestion checkpoint loaded from claude_usage_events."""

    last_ts: datetime
    last_message_id: str
    last_request_id: str


@dataclass(frozen=True)
class IngestionFileState:
    """File bookkeeping state persisted in claude_ingestion_files."""

    session_file_path: str
    file_size_bytes: int
    file_mtime: datetime


@dataclass(frozen=True)
class ParsedSessionFile:
    """Parser output for one session file."""

    metadata: SessionMetadataRow
    usage_rows: list[UsageEventRow]
    usage_rows_raw: int
    usage_rows_skipped_synthetic: int
    usage_rows_skipped_before_checkpoint: int
    duplicate_rows_skipped: int


@dataclass
class IngestionCounters:
    """Ingestion counters emitted by service.ingest()."""

    files_scanned: int = 0
    files_ingested: int = 0
    files_skipped_unchanged: int = 0
    sessions_ingested: int = 0
    usage_rows_raw: int = 0
    usage_rows_deduped: int = 0
    usage_rows_skipped_synthetic: int = 0
    usage_rows_skipped_before_checkpoint: int = 0
    duplicate_rows_skipped: int = 0
    parse_errors: int = 0
    failed_files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FileParseContext:
    """Parser context for detailed errors and logging."""

    session_file_path: Path
    line_number: int
