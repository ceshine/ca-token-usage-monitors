"""Typed schemas used by the ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from uuid import UUID


TOKEN_FIELDS: tuple[str, ...] = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


@dataclass(frozen=True)
class TokenUsageValues:
    """Token usage counters for one snapshot."""

    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_output_tokens: int
    total_tokens: int

    def field_value(self, field_name: str) -> int:
        """Return a field value by field name."""
        return getattr(self, field_name)


@dataclass(frozen=True)
class SessionMetadataRow:
    """One row persisted in codex_session_metadata."""

    session_id: UUID
    session_timestamp: datetime | None
    cwd: str | None
    session_file_path: str


@dataclass(frozen=True)
class SessionCheckpoint:
    """Tail-ingestion checkpoint loaded from codex_session_details."""

    last_ts: datetime
    last_total_tokens_cumulative: int


@dataclass(frozen=True)
class IngestionFileState:
    """File bookkeeping state persisted in codex_ingestion_files."""

    session_file_path: str
    file_size_bytes: int
    file_mtime: datetime


@dataclass(frozen=True)
class SessionIdentity:
    """Resolved session identity from the first session_meta event."""

    session_id: UUID
    session_timestamp: datetime | None
    cwd: str | None


@dataclass(frozen=True)
class TokenEventRow:
    """Internal token event row built from one token_count event."""

    session_id: UUID
    event_timestamp: datetime
    event_line_number: int
    model_code: str
    turn_id: UUID | None
    total_usage: TokenUsageValues
    last_usage: TokenUsageValues

    @property
    def total_tokens_cumulative(self) -> int:
        """Return total_tokens from cumulative totals snapshot."""
        return self.total_usage.total_tokens


@dataclass(frozen=True)
class ParsedSessionFile:
    """Parser output for one session file."""

    metadata: SessionMetadataRow
    token_rows: list[TokenEventRow]
    token_rows_raw: int
    token_rows_skipped_info_null: int
    token_rows_skipped_before_checkpoint: int


@dataclass(frozen=True)
class DedupeResult:
    """Deduplication output and counters for one session."""

    token_rows: list[TokenEventRow]
    duplicate_rows_skipped: int


@dataclass
class IngestionCounters:
    """Ingestion counters emitted by service.ingest()."""

    files_scanned: int = 0
    files_ingested: int = 0
    files_skipped_unchanged: int = 0
    sessions_ingested: int = 0
    token_rows_raw: int = 0
    token_rows_deduped: int = 0
    token_rows_skipped_info_null: int = 0
    token_rows_skipped_before_checkpoint: int = 0
    duplicate_rows_skipped: int = 0
    monotonicity_errors: int = 0
    delta_consistency_errors: int = 0
    parse_errors: int = 0
    failed_files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FileParseContext:
    """Parser context for detailed errors and logging."""

    session_file_path: Path
    line_number: int
