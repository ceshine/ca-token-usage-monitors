"""Typed schemas used by OpenCode ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SourceCheckpoint:
    """Incremental ingestion checkpoint tuple."""

    last_time_updated_ms: int
    last_message_id: str


@dataclass(frozen=True)
class SessionRow:
    """One row persisted in opencode_sessions."""

    session_id: str
    project_id: str
    project_worktree: str | None
    session_title: str
    session_directory: str
    session_version: str


@dataclass(frozen=True)
class MessageUsageRow:
    """One assistant message usage row persisted in opencode_message_usage."""

    message_id: str
    session_id: str
    project_id: str
    message_created_at: datetime
    message_completed_at: datetime | None
    provider_code: str | None
    model_code: str | None
    agent: str | None
    mode: str | None
    finish_reason: str | None
    input_tokens: int
    output_tokens: int
    reasoning_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    total_tokens: int | None
    cost_usd: float | None
    source_time_updated_ms: int


@dataclass(frozen=True)
class SourceMessageRow:
    """Raw assistant message row loaded from SQLite with joins."""

    message_id: str
    session_id: str
    time_created_ms: int
    time_updated_ms: int
    data_json: str
    project_id: str
    session_title: str
    session_directory: str
    session_version: str
    project_worktree: str | None


@dataclass
class IngestionCounters:
    """Aggregate counters emitted by OpenCode ingestion service."""

    messages_scanned: int = 0
    messages_ingested: int = 0
    sessions_upserted: int = 0
    batches_flushed: int = 0
    skipped_no_source_changes: bool = False
