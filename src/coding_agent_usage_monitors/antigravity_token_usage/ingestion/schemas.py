"""Typed schemas used by the AntiGravity token ingestion pipeline."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from dataclasses import field, dataclass


@dataclass(frozen=True)
class DiscoveredDatabase:
    """Represents a discovered AntiGravity SQLite database source."""

    database_path: Path
    discovery_source: str
    session_id: str


@dataclass(frozen=True)
class RawUsageEvent:
    """Candidate token usage event before global deduplication."""

    session_id: str
    discovery_source: str
    event_timestamp: datetime
    timestamp_rank: int
    model_code: str
    provider_id: int | None
    input_tokens: int
    output_tokens: int
    reasoning_tokens: int
    total_output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int
    total_tokens: int
    response_id: str | None
    provider_assigned_message_id: str | None
    message_id: str | None
    fallback_key: str


@dataclass(frozen=True)
class UsageEventRow:
    """Canonical event row ready for DuckDB persistence."""

    event_key: str
    discovery_source: str
    session_id: str
    event_timestamp: datetime
    model_code: str
    provider_id: int | None
    input_tokens: int
    output_tokens: int
    reasoning_tokens: int
    total_output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int
    total_tokens: int
    response_id: str | None
    provider_assigned_message_id: str | None
    message_id: str | None


@dataclass
class IngestionCounters:
    """Ingestion counters and execution status emitted by IngestionService."""

    databases_discovered: int = 0
    databases_parsed: int = 0
    databases_failed: int = 0
    raw_events_extracted: int = 0
    canonical_events_written: int = 0
    duplicate_events_merged: int = 0
    missing_optional_tables: int = 0
    failed_databases: list[str] = field(default_factory=list)
