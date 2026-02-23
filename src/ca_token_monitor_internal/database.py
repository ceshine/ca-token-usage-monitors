"""Shared database utilities for coding-agent-token-monitor."""

from __future__ import annotations

from datetime import UTC, datetime


def parse_db_timestamp(value: str | None) -> datetime | None:
    """Parse DuckDB TIMESTAMPTZ string output into an aware datetime."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Expected timestamp string from DB, got {type(value).__name__}.")
    normalized = value.replace(" ", "T")
    if normalized.endswith("+00"):
        normalized = f"{normalized}:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
