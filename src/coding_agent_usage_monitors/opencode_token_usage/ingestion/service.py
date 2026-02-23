"""Service orchestration for OpenCode SQLite ingestion."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from .errors import ParseError
from .repository import IngestionRepository
from .schemas import IngestionCounters, MessageUsageRow, SessionRow
from .source_reader import SourceReader


class IngestionService:
    """Coordinates source reads, parsing, and DuckDB upserts."""

    def __init__(
        self,
        repository: IngestionRepository,
        source_reader: SourceReader,
        batch_size: int = 1000,
    ) -> None:
        self._repository = repository
        self._source_reader = source_reader
        self._batch_size = batch_size

    def ingest(self, full_refresh: bool = False) -> IngestionCounters:
        """Run ingestion from SQLite source into DuckDB."""
        if self._batch_size <= 0:
            raise ValueError("batch_size must be positive")

        self._repository.ensure_schema()
        self._source_reader.ensure_schema()

        counters = IngestionCounters()
        checkpoint = None if full_refresh else self._repository.get_checkpoint()

        if not full_refresh and checkpoint is not None:
            source_max = self._source_reader.get_latest_assistant_time_updated_ms()
            if source_max is None or source_max < checkpoint.last_time_updated_ms:
                counters.skipped_no_source_changes = True
                return counters

        source_rows = self._source_reader.iter_assistant_rows(checkpoint)
        sessions_by_id: dict[str, SessionRow] = {}
        usage_batch: list[MessageUsageRow] = []

        for source_row in source_rows:
            counters.messages_scanned += 1
            session_row, usage_row = _parse_source_row(source_row)
            sessions_by_id[session_row.session_id] = session_row
            usage_batch.append(usage_row)

            if len(usage_batch) >= self._batch_size:
                self._flush_batch(
                    sessions=list(sessions_by_id.values()),
                    usage_rows=usage_batch,
                )
                counters.batches_flushed += 1
                counters.sessions_upserted += len(sessions_by_id)
                counters.messages_ingested += len(usage_batch)
                sessions_by_id.clear()
                usage_batch = []

        if usage_batch:
            self._flush_batch(
                sessions=list(sessions_by_id.values()),
                usage_rows=usage_batch,
            )
            counters.batches_flushed += 1
            counters.sessions_upserted += len(sessions_by_id)
            counters.messages_ingested += len(usage_batch)

        return counters

    def _flush_batch(self, sessions: list[SessionRow], usage_rows: list[MessageUsageRow]) -> None:
        with self._repository.transaction():
            self._repository.upsert_sessions(sessions)
            self._repository.upsert_message_usage(usage_rows)


def _parse_source_row(source_row: Any) -> tuple[SessionRow, MessageUsageRow]:
    try:
        payload = json.loads(source_row.data_json)
    except json.JSONDecodeError as exc:
        raise ParseError(f"Invalid JSON in message {source_row.message_id}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ParseError(f"Expected object payload in message {source_row.message_id}")

    role = payload.get("role")
    if role != "assistant":
        raise ParseError(f"Expected assistant role in message {source_row.message_id}")

    tokens_payload = payload.get("tokens")
    if not isinstance(tokens_payload, dict):
        raise ParseError(f"Missing tokens object in message {source_row.message_id}")

    input_tokens = _require_int(tokens_payload, "input", source_row.message_id)
    output_tokens = _require_int(tokens_payload, "output", source_row.message_id)
    reasoning_tokens = _require_int(tokens_payload, "reasoning", source_row.message_id)

    cache_payload = tokens_payload.get("cache")
    if not isinstance(cache_payload, dict):
        raise ParseError(f"Missing tokens.cache object in message {source_row.message_id}")
    cache_read_tokens = _require_int(cache_payload, "read", source_row.message_id, parent="tokens.cache")
    cache_write_tokens = _require_int(cache_payload, "write", source_row.message_id, parent="tokens.cache")

    total_tokens = tokens_payload.get("total")
    if total_tokens is not None and (not isinstance(total_tokens, int) or isinstance(total_tokens, bool)):
        raise ParseError(f"Invalid tokens.total in message {source_row.message_id}: expected int or null")

    cost_usd = payload.get("cost")
    if cost_usd is not None:
        if isinstance(cost_usd, bool) or not isinstance(cost_usd, int | float):
            raise ParseError(f"Invalid cost in message {source_row.message_id}: expected int/float or null")
        cost_usd = float(cost_usd)

    completed_time = _parse_completed_time(payload.get("time"), source_row.message_id)
    finish_reason = _optional_str(payload, "finish", source_row.message_id)

    session_row = SessionRow(
        session_id=source_row.session_id,
        project_id=source_row.project_id,
        project_worktree=source_row.project_worktree,
        session_title=source_row.session_title,
        session_directory=source_row.session_directory,
        session_version=source_row.session_version,
    )

    usage_row = MessageUsageRow(
        message_id=source_row.message_id,
        session_id=source_row.session_id,
        project_id=source_row.project_id,
        message_created_at=_ms_to_datetime(source_row.time_created_ms, source_row.message_id, "time_created"),
        message_completed_at=completed_time,
        provider_code=_optional_str(payload, "providerID", source_row.message_id),
        model_code=_optional_str(payload, "modelID", source_row.message_id),
        agent=_optional_str(payload, "agent", source_row.message_id),
        mode=_optional_str(payload, "mode", source_row.message_id),
        finish_reason=finish_reason,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        reasoning_tokens=reasoning_tokens,
        cache_read_tokens=cache_read_tokens,
        cache_write_tokens=cache_write_tokens,
        total_tokens=total_tokens,
        cost_usd=cost_usd,
        source_time_updated_ms=source_row.time_updated_ms,
    )
    return session_row, usage_row


def _require_int(payload: dict[str, Any], field: str, message_id: str, parent: str = "tokens") -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ParseError(f"Missing or invalid {parent}.{field} in message {message_id}: expected int")
    return value


def _optional_str(payload: dict[str, Any], field: str, message_id: str) -> str | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ParseError(f"Invalid {field} in message {message_id}: expected string or null")
    return value


def _parse_completed_time(time_payload: Any, message_id: str) -> datetime | None:
    if time_payload is None:
        return None
    if not isinstance(time_payload, dict):
        raise ParseError(f"Invalid time payload in message {message_id}: expected object")

    completed_ms = time_payload.get("completed")
    if completed_ms is None:
        return None
    if not isinstance(completed_ms, int) or isinstance(completed_ms, bool):
        raise ParseError(f"Invalid time.completed in message {message_id}: expected int milliseconds")
    return _ms_to_datetime(completed_ms, message_id, "time.completed")


def _ms_to_datetime(value: Any, message_id: str, field_name: str) -> datetime:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ParseError(f"Invalid {field_name} in message {message_id}: expected int milliseconds")
    return datetime.fromtimestamp(value / 1000, tz=UTC)
