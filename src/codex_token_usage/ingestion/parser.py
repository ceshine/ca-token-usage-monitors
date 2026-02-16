"""Parsing helpers for Codex session ingestion."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import orjson

from .errors import ModelAttributionError, ParseError, SessionIdentityError
from .schemas import (
    ParsedSessionFile,
    SessionCheckpoint,
    SessionIdentity,
    SessionMetadataRow,
    TokenEventRow,
    TokenUsageValues,
)

LOGGER = logging.getLogger(__name__)


def parse_session_identity(session_file_path: Path) -> SessionIdentity:
    """Resolve session identity from the first `session_meta` event in a JSONL file."""
    for line_number, event in _iter_json_events(session_file_path):
        if event.get("type") != "session_meta":
            continue

        payload = _as_mapping(event.get("payload"), session_file_path, line_number, "session_meta.payload")
        raw_session_id = payload.get("id")
        if raw_session_id is None:
            raise SessionIdentityError(f"Missing session_meta.payload.id in {session_file_path} at line {line_number}.")

        session_id = _parse_uuid(raw_session_id, session_file_path, line_number, "session_meta.payload.id")
        timestamp = _parse_optional_timestamp(event.get("timestamp"), session_file_path, line_number, "timestamp")
        cwd = payload.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise ParseError(
                f"Invalid session_meta.payload.cwd type in {session_file_path} at line {line_number}: "
                f"expected str or null, got {type(cwd).__name__}."
            )
        return SessionIdentity(session_id=session_id, session_timestamp=timestamp, cwd=cwd)

    raise SessionIdentityError(f"No session_meta event found in {session_file_path}.")


def parse_session_file(
    session_file_path: Path,
    session_identity: SessionIdentity,
    checkpoint: SessionCheckpoint | None,
) -> ParsedSessionFile:
    """Parse one session file and extract token event rows filtered by optional checkpoint."""
    current_model_code: str | None = None
    current_turn_id: UUID | None = None
    token_rows: list[TokenEventRow] = []

    token_rows_raw = 0
    token_rows_skipped_info_null = 0
    token_rows_skipped_before_checkpoint = 0
    first_session_meta_seen = False

    for line_number, event in _iter_json_events(session_file_path):
        event_type = event.get("type")

        if event_type == "session_meta" and not first_session_meta_seen:
            first_session_meta_seen = True
            payload = _as_mapping(event.get("payload"), session_file_path, line_number, "session_meta.payload")
            raw_session_id = payload.get("id")
            if raw_session_id is None:
                raise SessionIdentityError(
                    f"Missing session_meta.payload.id in {session_file_path} at line {line_number}."
                )
            session_id = _parse_uuid(raw_session_id, session_file_path, line_number, "session_meta.payload.id")
            if session_id != session_identity.session_id:
                raise SessionIdentityError(
                    f"Session identity mismatch in {session_file_path}: expected {session_identity.session_id}, "
                    f"got {session_id} at line {line_number}."
                )
            continue

        if event_type == "turn_context":
            payload = _as_mapping(event.get("payload"), session_file_path, line_number, "turn_context.payload")

            if "model" in payload:
                model_value = payload.get("model")
                if model_value is None:
                    current_model_code = None
                elif isinstance(model_value, str):
                    current_model_code = model_value
                else:
                    raise ParseError(
                        f"Invalid turn_context.payload.model in {session_file_path} at line {line_number}: "
                        f"expected str or null, got {type(model_value).__name__}."
                    )

            if "turn_id" in payload:
                turn_id_value = payload.get("turn_id")
                current_turn_id = _parse_optional_uuid(
                    turn_id_value, session_file_path, line_number, "turn_context.payload.turn_id"
                )
            else:
                current_turn_id = None
            continue

        token_payload = _extract_token_count_payload(event)
        if token_payload is None:
            continue

        info_value = token_payload.get("info")
        if info_value is None:
            token_rows_skipped_info_null += 1
            continue

        info = _as_mapping(info_value, session_file_path, line_number, "event_msg.payload.info")
        total_usage = _extract_usage_snapshot(info, "total_token_usage", session_file_path, line_number)
        last_usage = _extract_usage_snapshot(info, "last_token_usage", session_file_path, line_number)
        if total_usage is None or last_usage is None:
            continue

        if current_model_code is None:
            raise ModelAttributionError(
                f"Token event in {session_file_path} at line {line_number} has no active model context."
            )

        event_timestamp = _parse_required_timestamp(event.get("timestamp"), session_file_path, line_number, "timestamp")
        token_rows_raw += 1

        if checkpoint is not None and not _passes_checkpoint(event_timestamp, total_usage.total_tokens, checkpoint):
            token_rows_skipped_before_checkpoint += 1
            continue

        token_rows.append(
            TokenEventRow(
                session_id=session_identity.session_id,
                event_timestamp=event_timestamp,
                event_line_number=line_number,
                model_code=current_model_code,
                turn_id=current_turn_id,
                total_usage=total_usage,
                last_usage=last_usage,
            )
        )

    if not first_session_meta_seen:
        raise SessionIdentityError(f"No session_meta event found in {session_file_path}.")

    metadata = SessionMetadataRow(
        session_id=session_identity.session_id,
        session_timestamp=session_identity.session_timestamp,
        cwd=session_identity.cwd,
        session_file_path=str(session_file_path),
    )

    LOGGER.debug(
        "Parsed session file %s: %d candidate rows, %d info-null skipped, %d checkpoint-skipped",
        session_file_path,
        token_rows_raw,
        token_rows_skipped_info_null,
        token_rows_skipped_before_checkpoint,
    )

    return ParsedSessionFile(
        metadata=metadata,
        token_rows=token_rows,
        token_rows_raw=token_rows_raw,
        token_rows_skipped_info_null=token_rows_skipped_info_null,
        token_rows_skipped_before_checkpoint=token_rows_skipped_before_checkpoint,
    )


def _iter_json_events(session_file_path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield parsed JSON events with line numbers."""
    with session_file_path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            try:
                payload = orjson.loads(raw_line)
            except orjson.JSONDecodeError as exc:
                raise ParseError(f"Malformed JSON in {session_file_path} at line {line_number}: {exc}.") from exc
            if not isinstance(payload, dict):
                raise ParseError(
                    f"Expected JSON object in {session_file_path} at line {line_number}, got {type(payload).__name__}."
                )
            yield line_number, payload


def _extract_token_count_payload(event: dict[str, Any]) -> dict[str, Any] | None:
    """Return event payload when the event is a token_count message."""
    if event.get("type") != "event_msg":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "token_count":
        return None
    return payload


def _extract_usage_snapshot(
    info: dict[str, Any],
    key: str,
    session_file_path: Path,
    line_number: int,
) -> TokenUsageValues | None:
    """Extract one usage snapshot; return None when required fields are absent."""
    raw_snapshot = info.get(key)
    if not isinstance(raw_snapshot, dict):
        return None

    values: dict[str, int] = {}
    for field_name in (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    ):
        if field_name not in raw_snapshot:
            return None
        raw_value = raw_snapshot[field_name]
        if not isinstance(raw_value, int):
            raise ParseError(
                f"Invalid {key}.{field_name} in {session_file_path} at line {line_number}: "
                f"expected int, got {type(raw_value).__name__}."
            )
        values[field_name] = raw_value

    return TokenUsageValues(
        input_tokens=values["input_tokens"],
        cached_input_tokens=values["cached_input_tokens"],
        output_tokens=values["output_tokens"],
        reasoning_output_tokens=values["reasoning_output_tokens"],
        total_tokens=values["total_tokens"],
    )


def _passes_checkpoint(
    event_timestamp: datetime,
    total_tokens_cumulative: int,
    checkpoint: SessionCheckpoint,
) -> bool:
    """Return True when a token row is in the ingestion tail window."""
    if event_timestamp > checkpoint.last_ts:
        return True
    return event_timestamp == checkpoint.last_ts and total_tokens_cumulative >= checkpoint.last_total_tokens_cumulative


def _as_mapping(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> dict[str, Any]:
    """Validate that a value is a JSON object mapping."""
    if not isinstance(value, dict):
        raise ParseError(
            f"Invalid {field_name} in {session_file_path} at line {line_number}: "
            f"expected object, got {type(value).__name__}."
        )
    return value


def _parse_required_timestamp(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> datetime:
    """Parse a required RFC3339-style timestamp into an aware datetime."""
    timestamp = _parse_optional_timestamp(value, session_file_path, line_number, field_name)
    if timestamp is None:
        raise ParseError(f"Missing required {field_name} in {session_file_path} at line {line_number}.")
    return timestamp


def _parse_optional_timestamp(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> datetime | None:
    """Parse an optional RFC3339-style timestamp into an aware datetime."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ParseError(
            f"Invalid {field_name} in {session_file_path} at line {line_number}: "
            f"expected str or null, got {type(value).__name__}."
        )

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ParseError(f"Invalid timestamp '{value}' in {session_file_path} at line {line_number}.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_uuid(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> UUID:
    """Parse a required UUID value."""
    if not isinstance(value, str):
        raise ParseError(
            f"Invalid {field_name} in {session_file_path} at line {line_number}: "
            f"expected UUID string, got {type(value).__name__}."
        )
    try:
        return UUID(value)
    except ValueError as exc:
        raise ParseError(
            f"Invalid UUID '{value}' for {field_name} in {session_file_path} at line {line_number}."
        ) from exc


def _parse_optional_uuid(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> UUID | None:
    """Parse an optional UUID value."""
    if value is None:
        return None
    return _parse_uuid(value, session_file_path, line_number, field_name)
