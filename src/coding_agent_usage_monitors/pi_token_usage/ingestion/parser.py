"""Parsing helpers for Pi agent session ingestion."""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import UTC, datetime
from collections.abc import Iterator
from typing import Any

import orjson

from .errors import ParseError
from .schemas import UsageEventRow, PiReportedCost, ParsedSessionFile, SessionCheckpoint, SessionMetadataRow

LOGGER = logging.getLogger(__name__)


def discover_session_root(override: Path | None = None) -> Path:
    """Return the Pi agent sessions root.

    Args:
        override: Optional explicit root. When ``None``, default is
            ``~/.pi/agent/sessions``.

    Returns:
        Path to the Pi sessions root (may not exist on disk yet).
    """
    if override is not None:
        return override
    return Path.home() / ".pi" / "agent" / "sessions"


def discover_session_files(session_root: Path) -> list[Path]:
    """Return sorted list of ``*.jsonl`` session files under ``session_root``.

    Args:
        session_root: Directory to scan recursively.

    Returns:
        Sorted list of JSONL file paths. Empty when root is missing.
    """
    if not session_root.exists():
        return []
    return sorted(path for path in session_root.rglob("*.jsonl") if path.is_file())


def parse_session_identity(session_file_path: Path) -> tuple[SessionMetadataRow, bool]:
    """Extract session metadata from the line-1 ``session`` entry.

    The Pi format guarantees the ``session`` entry is line 1. ``cwd`` may be
    recovered from the parent directory name if missing from the entry.

    Args:
        session_file_path: Path to the Pi session JSONL file.

    Returns:
        Tuple of ``(SessionMetadataRow, cwd_recovered_from_path)``. The flag
        is ``True`` when ``cwd`` was decoded from the parent directory layout.

    Raises:
        ParseError: When the session entry is missing, malformed, or any
            required field (``id``, ``version``, ``timestamp``, ``cwd``) is
            unavailable from both the entry and the filesystem layout.
    """
    first_event = _read_first_event(session_file_path)

    if first_event.get("type") != "session":
        raise ParseError(
            f"Expected line 1 to be a 'session' entry in {session_file_path}, got type={first_event.get('type')!r}."
        )

    session_id = first_event.get("id")
    if not isinstance(session_id, str) or not session_id:
        raise ParseError(f"Missing or invalid 'id' on session entry in {session_file_path}.")

    version_raw = first_event.get("version")
    if not isinstance(version_raw, int) or isinstance(version_raw, bool):
        raise ParseError(f"Missing or invalid 'version' on session entry in {session_file_path}.")

    timestamp_raw = first_event.get("timestamp")
    session_started_at = _parse_required_timestamp(timestamp_raw, session_file_path, 1, "timestamp")

    filename_session_id = _extract_session_id_from_filename(session_file_path.name)
    if filename_session_id is not None and filename_session_id != session_id:
        raise ParseError(
            f"Session id mismatch in {session_file_path}: filename has "
            f"{filename_session_id!r}, entry has {session_id!r}."
        )

    cwd_raw = first_event.get("cwd")
    cwd_recovered_from_path = False
    if isinstance(cwd_raw, str) and cwd_raw:
        cwd = cwd_raw
    else:
        cwd_from_dir = _decode_cwd_from_parent_dir(session_file_path)
        if cwd_from_dir is None:
            raise ParseError(
                f"Missing 'cwd' on session entry in {session_file_path} and parent directory name cannot be decoded."
            )
        cwd = cwd_from_dir
        cwd_recovered_from_path = True
        LOGGER.warning(
            "Recovered cwd from parent directory layout for %s -> %s",
            session_file_path,
            cwd,
        )

    metadata = SessionMetadataRow(
        session_id=session_id,
        session_version=version_raw,
        cwd=cwd,
        session_started_at=session_started_at,
        session_file_path=str(session_file_path),
    )
    return metadata, cwd_recovered_from_path


def parse_session_file(
    session_file_path: Path,
    checkpoint: SessionCheckpoint | None = None,
    identity: tuple[SessionMetadataRow, bool] | None = None,
) -> ParsedSessionFile:
    """Parse a Pi session file and return metadata plus assistant usage rows.

    Args:
        session_file_path: Path to the JSONL session file.
        checkpoint: Optional tail-filter checkpoint from prior ingestion.
        identity: Optional pre-computed ``(metadata, cwd_recovered_from_path)``
            tuple from :func:`parse_session_identity`. When ``None``, identity
            is resolved by reading line 1 again.

    Returns:
        ParsedSessionFile with metadata and usage rows (post-checkpoint).

    Raises:
        ParseError: When structural invariants are violated (e.g. missing
            required fields on assistant rows, duplicate entry ids).
    """
    if identity is None:
        metadata, cwd_recovered_from_path = parse_session_identity(session_file_path)
    else:
        metadata, cwd_recovered_from_path = identity
    session_id = metadata.session_id

    usage_rows: list[UsageEventRow] = []
    seen_ids: set[str] = set()
    usage_rows_raw = 0
    usage_rows_skipped_before_checkpoint = 0

    for line_number, event in _iter_json_events(session_file_path):
        if line_number == 1:
            # Already consumed as the session entry.
            continue
        if event.get("type") != "message":
            continue

        message = event.get("message")
        if not isinstance(message, dict):
            continue
        if message.get("role") != "assistant":
            continue

        usage_raw = message.get("usage")
        if not isinstance(usage_raw, dict):
            continue
        if "input" not in usage_raw or "output" not in usage_raw:
            continue

        message_id = event.get("id")
        if not isinstance(message_id, str) or not message_id:
            raise ParseError(f"Missing required 'id' on assistant entry in {session_file_path} at line {line_number}.")

        if message_id in seen_ids:
            raise ParseError(f"Duplicate entry id {message_id!r} in {session_file_path} at line {line_number}.")
        seen_ids.add(message_id)

        event_timestamp = _parse_required_timestamp(event.get("timestamp"), session_file_path, line_number, "timestamp")

        input_tokens = _require_int(usage_raw, "input", session_file_path, line_number, parent="message.usage")
        output_tokens = _require_int(usage_raw, "output", session_file_path, line_number, parent="message.usage")
        cache_read_tokens = _optional_int(
            usage_raw, "cacheRead", session_file_path, line_number, parent="message.usage", default=0
        )
        cache_write_tokens = _optional_int(
            usage_raw, "cacheWrite", session_file_path, line_number, parent="message.usage", default=0
        )
        total_tokens = _optional_int(
            usage_raw, "totalTokens", session_file_path, line_number, parent="message.usage", default=None
        )

        usage_rows_raw += 1

        if checkpoint is not None and not _passes_checkpoint(event_timestamp, message_id, checkpoint):
            usage_rows_skipped_before_checkpoint += 1
            continue

        reported_cost = _extract_cost(usage_raw.get("cost"), session_file_path, line_number)

        parent_id = event.get("parentId")
        if parent_id is not None and not isinstance(parent_id, str):
            raise ParseError(
                f"Invalid 'parentId' on assistant entry in {session_file_path} at line {line_number}: "
                f"expected string or null, got {type(parent_id).__name__}."
            )

        provider_code = _optional_str(message, "provider", session_file_path, line_number)
        model_code = _optional_str(message, "model", session_file_path, line_number)
        stop_reason = _optional_str(message, "stopReason", session_file_path, line_number)

        usage_rows.append(
            UsageEventRow(
                session_id=session_id,
                message_id=message_id,
                parent_id=parent_id,
                event_timestamp=event_timestamp,
                event_line_number=line_number,
                provider_code=provider_code,
                model_code=model_code,
                stop_reason=stop_reason,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_write_tokens=cache_write_tokens,
                total_tokens=total_tokens,
                reported_cost=reported_cost,
            )
        )

    return ParsedSessionFile(
        metadata=metadata,
        usage_rows=usage_rows,
        usage_rows_raw=usage_rows_raw,
        usage_rows_skipped_before_checkpoint=usage_rows_skipped_before_checkpoint,
        cwd_recovered_from_path=cwd_recovered_from_path,
    )


def _iter_json_events(session_file_path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield ``(line_number, event)`` tuples for each non-empty JSON line."""
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


def _read_first_event(session_file_path: Path) -> dict[str, Any]:
    """Read and decode line 1 of the file, raising ``ParseError`` when empty."""
    for _line_number, event in _iter_json_events(session_file_path):
        return event
    raise ParseError(f"Session file is empty: {session_file_path}.")


def _extract_session_id_from_filename(filename: str) -> str | None:
    """Extract the trailing ``<sessionId>`` from a Pi session filename.

    The format is ``<iso-timestamp>_<sessionId>.jsonl``.
    """
    if not filename.endswith(".jsonl"):
        return None
    stem = filename[: -len(".jsonl")]
    if "_" not in stem:
        return None
    return stem.rsplit("_", 1)[1] or None


def _decode_cwd_from_parent_dir(session_file_path: Path) -> str | None:
    """Decode a cwd path from Pi's parent directory name encoding.

    Pi encodes the working directory as ``--<path-with-slashes-replaced-by-dashes>--``
    (e.g. ``/home/foo`` -> ``--home-foo--``). Returns ``None`` when the layout
    is malformed.
    """
    dir_name = session_file_path.parent.name
    if not (dir_name.startswith("--") and dir_name.endswith("--")):
        return None
    stripped = dir_name[2:-2]
    if not stripped:
        return None
    return "/" + stripped.replace("-", "/")


def _passes_checkpoint(
    event_timestamp: datetime,
    message_id: str,
    checkpoint: SessionCheckpoint,
) -> bool:
    """Return True when an event is strictly newer than the checkpoint."""
    if event_timestamp > checkpoint.last_ts:
        return True
    if event_timestamp < checkpoint.last_ts:
        return False
    return message_id > checkpoint.last_message_id


def _parse_required_timestamp(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> datetime:
    """Parse a required ISO-8601 timestamp into an aware UTC datetime."""
    if value is None:
        raise ParseError(f"Missing required {field_name} in {session_file_path} at line {line_number}.")
    if not isinstance(value, str):
        raise ParseError(
            f"Invalid {field_name} in {session_file_path} at line {line_number}: "
            f"expected str, got {type(value).__name__}."
        )

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ParseError(f"Invalid timestamp {value!r} in {session_file_path} at line {line_number}.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _require_int(
    payload: dict[str, Any],
    field_name: str,
    session_file_path: Path,
    line_number: int,
    parent: str,
) -> int:
    value = payload.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ParseError(
            f"Missing or invalid {parent}.{field_name} in {session_file_path} at line {line_number}: expected int."
        )
    return value


def _optional_int(
    payload: dict[str, Any],
    field_name: str,
    session_file_path: Path,
    line_number: int,
    parent: str,
    default: int | None,
) -> int | None:
    value = payload.get(field_name)
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool):
        raise ParseError(
            f"Invalid {parent}.{field_name} in {session_file_path} at line {line_number}: expected int or null."
        )
    return value


def _optional_str(
    payload: dict[str, Any],
    field_name: str,
    session_file_path: Path,
    line_number: int,
) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ParseError(
            f"Invalid {field_name} in {session_file_path} at line {line_number}: "
            f"expected string or null, got {type(value).__name__}."
        )
    return value


def _extract_cost(cost_raw: Any, session_file_path: Path, line_number: int) -> PiReportedCost:
    """Extract Pi's reported per-message cost breakdown verbatim."""
    if cost_raw is None:
        return PiReportedCost(
            input_usd=None,
            output_usd=None,
            cache_read_usd=None,
            cache_write_usd=None,
            total_usd=None,
        )
    if not isinstance(cost_raw, dict):
        raise ParseError(
            f"Invalid message.usage.cost in {session_file_path} at line {line_number}: expected object or null."
        )
    return PiReportedCost(
        input_usd=_optional_float(cost_raw, "input", session_file_path, line_number, parent="message.usage.cost"),
        output_usd=_optional_float(cost_raw, "output", session_file_path, line_number, parent="message.usage.cost"),
        cache_read_usd=_optional_float(
            cost_raw, "cacheRead", session_file_path, line_number, parent="message.usage.cost"
        ),
        cache_write_usd=_optional_float(
            cost_raw, "cacheWrite", session_file_path, line_number, parent="message.usage.cost"
        ),
        total_usd=_optional_float(cost_raw, "total", session_file_path, line_number, parent="message.usage.cost"),
    )


def _optional_float(
    payload: dict[str, Any],
    field_name: str,
    session_file_path: Path,
    line_number: int,
    parent: str,
) -> float | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ParseError(
            f"Invalid {parent}.{field_name} in {session_file_path} at line {line_number}: expected number or null."
        )
    return float(value)
