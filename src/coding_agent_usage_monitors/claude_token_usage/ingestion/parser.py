"""Parsing helpers for Claude Code session ingestion."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Iterator
from typing import Any

import orjson

from .errors import ParseError, SessionIdentityError
from .schemas import (
    ParsedSessionFile,
    SessionCheckpoint,
    SessionMetadataRow,
    TokenUsageValues,
    UsageEventRow,
)

LOGGER = logging.getLogger(__name__)

SYNTHETIC_MODEL = "<synthetic>"


def discover_session_roots() -> list[Path]:
    """Return all candidate root directories for Claude session files.

    Checks:
    - ``~/.claude/projects/``
    - ``~/.config/claude/projects/``
    - Paths from ``CLAUDE_CONFIG_DIR`` env var (comma-separated)
    """
    roots: list[Path] = []
    home = Path.home()

    for candidate in [
        home / ".claude" / "projects",
        home / ".config" / "claude" / "projects",
    ]:
        if candidate.is_dir():
            roots.append(candidate)

    env_dirs = os.environ.get("CLAUDE_CONFIG_DIR", "")
    if env_dirs:
        for raw_path in env_dirs.split(","):
            stripped = raw_path.strip()
            if stripped:
                candidate = Path(stripped) / "projects"
                if candidate.is_dir():
                    roots.append(candidate)

    return roots


def parse_session_identity(
    session_file_path: Path,
) -> tuple[str, str | None, str | None, str | None, str | None]:
    """Resolve session identity from the first entry with a sessionId.

    Args:
        session_file_path: Path to the JSONL session file.

    Returns:
        Tuple of (session_id, slug, cwd, version, agent_id). agent_id is
        non-None for subagent files (every entry carries a consistent agentId).

    Raises:
        SessionIdentityError: When session identity cannot be determined.
        ParseError: When JSON is malformed.
    """
    for _line_number, event in _iter_json_events(session_file_path):
        session_id = event.get("sessionId")
        if session_id is not None and isinstance(session_id, str) and session_id:
            slug = event.get("slug")
            cwd = event.get("cwd")
            version = event.get("version")
            agent_id = event.get("agentId")
            return (
                session_id,
                slug if isinstance(slug, str) else None,
                cwd if isinstance(cwd, str) else None,
                version if isinstance(version, str) else None,
                agent_id if isinstance(agent_id, str) else None,
            )

    raise SessionIdentityError(f"No entry with sessionId found in {session_file_path}.")


def parse_session_file(
    session_file_path: Path,
    session_id: str,
    checkpoint: SessionCheckpoint | None,
    project_name: str | None = None,
    slug: str | None = None,
    cwd: str | None = None,
    version: str | None = None,
) -> ParsedSessionFile:
    """Parse one session file and extract usage event rows filtered by optional checkpoint.

    Args:
        session_file_path: Path to the JSONL session file.
        session_id: Resolved session ID string.
        checkpoint: Optional checkpoint for tail filtering.
        project_name: Project name derived from directory path.
        slug: Human-readable session name.
        cwd: Working directory from session.
        version: Claude Code version from session.

    Returns:
        ParsedSessionFile with metadata and extracted usage rows.

    Raises:
        ParseError: When entry data is malformed (including missing message_id or request_id).
    """
    seen_rows: dict[tuple[str, str], UsageEventRow] = {}

    usage_rows_raw = 0
    usage_rows_skipped_synthetic = 0
    usage_rows_skipped_before_checkpoint = 0
    duplicate_rows_skipped = 0

    for line_number, event in _iter_json_events(session_file_path):
        if event.get("type") != "assistant":
            continue

        message = event.get("message")
        if not isinstance(message, dict):
            continue

        usage_raw = message.get("usage")
        if not isinstance(usage_raw, dict):
            continue

        model = message.get("model")
        if not isinstance(model, str) or not model:
            continue

        if model == SYNTHETIC_MODEL:
            usage_rows_skipped_synthetic += 1
            continue

        input_tokens = usage_raw.get("input_tokens")
        output_tokens = usage_raw.get("output_tokens")
        if not isinstance(input_tokens, int) or not isinstance(output_tokens, int):
            continue

        # Require message_id and request_id
        message_id = message.get("id")
        request_id = event.get("requestId")
        if not isinstance(message_id, str) or not isinstance(request_id, str):
            raise ParseError(f"Missing message.id or requestId in {session_file_path} at line {line_number}.")

        usage_rows_raw += 1

        # Derive model code with speed suffix
        speed = event.get("speed")
        model_code = f"{model}-fast" if speed == "fast" else model

        # Parse timestamp
        event_timestamp = _parse_required_timestamp(event.get("timestamp"), session_file_path, line_number, "timestamp")

        # Checkpoint filter
        if checkpoint is not None and not _passes_checkpoint(event_timestamp, message_id, request_id, checkpoint):
            usage_rows_skipped_before_checkpoint += 1
            continue

        # Extract optional cache fields
        cache_creation = usage_raw.get("cache_creation_input_tokens", 0)
        cache_read = usage_raw.get("cache_read_input_tokens", 0)
        if not isinstance(cache_creation, int):
            cache_creation = 0
        if not isinstance(cache_read, int):
            cache_read = 0

        usage = TokenUsageValues(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation,
            cache_read_input_tokens=cache_read,
        )

        # Sidechain / agent fields
        is_sidechain = bool(event.get("isSidechain"))
        agent_id = event.get("agentId")
        if not isinstance(agent_id, str):
            agent_id = None

        # In-memory dedup by (message_id, request_id).
        # Claude Code emits intermediate streaming entries (stop_reason=null) followed by
        # a final completed entry (stop_reason is not null) for the same request. The final
        # entry carries the authoritative token counts, so we replace the stored row when
        # the new entry has a non-null stop_reason. Intermediates with matching tokens are
        # silently skipped; intermediates with conflicting tokens are also skipped because
        # a subsequent final entry is expected to supersede them.
        key = (message_id, request_id)
        stop_reason = message.get("stop_reason")
        if key in seen_rows:
            existing_row = seen_rows[key]
            if existing_row.usage == usage:
                duplicate_rows_skipped += 1
                continue
            if stop_reason is not None:
                # Final entry supersedes the earlier streaming partial.
                seen_rows[key] = UsageEventRow(
                    session_id=session_id,
                    message_id=message_id,
                    request_id=request_id,
                    event_timestamp=event_timestamp,
                    event_line_number=line_number,
                    model_code=model_code,
                    is_sidechain=is_sidechain,
                    agent_id=agent_id,
                    usage=usage,
                )
            else:
                # Intermediate entry with conflicting tokens — a final entry is expected later.
                duplicate_rows_skipped += 1
            continue

        seen_rows[key] = UsageEventRow(
            session_id=session_id,
            message_id=message_id,
            request_id=request_id,
            event_timestamp=event_timestamp,
            event_line_number=line_number,
            model_code=model_code,
            is_sidechain=is_sidechain,
            agent_id=agent_id,
            usage=usage,
        )

    usage_rows = list(seen_rows.values())

    metadata = SessionMetadataRow(
        session_id=session_id,
        project_name=project_name,
        slug=slug,
        cwd=cwd,
        version=version,
        session_file_path=str(session_file_path),
    )

    LOGGER.debug(
        "Parsed session file %s: %d candidate rows, %d synthetic skipped, %d checkpoint-skipped, %d duplicates",
        session_file_path,
        usage_rows_raw,
        usage_rows_skipped_synthetic,
        usage_rows_skipped_before_checkpoint,
        duplicate_rows_skipped,
    )

    return ParsedSessionFile(
        metadata=metadata,
        usage_rows=usage_rows,
        usage_rows_raw=usage_rows_raw,
        usage_rows_skipped_synthetic=usage_rows_skipped_synthetic,
        usage_rows_skipped_before_checkpoint=usage_rows_skipped_before_checkpoint,
        duplicate_rows_skipped=duplicate_rows_skipped,
    )


def derive_project_name(session_file_path: Path, roots: list[Path]) -> str | None:
    """Derive project name from the directory path relative to a session root.

    Args:
        session_file_path: Absolute path to the session file.
        roots: List of session root directories.

    Returns:
        The first path component after the root, or None if not derivable.
    """
    for root in roots:
        try:
            relative = session_file_path.relative_to(root)
        except ValueError:
            continue
        parts = relative.parts
        if parts:
            return parts[0]
    return None


def _iter_json_events(session_file_path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield parsed JSON events with line numbers.

    Args:
        session_file_path: Path to the JSONL file.

    Yields:
        Tuple of (line_number, parsed_dict) for each valid JSON line.

    Raises:
        ParseError: When a line contains malformed JSON.
    """
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


def _passes_checkpoint(
    event_timestamp: datetime,
    message_id: str,
    request_id: str,
    checkpoint: SessionCheckpoint,
) -> bool:
    """Return True when a usage row is in the ingestion tail window.

    Args:
        event_timestamp: Timestamp of the current event.
        message_id: Message ID of the current event.
        request_id: Request ID of the current event.
        checkpoint: The checkpoint to filter against.

    Returns:
        True if the event should be included.
    """
    if event_timestamp > checkpoint.last_ts:
        return True
    if event_timestamp < checkpoint.last_ts:
        return False
    # Same timestamp: compare (message_id, request_id) tuple
    return (message_id, request_id) >= (checkpoint.last_message_id, checkpoint.last_request_id)


def _parse_required_timestamp(
    value: Any,
    session_file_path: Path,
    line_number: int,
    field_name: str,
) -> datetime:
    """Parse a required timestamp into an aware datetime.

    Args:
        value: Raw timestamp value from JSON.
        session_file_path: File path for error context.
        line_number: Line number for error context.
        field_name: Field name for error context.

    Returns:
        An aware datetime object.

    Raises:
        ParseError: When the timestamp is missing or malformed.
    """
    if value is None:
        raise ParseError(f"Missing required {field_name} in {session_file_path} at line {line_number}.")
    if not isinstance(value, str):
        raise ParseError(
            f"Invalid {field_name} in {session_file_path} at line {line_number}: expected str, got {type(value).__name__}."
        )

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ParseError(f"Invalid timestamp '{value}' in {session_file_path} at line {line_number}.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed
