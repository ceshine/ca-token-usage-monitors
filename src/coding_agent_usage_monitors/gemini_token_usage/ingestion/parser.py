"""Parsing helpers for Gemini JSONL ingestion."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import orjson

from ..preprocessing.metadata import read_project_metadata

from .errors import AppendOnlyViolationError, DuplicateEventError, MetadataValidationError, ParseError
from .schemas import ParsedJsonlFile, SourceCheckpoint, UsageEventRow


def parse_usage_jsonl(
    jsonl_file_path: Path,
    expected_project_id: UUID,
    checkpoint: SourceCheckpoint | None,
) -> ParsedJsonlFile:
    """Parse one preprocessed telemetry JSONL file for usage ingestion."""
    metadata = _read_validated_metadata(jsonl_file_path, expected_project_id)

    usage_rows: list[UsageEventRow] = []
    usage_events_total = 0
    usage_events_skipped_before_checkpoint = 0
    max_event_key: tuple[datetime, str] | None = None

    seen_event_keys: dict[tuple[datetime, str], int] = {}
    with jsonl_file_path.open("rb") as handle:
        _ = handle.readline()  # Skip metadata line; already validated.
        for line_number, raw_line in enumerate(handle, start=2):
            if not raw_line.strip():
                continue

            payload = _parse_json_object(raw_line, jsonl_file_path, line_number)
            attributes = payload.get("attributes")
            if not isinstance(attributes, dict):
                continue
            if attributes.get("event.name") != "gemini_cli.api_response":
                continue

            usage_events_total += 1
            event_timestamp = _parse_required_timestamp(
                attributes.get("event.timestamp"),
                jsonl_file_path,
                line_number,
                "attributes.event.timestamp",
            )
            model_code = _parse_required_model_code(
                attributes.get("model"),
                jsonl_file_path,
                line_number,
            )
            event_key = (event_timestamp, model_code)

            prior_line_number = seen_event_keys.get(event_key)
            if prior_line_number is not None:
                raise DuplicateEventError(
                    (
                        f"Duplicate usage event key {(event_timestamp.isoformat(), model_code)} in {jsonl_file_path}: "
                        f"line {prior_line_number} and line {line_number}."
                    )
                )
            seen_event_keys[event_key] = line_number

            if max_event_key is None or event_key > max_event_key:
                max_event_key = event_key

            if checkpoint is not None and not _passes_checkpoint(event_timestamp, model_code, checkpoint):
                usage_events_skipped_before_checkpoint += 1
                continue

            usage_rows.append(
                UsageEventRow(
                    project_id=metadata.project_id,
                    event_timestamp=event_timestamp,
                    model_code=model_code,
                    input_tokens=_parse_required_int(
                        attributes.get("input_token_count"),
                        jsonl_file_path,
                        line_number,
                        "attributes.input_token_count",
                    ),
                    cached_input_tokens=_parse_required_int(
                        attributes.get("cached_content_token_count"),
                        jsonl_file_path,
                        line_number,
                        "attributes.cached_content_token_count",
                    ),
                    output_tokens=_parse_required_int(
                        attributes.get("output_token_count"),
                        jsonl_file_path,
                        line_number,
                        "attributes.output_token_count",
                    ),
                    thoughts_tokens=_parse_required_int(
                        attributes.get("thoughts_token_count"),
                        jsonl_file_path,
                        line_number,
                        "attributes.thoughts_token_count",
                    ),
                    total_tokens=_parse_required_int(
                        attributes.get("total_token_count"),
                        jsonl_file_path,
                        line_number,
                        "attributes.total_token_count",
                    ),
                )
            )

    if checkpoint is not None:
        checkpoint_key = (checkpoint.last_event_timestamp, checkpoint.last_model_code)
        if max_event_key is None or max_event_key < checkpoint_key:
            raise AppendOnlyViolationError(
                (
                    "Detected non-append-only rewrite for "
                    f"{jsonl_file_path}: max event key {max_event_key} is behind checkpoint {checkpoint_key}."
                )
            )

    return ParsedJsonlFile(
        project_id=metadata.project_id,
        usage_rows=usage_rows,
        usage_events_total=usage_events_total,
        usage_events_skipped_before_checkpoint=usage_events_skipped_before_checkpoint,
        max_event_key=max_event_key,
    )


def _read_validated_metadata(jsonl_file_path: Path, expected_project_id: UUID):
    try:
        metadata = read_project_metadata(jsonl_file_path)
    except (FileNotFoundError, ValueError) as exc:
        raise MetadataValidationError(str(exc)) from exc
    if metadata.project_id != expected_project_id:
        raise MetadataValidationError(
            (
                f"Metadata project_id mismatch for {jsonl_file_path}: expected {expected_project_id}, "
                f"got {metadata.project_id}."
            )
        )
    return metadata


def _parse_json_object(raw_line: bytes, jsonl_file_path: Path, line_number: int) -> dict[str, Any]:
    try:
        parsed = orjson.loads(raw_line)
    except orjson.JSONDecodeError as exc:
        raise ParseError(f"Malformed JSON in {jsonl_file_path} at line {line_number}: {exc}.") from exc
    if not isinstance(parsed, dict):
        raise ParseError(
            f"Expected JSON object in {jsonl_file_path} at line {line_number}, got {type(parsed).__name__}."
        )
    return parsed


def _parse_required_timestamp(
    value: Any,
    jsonl_file_path: Path,
    line_number: int,
    field_name: str,
) -> datetime:
    if not isinstance(value, str) or not value:
        raise ParseError(
            f"Missing or invalid {field_name} in {jsonl_file_path} at line {line_number}: expected non-empty string."
        )
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ParseError(f"Invalid timestamp {value!r} in {jsonl_file_path} at line {line_number}.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_required_model_code(value: Any, jsonl_file_path: Path, line_number: int) -> str:
    if not isinstance(value, str) or not value:
        raise ParseError(
            (
                f"Missing or invalid attributes.model in {jsonl_file_path} at line {line_number}: "
                "expected non-empty string."
            )
        )
    return value


def _parse_required_int(value: Any, jsonl_file_path: Path, line_number: int, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ParseError(
            f"Invalid {field_name} in {jsonl_file_path} at line {line_number}: expected int, got {type(value)}."
        )
    return value


def _passes_checkpoint(
    event_timestamp: datetime,
    model_code: str,
    checkpoint: SourceCheckpoint,
) -> bool:
    if event_timestamp > checkpoint.last_event_timestamp:
        return True
    return event_timestamp == checkpoint.last_event_timestamp and model_code >= checkpoint.last_model_code
