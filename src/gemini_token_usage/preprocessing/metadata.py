"""Metadata helpers for Gemini telemetry JSONL files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import orjson

PROJECT_METADATA_RECORD_TYPE = "gemini_cli.project_metadata"
PROJECT_METADATA_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ProjectMetadata:
    """Parsed project metadata from the first JSONL line."""

    project_id: UUID


def ensure_project_metadata_line(jsonl_path: Path) -> ProjectMetadata:
    """Ensure metadata exists as line 1 in a JSONL file.

    Existing valid metadata is preserved. When the first line is a normal event
    (no `record_type` key), a new metadata line is prepended. Malformed metadata
    fails fast.
    """
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {jsonl_path}")

    first_line = _read_first_line(jsonl_path)
    if first_line is None:
        metadata = ProjectMetadata(project_id=uuid4())
        _rewrite_with_metadata(jsonl_path, metadata)
        return metadata

    first_object = _parse_first_line_as_object(first_line, jsonl_path)
    if "record_type" in first_object:
        return _parse_metadata_object(first_object, jsonl_path)

    metadata = ProjectMetadata(project_id=uuid4())
    _rewrite_with_metadata(jsonl_path, metadata)
    return metadata


def read_project_metadata(jsonl_path: Path) -> ProjectMetadata:
    """Read and validate metadata from line 1."""
    first_line = _read_first_line(jsonl_path)
    if first_line is None:
        raise ValueError(f"Expected metadata line in {jsonl_path}, but the file is empty.")
    first_object = _parse_first_line_as_object(first_line, jsonl_path)
    return _parse_metadata_object(first_object, jsonl_path)


def build_metadata_line(metadata: ProjectMetadata) -> bytes:
    """Serialize metadata line as UTF-8 bytes without trailing newline."""
    return orjson.dumps(
        {
            "record_type": PROJECT_METADATA_RECORD_TYPE,
            "schema_version": PROJECT_METADATA_SCHEMA_VERSION,
            "project_id": str(metadata.project_id),
        }
    )


def _read_first_line(jsonl_path: Path) -> bytes | None:
    with jsonl_path.open("rb") as handle:
        first_line = handle.readline()
    return first_line if first_line else None


def _parse_first_line_as_object(first_line: bytes, jsonl_path: Path) -> dict[str, Any]:
    try:
        parsed = orjson.loads(first_line)
    except orjson.JSONDecodeError as exc:
        raise ValueError(f"Malformed JSON in first line of {jsonl_path}: {exc}.") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"First line in {jsonl_path} must be a JSON object.")
    return parsed


def _parse_metadata_object(parsed: dict[str, Any], jsonl_path: Path) -> ProjectMetadata:
    record_type = parsed.get("record_type")
    if record_type != PROJECT_METADATA_RECORD_TYPE:
        raise ValueError(
            (
                f"Malformed metadata in {jsonl_path}: expected record_type="
                f"{PROJECT_METADATA_RECORD_TYPE!r}, got {record_type!r}."
            )
        )

    schema_version = parsed.get("schema_version")
    if schema_version != PROJECT_METADATA_SCHEMA_VERSION:
        raise ValueError(
            (
                f"Malformed metadata in {jsonl_path}: expected schema_version="
                f"{PROJECT_METADATA_SCHEMA_VERSION}, got {schema_version!r}."
            )
        )

    project_id_value = parsed.get("project_id")
    if not isinstance(project_id_value, str):
        raise ValueError(
            f"Malformed metadata in {jsonl_path}: project_id must be a UUID string, got {type(project_id_value)}."
        )
    try:
        project_id = UUID(project_id_value)
    except ValueError as exc:
        raise ValueError(f"Malformed metadata in {jsonl_path}: invalid project_id {project_id_value!r}.") from exc

    return ProjectMetadata(project_id=project_id)


def _rewrite_with_metadata(jsonl_path: Path, metadata: ProjectMetadata) -> None:
    original_content = jsonl_path.read_bytes() if jsonl_path.exists() else b""
    temp_path = jsonl_path.with_suffix(".jsonl.tmp")
    if temp_path.exists():
        raise ValueError(f"Temp file already exists: {temp_path}. Clean it up manually and retry.")

    try:
        with temp_path.open("wb") as handle:
            handle.write(build_metadata_line(metadata))
            handle.write(b"\n")
            if original_content:
                handle.write(original_content)
        _ = temp_path.replace(jsonl_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
