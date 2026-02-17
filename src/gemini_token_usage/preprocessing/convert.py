"""Conversion helpers for Gemini raw telemetry logs."""

from __future__ import annotations

from datetime import datetime
import logging
from os import SEEK_END
from pathlib import Path
import shutil
from uuid import uuid4

import orjson

from .metadata import ProjectMetadata, build_metadata_line, ensure_project_metadata_line
from .simplify import simplify_record

LOGGER = logging.getLogger(__name__)


def get_last_timestamp(file_path: Path) -> str | None:
    """Read the last event timestamp from an existing JSONL file."""
    if not file_path.exists():
        return None

    try:
        with file_path.open("rb") as handle:
            _ = handle.seek(0, SEEK_END)
            position = handle.tell()
            remainder = b""
            chunk_size = 64 * 1024

            while position > 0:
                read_len = min(chunk_size, position)
                position -= read_len
                _ = handle.seek(position)
                chunk = handle.read(read_len)

                if b"\n" in chunk:
                    parts = chunk.split(b"\n")

                    last_line = parts[-1] + remainder
                    if last_line.strip():
                        timestamp = _extract_timestamp(last_line)
                        if timestamp is not None:
                            return timestamp

                    for line in reversed(parts[1:-1]):
                        if not line.strip():
                            continue
                        timestamp = _extract_timestamp(line)
                        if timestamp is not None:
                            return timestamp

                    remainder = parts[0]
                else:
                    remainder = chunk + remainder

            if remainder.strip():
                return _extract_timestamp(remainder)
    except Exception as exc:
        raise ValueError(f"Failed to read last timestamp from {file_path}: {exc}") from exc

    return None


def convert_log_file(
    input_file_path: Path,
    output_file_path: Path,
    last_timestamp: str | None = None,
    simplify_level: int = 0,
) -> tuple[int, int]:
    """Convert concatenated JSON objects from `.log` to newline-delimited `.jsonl`."""
    converted_count = 0
    skipped_count = 0

    with (
        input_file_path.open("r", encoding="utf-8") as input_handle,
        output_file_path.open("ab") as output_handle,
    ):
        buffer = ""
        for line in input_handle:
            buffer += line
            if line.strip() != "}":
                continue

            try:
                obj = orjson.loads(buffer)
            except orjson.JSONDecodeError:
                continue
            buffer = ""

            if not isinstance(obj, dict):
                continue

            attributes = obj.get("attributes")
            if not isinstance(attributes, dict):
                continue

            current_timestamp = attributes.get("event.timestamp")
            if not isinstance(current_timestamp, str) or not current_timestamp:
                continue

            if last_timestamp is not None and current_timestamp <= last_timestamp:
                skipped_count += 1
                continue

            simplified = simplify_record(obj, simplify_level)
            if simplified is None:
                skipped_count += 1
                continue

            output_handle.write(orjson.dumps(simplified))
            output_handle.write(b"\n")
            converted_count += 1

        if buffer.strip():
            LOGGER.warning("End of file reached with incomplete JSON data in buffer for %s.", input_file_path)

    return converted_count, skipped_count


def run_log_conversion(
    input_file_path: Path,
    output_file_path: Path | None = None,
    simplify_level: int = 0,
    archiving_enabled: bool = False,
    archive_folder_path: Path = Path("/tmp"),
) -> Path:
    """Convert raw Gemini log file to JSONL and optionally archive the source file."""
    if not input_file_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file_path}")

    destination_path = output_file_path or input_file_path.with_suffix(".jsonl")
    output_exists = destination_path.exists()
    if output_exists:
        _ = ensure_project_metadata_line(destination_path)
        last_timestamp = get_last_timestamp(destination_path)
        if last_timestamp:
            LOGGER.info("Found existing output. Appending entries after %s.", last_timestamp)
        else:
            LOGGER.info("Found existing output with no timestamped events; appending from start.")
    else:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        _initialize_output_file(destination_path)
        last_timestamp = None
        LOGGER.info("Starting fresh conversion for %s.", input_file_path)

    converted_count, skipped_count = convert_log_file(
        input_file_path=input_file_path,
        output_file_path=destination_path,
        last_timestamp=last_timestamp,
        simplify_level=simplify_level,
    )
    LOGGER.info(
        "Converted %d records to %s (skipped %d).",
        converted_count,
        destination_path,
        skipped_count,
    )

    if archiving_enabled:
        archive_folder_path.mkdir(exist_ok=True, parents=True)
        archived_name = f"{input_file_path.stem}.{int(datetime.now().timestamp())}{input_file_path.suffix}"
        archived_path = archive_folder_path / archived_name
        _ = shutil.move(str(input_file_path), str(archived_path))
        LOGGER.info("Archived %s to %s", input_file_path, archived_path)

    return destination_path


def _extract_timestamp(raw_line: bytes) -> str | None:
    """Extract `attributes.event.timestamp` from a JSON line payload."""
    try:
        decoded = orjson.loads(raw_line)
    except orjson.JSONDecodeError:
        LOGGER.warning("Invalid JSON string encountered while scanning from tail.")
        return None

    if not isinstance(decoded, dict):
        return None
    attributes = decoded.get("attributes")
    if not isinstance(attributes, dict):
        return None
    timestamp = attributes.get("event.timestamp")
    return timestamp if isinstance(timestamp, str) else None


def _initialize_output_file(output_file_path: Path) -> None:
    metadata = ProjectMetadata(project_id=uuid4())
    with output_file_path.open("wb") as handle:
        handle.write(build_metadata_line(metadata))
        handle.write(b"\n")
