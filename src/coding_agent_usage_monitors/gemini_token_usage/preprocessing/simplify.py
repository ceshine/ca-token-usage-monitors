"""Log record simplification helpers for Gemini telemetry JSONL files."""

from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
import shutil
from typing import Any

import orjson
import orjsonl

from .metadata import ensure_project_metadata_line
from .resolve_input import resolve_jsonl_input

LOGGER = logging.getLogger(__name__)


def simplify_record(record: dict[str, Any], level: int, line_number: int | None = None) -> dict[str, Any] | None:
    """Simplify one telemetry record by level.

    Args:
        record: Original telemetry record.
        level: Simplification level (0-3).
        line_number: Source line number in JSONL input, when available.

    Returns:
        Simplified record or `None` when filtered out.

    Raises:
        ValueError: If `level` is outside `[0, 3]`.
    """
    record_type = record.get("record_type")
    if record_type == "gemini_cli.project_metadata" and (line_number is None or line_number > 1):
        # Note: assuming not at the first line when line_number is None
        raise ValueError("Metadata record is only allowed at line 1.")
    if record_type == "gemini_cli.project_metadata":
        return record

    if level == 0:
        return record
    if level < 0 or level > 3:
        raise ValueError("Level must be between 0 and 3.")

    attributes = record.get("attributes")
    if not isinstance(attributes, dict):
        LOGGER.warning("Skipping record with invalid `attributes`: %s", orjson.dumps(record).decode())
        return None

    event_name = attributes.get("event.name")
    if not isinstance(event_name, str):
        LOGGER.warning("Skipping record with missing `attributes.event.name`: %s", orjson.dumps(record).decode())
        return None

    try:
        if level >= 3:
            if event_name != "gemini_cli.api_response":
                return None
            record["attributes"] = {
                "event.timestamp": attributes["event.timestamp"],
                "duration_ms": attributes["duration_ms"],
                "input_token_count": attributes["input_token_count"],
                "output_token_count": attributes["output_token_count"],
                "cached_content_token_count": attributes["cached_content_token_count"],
                "thoughts_token_count": attributes["thoughts_token_count"],
                "total_token_count": attributes["total_token_count"],
                "tool_token_count": attributes["tool_token_count"],
                "model": attributes["model"],
                "session.id": attributes["session.id"],
                "event.name": attributes["event.name"],
            }

        if 1 <= level < 3 and event_name not in ("gemini_cli.api_response", "gemini_cli.api_request"):
            return None

        if level >= 2:
            record = {"attributes": record["attributes"], "_body": record["_body"]}
    except KeyError:
        LOGGER.warning("Skipping record with unexpected structure: %s", record)
        return None

    return record


def run_log_simplification(
    input_file_path: Path,
    level: int,
    archive_folder: Path = Path("/tmp"),
    disable_archiving: bool = False,
) -> Path:
    """Simplify an existing JSONL file in-place.

    Args:
        input_file_path: File or directory path. Directory mode resolves `telemetry.jsonl`.
        level: Simplification level (0-3).
        archive_folder: Archive destination for the original file.
        disable_archiving: When true, remove the original file instead of archiving.

    Returns:
        Simplified JSONL file path.

    Raises:
        FileNotFoundError: If the input file cannot be resolved.
        ValueError: If inputs are invalid.
        RuntimeError: If simplification fails unexpectedly.
    """
    if level < 0 or level > 3:
        raise ValueError("Level must be between 0 and 3.")

    jsonl_path = resolve_jsonl_input(input_file_path)
    _ = ensure_project_metadata_line(jsonl_path)
    if level == 0:
        LOGGER.warning("Level 0 is a no-op.")
        return jsonl_path

    temp_file = jsonl_path.with_suffix(".jsonl.tmp")
    if temp_file.exists():
        raise ValueError(f"Temp file already exists: {temp_file}. Clean it up manually if left by a prior failed run.")

    archive_file_path: Path | None = None
    if not disable_archiving:
        archive_target = f"{jsonl_path.stem}.{int(datetime.now().timestamp())}{jsonl_path.suffix}"
        archive_file_path = archive_folder / archive_target
        if archive_file_path.exists():
            raise ValueError(f"Archive file already exists: {archive_file_path}. Clean it up manually and retry.")

    try:
        with temp_file.open("wb") as output_handle:
            for line_number, obj in enumerate(orjsonl.stream(jsonl_path), start=1):
                if not isinstance(obj, dict):
                    LOGGER.warning("Found malformed record in %s at line %d. Skipping.", jsonl_path, line_number)
                    continue
                simplified_obj = simplify_record(obj, level=level, line_number=line_number)
                if simplified_obj is None:
                    continue
                output_handle.write(orjson.dumps(simplified_obj))
                output_handle.write(b"\n")

        if not disable_archiving:
            assert archive_file_path is not None
            archive_folder.mkdir(parents=True, exist_ok=True)
            _ = shutil.move(str(jsonl_path), str(archive_file_path))
            LOGGER.info("Archived %s to %s", jsonl_path, archive_file_path)
        else:
            jsonl_path.unlink()
            LOGGER.info("Removed %s", jsonl_path)

        _ = temp_file.rename(jsonl_path)
        LOGGER.info("%s simplified at level %d", jsonl_path, level)
        return jsonl_path
    except ValueError:
        if temp_file.exists():
            temp_file.unlink(missing_ok=True)
        raise
    except Exception as exc:
        if temp_file.exists():
            temp_file.unlink(missing_ok=True)
        raise RuntimeError(f"Unexpected error during log simplification: {exc}") from exc
