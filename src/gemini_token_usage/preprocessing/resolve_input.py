"""Input path resolution helpers for Gemini preprocessing."""

from __future__ import annotations

from pathlib import Path

from .schemas import PreprocessInputResolution


def resolve_preprocess_input(log_file_path: Path) -> PreprocessInputResolution:
    """Resolve input path for preprocessing.

    Args:
        log_file_path: Input path that can be a directory, `.log`, or `.jsonl`.

    Returns:
        Resolved source log / JSONL paths.

    Raises:
        FileNotFoundError: If no expected file is found.
        ValueError: If the provided file type is unsupported.
    """
    if log_file_path.is_dir():
        source_log_file, jsonl_file = _resolve_from_directory(log_file_path)
        if source_log_file is not None:
            return PreprocessInputResolution(source_log_file=source_log_file, jsonl_file=None)
        if jsonl_file is not None:
            return PreprocessInputResolution(source_log_file=None, jsonl_file=jsonl_file)
        raise FileNotFoundError(
            (f"Could not find telemetry.log or telemetry.jsonl in {log_file_path} nor in its '.gemini' subdirectory.")
        )

    if not log_file_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {log_file_path}")

    if log_file_path.suffix == ".log":
        return PreprocessInputResolution(source_log_file=log_file_path, jsonl_file=None)
    if log_file_path.suffix == ".jsonl":
        source_log_candidate = log_file_path.with_name("telemetry.log")
        source_log_file = source_log_candidate if source_log_candidate.exists() else None
        return PreprocessInputResolution(source_log_file=source_log_file, jsonl_file=log_file_path)
    raise ValueError(f"Input file must be a directory, .log, or .jsonl path: {log_file_path}")


def resolve_jsonl_input(input_file_path: Path) -> Path:
    """Resolve an input path to a concrete JSONL file for simplification."""
    if input_file_path.is_dir():
        _, jsonl_file = _resolve_from_directory(input_file_path)
        if jsonl_file is not None:
            return jsonl_file
        raise FileNotFoundError(
            f"Could not find telemetry.jsonl in {input_file_path} nor in its '.gemini' subdirectory."
        )

    if input_file_path.suffix != ".jsonl":
        raise ValueError(f"Input file must be a .jsonl file, got: {input_file_path}")
    if not input_file_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_file_path}")
    return input_file_path


def _resolve_from_directory(directory: Path) -> tuple[Path | None, Path | None]:
    """Resolve source `.log` and `.jsonl` candidates from a directory."""
    source_log_file: Path | None = None
    jsonl_file: Path | None = None

    if (directory / "telemetry.log").exists():
        source_log_file = directory / "telemetry.log"
    elif (directory / ".gemini" / "telemetry.log").exists():
        source_log_file = directory / ".gemini" / "telemetry.log"
    elif (directory / "telemetry.jsonl").exists():
        jsonl_file = directory / "telemetry.jsonl"
    elif (directory / ".gemini" / "telemetry.jsonl").exists():
        jsonl_file = directory / ".gemini" / "telemetry.jsonl"

    return source_log_file, jsonl_file
