"""Typed schemas used by Gemini preprocessing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PreprocessInputResolution:
    """Resolved log paths for preprocessing.

    `source_log_file` may be set alongside `jsonl_file` when resolving from a
    JSONL input path when a sibling `telemetry.log` exists.
    """

    source_log_file: Path | None
    jsonl_file: Path | None
