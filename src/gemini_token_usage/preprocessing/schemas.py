"""Typed schemas used by Gemini preprocessing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PreprocessInputResolution:
    """Resolved log paths for preprocessing.

    Exactly one of `source_log_file` or `jsonl_file` is expected to be set.
    """

    source_log_file: Path | None
    jsonl_file: Path | None
