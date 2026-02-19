"""Tests for Gemini preprocessing input resolution helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from gemini_token_usage.preprocessing.resolve_input import resolve_jsonl_input, resolve_preprocess_input


def test_resolve_preprocess_input_prefers_raw_log_for_directory(tmp_path: Path) -> None:
    """Directory resolution should prefer telemetry.log over telemetry.jsonl."""
    source_log = tmp_path / "telemetry.log"
    source_log.write_text("{}", encoding="utf-8")
    jsonl_file = tmp_path / "telemetry.jsonl"
    jsonl_file.write_text("{}", encoding="utf-8")

    resolved = resolve_preprocess_input(tmp_path)

    assert resolved.source_log_file == source_log
    assert resolved.jsonl_file is None


def test_resolve_jsonl_input_uses_dot_gemini_fallback(tmp_path: Path) -> None:
    """Directory resolution for simplification should fall back to .gemini/telemetry.jsonl."""
    dot_gemini_dir = tmp_path / ".gemini"
    dot_gemini_dir.mkdir()
    dot_gemini_jsonl = dot_gemini_dir / "telemetry.jsonl"
    dot_gemini_jsonl.write_text("{}", encoding="utf-8")

    resolved = resolve_jsonl_input(tmp_path)

    assert resolved == dot_gemini_jsonl


def test_resolve_jsonl_input_rejects_non_jsonl_file(tmp_path: Path) -> None:
    """Non-JSONL file paths should be rejected."""
    text_file = tmp_path / "telemetry.txt"
    text_file.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match=r"\.jsonl"):
        _ = resolve_jsonl_input(text_file)


def test_resolve_preprocess_input_sets_source_log_for_jsonl_path_when_present(tmp_path: Path) -> None:
    """JSONL file resolution should also expose sibling telemetry.log source path."""
    source_log = tmp_path / "telemetry.log"
    source_log.write_text("{}", encoding="utf-8")
    jsonl_file = tmp_path / "telemetry.jsonl"
    jsonl_file.write_text("{}", encoding="utf-8")

    resolved = resolve_preprocess_input(jsonl_file)

    assert resolved.source_log_file == source_log
    assert resolved.jsonl_file == jsonl_file


def test_resolve_preprocess_input_leaves_source_log_unset_for_jsonl_path_when_missing(tmp_path: Path) -> None:
    """JSONL file resolution should not set source_log_file when telemetry.log is absent."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    jsonl_file.write_text("{}", encoding="utf-8")

    resolved = resolve_preprocess_input(jsonl_file)

    assert resolved.source_log_file is None
    assert resolved.jsonl_file == jsonl_file
