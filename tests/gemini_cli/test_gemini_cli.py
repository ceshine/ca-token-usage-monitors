"""Tests for Gemini Typer CLI entrypoints."""

from __future__ import annotations

from pathlib import Path
import json

import orjson
from typer.testing import CliRunner

from gemini_token_usage.cli import TYPER_APP


def test_preprocess_converts_log_directory_without_stats(tmp_path: Path) -> None:
    """`preprocess` should convert telemetry.log into telemetry.jsonl."""
    source_log = tmp_path / "telemetry.log"
    _write_concatenated_log(
        source_log,
        [
            {
                "attributes": {
                    "event.name": "gemini_cli.api_response",
                    "event.timestamp": "2026-02-17T00:00:00Z",
                    "duration_ms": 100,
                    "input_token_count": 100,
                    "output_token_count": 10,
                    "cached_content_token_count": 40,
                    "thoughts_token_count": 5,
                    "total_token_count": 115,
                    "tool_token_count": 0,
                    "model": "gemini-2.5-pro",
                    "session.id": "session-1",
                },
                "_body": {},
            }
        ],
    )

    runner = CliRunner()
    result = runner.invoke(TYPER_APP, ["preprocess", str(tmp_path)])

    assert result.exit_code == 0
    assert "Converted" in result.stdout
    assert (tmp_path / "telemetry.jsonl").exists()


def test_preprocess_with_stats_prints_statistics_tables(tmp_path: Path, monkeypatch) -> None:
    """`preprocess --stats` should render daily and overall stats tables."""
    jsonl_path = tmp_path / "telemetry.jsonl"
    with jsonl_path.open("wb") as handle:
        handle.write(
            orjson.dumps(
                {
                    "attributes": {
                        "event.name": "gemini_cli.api_response",
                        "event.timestamp": "2026-02-17T00:00:00Z",
                        "model": "gemini-2.5-pro",
                        "input_token_count": 100,
                        "output_token_count": 10,
                        "cached_content_token_count": 40,
                        "thoughts_token_count": 5,
                    }
                }
            )
            + b"\n"
        )

    monkeypatch.setattr(
        "gemini_token_usage.stats.service.get_price_spec",
        lambda: {
            "gemini-2.5-pro": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
            }
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        ["preprocess", str(jsonl_path), "--stats", "--timezone", "UTC"],
    )

    assert result.exit_code == 0
    assert "Daily Token Usage" in result.stdout
    assert "Daily Aggregated Costs" in result.stdout
    assert "Overall Token Usage by Model" in result.stdout


def test_stats_command_is_not_available() -> None:
    """The old `stats` command should be removed."""
    runner = CliRunner()
    result = runner.invoke(TYPER_APP, ["stats"])

    assert result.exit_code != 0
    assert "No such command" in result.output


def _write_concatenated_log(path: Path, rows: list[dict[str, object]]) -> None:
    """Write concatenated JSON objects (non-JSONL), matching telemetry.log style."""
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, indent=2))
            handle.write("\n")
