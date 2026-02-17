"""Tests for Gemini ingest CLI command."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import orjson
from typer.testing import CliRunner

from gemini_token_usage.cli import TYPER_APP


def test_ingest_command_registers_and_ingests_new_source(tmp_path: Path) -> None:
    """`ingest` should prompt for new source registration and ingest events."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(UUID("00000000-0000-0000-0000-000000000001")),
            _api_response("2026-02-17T00:00:00Z", "gemini-2.5-pro"),
        ],
    )
    database_path = tmp_path / "usage.duckdb"

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        ["ingest", str(jsonl_file), "--database-path", str(database_path)],
        input="y\n",
    )

    assert result.exit_code == 0
    assert "sources_scanned=1" in result.stdout
    assert "sources_ingested=1" in result.stdout
    assert "usage_rows_attempted_insert=1" in result.stdout


def test_ingest_command_exits_nonzero_when_confirmation_declined(tmp_path: Path) -> None:
    """Declining required source registration should exit non-zero."""
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(UUID("00000000-0000-0000-0000-000000000001")),
            _api_response("2026-02-17T00:00:00Z", "gemini-2.5-pro"),
        ],
    )
    database_path = tmp_path / "usage.duckdb"

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        ["ingest", str(jsonl_file), "--database-path", str(database_path)],
        input="n\n",
    )

    assert result.exit_code == 1
    assert "declined" in result.stdout.lower()


def test_ingest_command_reports_preprocess_suggestion_for_missing_jsonl(tmp_path: Path) -> None:
    """Non-preprocessed input should fail with exact preprocess suggestion."""
    log_file = tmp_path / "telemetry.log"
    log_file.write_text("{}", encoding="utf-8")
    database_path = tmp_path / "usage.duckdb"

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        ["ingest", str(log_file), "--database-path", str(database_path)],
    )

    assert result.exit_code == 2
    assert "Run: gemini-token-usage preprocess" in result.output


def _metadata(project_id: UUID) -> dict[str, object]:
    return {
        "record_type": "gemini_cli.project_metadata",
        "schema_version": 1,
        "project_id": str(project_id),
    }


def _api_response(timestamp: str, model_code: str) -> dict[str, object]:
    return {
        "attributes": {
            "event.name": "gemini_cli.api_response",
            "event.timestamp": timestamp,
            "model": model_code,
            "input_token_count": 10,
            "cached_content_token_count": 1,
            "output_token_count": 5,
            "thoughts_token_count": 1,
            "total_token_count": 17,
        }
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("wb") as handle:
        for row in rows:
            handle.write(orjson.dumps(row))
            handle.write(b"\n")
