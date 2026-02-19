"""Tests for Gemini ingest CLI command."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import orjson
from typer.testing import CliRunner

from gemini_token_usage.cli import TYPER_APP
from gemini_token_usage.ingestion.repository import IngestionRepository


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
    assert "Statistics (last 7 days):" in result.stdout
    assert "Daily Token Usage" in result.stdout


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


def test_ingest_command_preprocesses_selected_paths_before_ingestion(tmp_path: Path) -> None:
    """`ingest` should preprocess telemetry.log inputs before ingestion."""
    log_file = tmp_path / "telemetry.log"
    _write_concatenated_log(
        log_file,
        [
            _api_response("2026-02-17T00:00:00Z", "gemini-2.5-pro"),
            _api_response("2010-01-01T00:00:00Z", "gemini-2.5-pro"),
        ],
    )
    database_path = tmp_path / "usage.duckdb"

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        ["ingest", str(log_file), "--database-path", str(database_path)],
        input="y\n",
    )

    assert result.exit_code == 0
    assert "Converted" in result.stdout
    assert "sources_ingested=1" in result.stdout
    assert "Statistics (last 7 days):" in result.stdout
    assert "2010-01-01" not in result.stdout


def test_ingest_command_preprocesses_all_active_paths_before_ingestion(tmp_path: Path) -> None:
    """`ingest --all-active` should preprocess active source paths before ingestion."""
    project_id = UUID("00000000-0000-0000-0000-000000000001")
    jsonl_file = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            _metadata(project_id),
            _api_response("2026-02-17T00:00:00Z", "gemini-2.5-pro"),
        ],
    )
    database_path = tmp_path / "usage.duckdb"
    repository = IngestionRepository(database_path)
    try:
        repository.ensure_schema()
        repository.insert_source(project_id=project_id, jsonl_file_path=str(jsonl_file), active=True)
    finally:
        repository.close()

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        ["ingest", "--all-active", "--database-path", str(database_path)],
    )

    assert result.exit_code == 0
    assert str(jsonl_file) in result.stdout
    assert "as the JSONL log file" in result.stdout
    assert "sources_scanned=1" in result.stdout
    assert "sources_ingested=1" in result.stdout


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


def _write_concatenated_log(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, indent=2))
            handle.write("\n")
