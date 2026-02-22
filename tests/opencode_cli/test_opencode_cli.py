"""Tests for OpenCode Typer CLI entrypoints."""

from __future__ import annotations

from pathlib import Path

import duckdb
from typer.testing import CliRunner

from opencode_token_usage.cli import TYPER_APP


def test_stats_command_prints_provider_and_model_breakdown(tmp_path: Path, monkeypatch) -> None:
    """`stats` should show provider and model columns in usage breakdown tables."""
    database_path = tmp_path / "usage.duckdb"
    _create_stats_database(
        database_path,
        [
            {
                "provider_code": "opencode",
                "model_code": "gpt-5-free",
                "message_created_at": "2026-02-22T01:00:00+00:00",
                "message_completed_at": "2026-02-22T01:01:00+00:00",
                "input_tokens": 100,
                "cache_read_tokens": 20,
                "cache_write_tokens": 10,
                "output_tokens": 10,
                "reasoning_tokens": 3,
            },
            {
                "provider_code": "openrouter",
                "model_code": "qwen/qwen3-coder",
                "message_created_at": "2026-02-22T02:00:00+00:00",
                "message_completed_at": "2026-02-22T02:01:00+00:00",
                "input_tokens": 200,
                "cache_read_tokens": 40,
                "cache_write_tokens": 0,
                "output_tokens": 20,
                "reasoning_tokens": 5,
            },
        ],
    )

    monkeypatch.setattr(
        "opencode_token_usage.stats.service.get_price_spec",
        lambda: {
            "gpt-5": {
                "input_cost_per_token": 0.001,
                "output_cost_per_token": 0.002,
                "cache_read_input_token_cost": 0.0001,
                "cache_creation_input_token_cost": 0.00005,
            },
            "openrouter/qwen/qwen3-coder": {
                "input_cost_per_token": 0.003,
                "output_cost_per_token": 0.004,
                "cache_read_input_token_cost": 0.0003,
            },
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "stats",
            "--database-path",
            str(database_path),
            "--timezone",
            "UTC",
        ],
        terminal_width=220,
    )

    assert result.exit_code == 0
    assert "Daily Token Usage" in result.stdout
    assert "Overall Token Usage by Model" in result.stdout
    assert "0.674500" in result.stdout


def test_stats_command_since_filters_older_days(tmp_path: Path, monkeypatch) -> None:
    """`stats --since` should exclude usage before the given date."""
    database_path = tmp_path / "usage.duckdb"
    _create_stats_database(
        database_path,
        [
            {
                "provider_code": "opencode",
                "model_code": "gpt-5",
                "message_created_at": "2026-02-21T01:00:00+00:00",
                "message_completed_at": "2026-02-21T01:01:00+00:00",
                "input_tokens": 100,
                "cache_read_tokens": 20,
                "cache_write_tokens": 10,
                "output_tokens": 10,
                "reasoning_tokens": 3,
            },
            {
                "provider_code": "opencode",
                "model_code": "gpt-5",
                "message_created_at": "2026-02-22T01:00:00+00:00",
                "message_completed_at": "2026-02-22T01:01:00+00:00",
                "input_tokens": 100,
                "cache_read_tokens": 20,
                "cache_write_tokens": 10,
                "output_tokens": 10,
                "reasoning_tokens": 3,
            },
        ],
    )
    monkeypatch.setattr("opencode_token_usage.stats.service.get_price_spec", lambda: {})

    runner = CliRunner()
    result = runner.invoke(
        TYPER_APP,
        [
            "stats",
            "--database-path",
            str(database_path),
            "--timezone",
            "UTC",
            "--since",
            "2026-02-22",
        ],
        terminal_width=220,
    )

    assert result.exit_code == 0
    assert "2026-02-21" not in result.stdout
    assert "2026-02-22" in result.stdout


def _create_stats_database(database_path: Path, rows: list[dict[str, object]]) -> None:
    """Create a stats test database with an OpenCode usage table."""
    connection = duckdb.connect(str(database_path))
    try:
        _ = connection.execute(
            """
CREATE TABLE opencode_message_usage (
    provider_code VARCHAR,
    model_code VARCHAR,
    message_created_at TIMESTAMPTZ NOT NULL,
    message_completed_at TIMESTAMPTZ,
    input_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL,
    cache_write_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_tokens BIGINT NOT NULL
)
            """
        )
        if rows:
            _ = connection.executemany(
                """
INSERT INTO opencode_message_usage (
    provider_code,
    model_code,
    message_created_at,
    message_completed_at,
    input_tokens,
    cache_read_tokens,
    cache_write_tokens,
    output_tokens,
    reasoning_tokens
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    [
                        row["provider_code"],
                        row["model_code"],
                        row["message_created_at"],
                        row["message_completed_at"],
                        row["input_tokens"],
                        row["cache_read_tokens"],
                        row["cache_write_tokens"],
                        row["output_tokens"],
                        row["reasoning_tokens"],
                    ]
                    for row in rows
                ],
            )
    finally:
        connection.close()
