"""Unit tests for Gemini stats service and pricing logic."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import orjson
import pytest

from gemini_token_usage.stats.schemas import TokenUsageEvent
from gemini_token_usage.stats.service import StatsService, calculate_event_cost


def test_calculate_event_cost_uses_above_200k_tier() -> None:
    """Cost calculation should use the above-200k tier when input exceeds threshold."""
    event = TokenUsageEvent(
        model_code="gemini-2.5-pro",
        event_timestamp=datetime(2026, 2, 17, 0, 0, tzinfo=UTC),
        input_tokens=250000,
        cached_input_tokens=50000,
        output_tokens=1000,
        thoughts_tokens=500,
    )
    price_spec = {
        "gemini-2.5-pro": {
            "input_cost_per_token": 1.0,
            "output_cost_per_token": 2.0,
            "cache_read_input_token_cost": 0.5,
            "input_cost_per_token_above_200k_tokens": 3.0,
            "output_cost_per_token_above_200k_tokens": 4.0,
            "cache_read_input_token_cost_above_200k_tokens": 1.5,
        }
    }

    cost = calculate_event_cost(event, price_spec)
    expected = (200000 * 3.0) + (1500 * 4.0) + (50000 * 1.5)

    assert cost == expected


def test_collect_daily_statistics_aggregates_usage_and_cost(tmp_path: Path) -> None:
    """Stats service should aggregate daily and overall values from JSONL input."""
    log_file_path = tmp_path / "telemetry.jsonl"
    _write_jsonl(
        log_file_path,
        [
            _api_response(
                timestamp="2026-02-17T00:00:00Z",
                model="gemini-2.5-pro",
                input_tokens=100,
                output_tokens=10,
                cached_tokens=40,
                thoughts_tokens=5,
            ),
            _api_response(
                timestamp="2026-02-17T01:00:00Z",
                model="gemini-2.5-pro",
                input_tokens=50,
                output_tokens=20,
                cached_tokens=0,
                thoughts_tokens=0,
            ),
            {
                "attributes": {
                    "event.name": "gemini_cli.api_request",
                    "event.timestamp": "2026-02-17T02:00:00Z",
                }
            },
        ],
    )
    service = StatsService(
        price_spec={
            "gemini-2.5-pro": {
                "input_cost_per_token": 1.0,
                "output_cost_per_token": 2.0,
                "cache_read_input_token_cost": 0.5,
            }
        }
    )

    report = service.collect_daily_statistics(log_file_path=log_file_path)

    assert report.total_events == 2
    stats = report.overall_usage["gemini-2.5-pro"]
    assert stats.input_tokens == 150
    assert stats.output_tokens == 30
    assert stats.cached_tokens == 40
    assert stats.thoughts_tokens == 5
    assert stats.count == 2
    assert stats.cost == pytest.approx(200.0)


def _api_response(
    timestamp: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int,
    thoughts_tokens: int,
) -> dict[str, object]:
    """Build a simplified gemini_cli.api_response event."""
    return {
        "attributes": {
            "event.name": "gemini_cli.api_response",
            "event.timestamp": timestamp,
            "model": model,
            "input_token_count": input_tokens,
            "output_token_count": output_tokens,
            "cached_content_token_count": cached_tokens,
            "thoughts_token_count": thoughts_tokens,
        }
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    """Write JSONL rows to disk."""
    with path.open("wb") as handle:
        for row in rows:
            handle.write(orjson.dumps(row))
            handle.write(b"\n")
