"""Tests for dedupe and integrity validation."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from codex_token_usage.ingestion.dedupe import dedupe_and_validate_token_rows
from codex_token_usage.ingestion.errors import DeltaConsistencyError, DuplicateConflictError, MonotonicityError
from codex_token_usage.ingestion.schemas import TokenEventRow, TokenUsageValues


def test_dedupe_keeps_first_duplicate_even_when_last_usage_differs() -> None:
    """Duplicates with matching total_usage should keep first row only."""
    rows = [
        _row(line=1, total=10, last=10),
        _row(line=2, total=10, last=0),
        _row(line=3, total=18, last=8),
    ]

    result = dedupe_and_validate_token_rows(Path("session.jsonl"), rows)

    assert [row.event_line_number for row in result.token_rows] == [1, 3]
    assert result.duplicate_rows_skipped == 1


def test_dedupe_fails_on_conflicting_duplicate_payload() -> None:
    """Duplicate cumulative totals with mismatched total_usage must fail."""
    first_row = _row(line=1, total=10, last=10)
    conflicting_duplicate = replace(
        _row(line=2, total=10, last=0),
        total_usage=TokenUsageValues(
            input_tokens=9,
            cached_input_tokens=1,
            output_tokens=0,
            reasoning_output_tokens=0,
            total_tokens=10,
        ),
    )
    rows = [first_row, conflicting_duplicate]

    with pytest.raises(DuplicateConflictError):
        dedupe_and_validate_token_rows(Path("session.jsonl"), rows)


def test_dedupe_fails_when_cumulative_totals_decrease() -> None:
    """Cumulative totals must be strictly increasing after dedupe."""
    rows = [
        _row(line=1, total=10, last=10),
        _row(line=2, total=9, last=-1),
    ]

    with pytest.raises(MonotonicityError):
        dedupe_and_validate_token_rows(Path("session.jsonl"), rows)


def test_dedupe_fails_when_delta_does_not_match_last_usage() -> None:
    """Cumulative deltas must equal the event's last_token_usage values."""
    rows = [
        _row(line=1, total=10, last=10),
        _row(line=2, total=18, last=7),
    ]

    with pytest.raises(DeltaConsistencyError):
        dedupe_and_validate_token_rows(Path("session.jsonl"), rows)


def _row(line: int, total: int, last: int) -> TokenEventRow:
    """Build one TokenEventRow for tests."""
    return TokenEventRow(
        session_id=UUID("00000000-0000-0000-0000-000000000001"),
        event_timestamp=datetime(2026, 2, 15, 0, 0, line, tzinfo=UTC),
        event_line_number=line,
        model_code="gpt-5",
        turn_id=UUID("00000000-0000-0000-0000-000000000010"),
        total_usage=TokenUsageValues(
            input_tokens=total,
            cached_input_tokens=0,
            output_tokens=0,
            reasoning_output_tokens=0,
            total_tokens=total,
        ),
        last_usage=TokenUsageValues(
            input_tokens=last,
            cached_input_tokens=0,
            output_tokens=0,
            reasoning_output_tokens=0,
            total_tokens=last,
        ),
    )
