"""Deduplication and integrity checks for token event rows."""

from __future__ import annotations

from pathlib import Path

from codex_token_usage.ingestion.errors import DeltaConsistencyError, DuplicateConflictError, MonotonicityError
from codex_token_usage.ingestion.schemas import DedupeResult, TokenEventRow, TokenUsageValues


def dedupe_and_validate_token_rows(session_file_path: Path, token_rows: list[TokenEventRow]) -> DedupeResult:
    """Dedupe by cumulative total and enforce monotonicity + delta consistency."""
    first_by_cumulative: dict[int, TokenEventRow] = {}
    deduped_rows: list[TokenEventRow] = []
    duplicate_rows_skipped = 0

    for row in token_rows:
        cumulative = row.total_tokens_cumulative
        existing_row = first_by_cumulative.get(cumulative)
        if existing_row is None:
            first_by_cumulative[cumulative] = row
            deduped_rows.append(row)
            continue

        if not _rows_have_matching_payload(existing_row, row):
            raise DuplicateConflictError(
                f"Conflicting duplicate total_tokens_cumulative={cumulative} in {session_file_path}: "
                f"line {existing_row.event_line_number} vs line {row.event_line_number}."
            )
        duplicate_rows_skipped += 1

    _validate_uniqueness(session_file_path, deduped_rows)
    _validate_monotonicity_and_deltas(session_file_path, deduped_rows)
    return DedupeResult(token_rows=deduped_rows, duplicate_rows_skipped=duplicate_rows_skipped)


def _validate_uniqueness(session_file_path: Path, token_rows: list[TokenEventRow]) -> None:
    """Fail if duplicate cumulative totals remain after first-pass dedupe."""
    observed: set[int] = set()
    for row in token_rows:
        cumulative = row.total_tokens_cumulative
        if cumulative in observed:
            raise DuplicateConflictError(
                f"Duplicate total_tokens_cumulative={cumulative} remains after dedupe in {session_file_path}."
            )
        observed.add(cumulative)


def _validate_monotonicity_and_deltas(session_file_path: Path, token_rows: list[TokenEventRow]) -> None:
    """Validate strict monotonicity and snapshot delta consistency."""
    if len(token_rows) <= 1:
        return

    previous = token_rows[0]
    for current in token_rows[1:]:
        if current.total_tokens_cumulative <= previous.total_tokens_cumulative:
            raise MonotonicityError(
                f"Cumulative total decreased or stalled in {session_file_path}: "
                f"line {previous.event_line_number} ({previous.total_tokens_cumulative}) -> "
                f"line {current.event_line_number} ({current.total_tokens_cumulative})."
            )

        _validate_row_delta(session_file_path, previous, current)
        previous = current


def _validate_row_delta(session_file_path: Path, previous: TokenEventRow, current: TokenEventRow) -> None:
    """Validate delta equality for all token fields between adjacent deduped rows."""
    for field_name in (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    ):
        previous_value = _usage_field(previous.total_usage, field_name)
        current_value = _usage_field(current.total_usage, field_name)
        expected_delta = current_value - previous_value
        actual_delta = _usage_field(current.last_usage, field_name)
        if expected_delta != actual_delta:
            raise DeltaConsistencyError(
                f"Delta mismatch for {field_name} in {session_file_path}: "
                f"line {previous.event_line_number} -> line {current.event_line_number}, "
                f"expected {expected_delta}, got {actual_delta}."
            )


def _rows_have_matching_payload(left: TokenEventRow, right: TokenEventRow) -> bool:
    """Return True when duplicate rows carry identical token payload values."""
    return left.total_usage == right.total_usage and left.last_usage == right.last_usage


def _usage_field(usage: TokenUsageValues, field_name: str) -> int:
    """Return a usage field value by name."""
    return getattr(usage, field_name)
