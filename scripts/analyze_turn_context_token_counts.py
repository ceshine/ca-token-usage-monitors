#!/usr/bin/env python3
"""Analyze how many token_count events follow each turn_context event.

This script scans Codex session logs and computes, for each `turn_context` event,
the number of subsequent `token_count` events until the next `turn_context`.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median
from typing import Annotated, Any

import typer


@dataclass
class TurnContextSegment:
    """Represents one turn_context window and token_count events under it."""

    session_id: str
    file_path: Path
    turn_context_line: int
    turn_context_timestamp: str | None
    model_code: str | None
    token_count_events_raw: int = 0
    token_count_events_with_info: int = 0
    total_tokens_snapshots: set[int] = field(default_factory=set)

    @property
    def token_count_events_deduped(self) -> int:
        """Returns deduplicated token_count events by cumulative total tokens."""
        return len(self.total_tokens_snapshots)


@dataclass
class ParseResult:
    """Holds parsed segment data and parse counters."""

    segments: list[TurnContextSegment]
    files_scanned: int
    sessions_found: int
    orphan_token_count_events: int
    parse_errors: int
    sessions_missing_metadata: int
    files_filtered_out: int


@dataclass
class BoundaryWindow:
    """Tracks token-count boundaries around one turn_context event."""

    session_id: str
    file_path: Path
    turn_context_line: int
    turn_context_timestamp: str | None
    model_code: str | None
    prev_total_tokens: int | None = None
    prev_token_line: int | None = None
    prev_token_timestamp: str | None = None
    first_after_total_tokens: int | None = None
    first_after_line: int | None = None
    first_after_timestamp: str | None = None
    second_after_total_tokens: int | None = None
    second_after_line: int | None = None
    second_after_timestamp: str | None = None

    @property
    def has_prev_and_first(self) -> bool:
        """Whether this turn_context has both previous and first-after token rows."""
        return self.prev_total_tokens is not None and self.first_after_total_tokens is not None

    @property
    def first_matches_previous(self) -> bool:
        """Whether first-after total equals previous total."""
        return (
            self.prev_total_tokens is not None
            and self.first_after_total_tokens is not None
            and self.prev_total_tokens == self.first_after_total_tokens
        )

    @property
    def second_is_new(self) -> bool:
        """Whether second-after total is strictly greater than first-after total."""
        return (
            self.first_after_total_tokens is not None
            and self.second_after_total_tokens is not None
            and self.second_after_total_tokens > self.first_after_total_tokens
        )


@dataclass
class BoundaryParseResult:
    """Holds boundary-window parse outputs and counters."""

    windows: list[BoundaryWindow]
    files_scanned: int
    sessions_found: int
    parse_errors: int
    sessions_missing_metadata: int
    files_filtered_out: int


APP = typer.Typer(add_completion=False, help=__doc__)


def safe_int(value: Any) -> int | None:
    """Converts a value to int when safe, else returns None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def extract_session_id(event: dict[str, Any]) -> str | None:
    """Extracts session ID from metadata event if present."""
    event_type = event.get("type")
    if event_type not in {"session_meta", "session_metadata"}:
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    session_id = payload.get("id")
    if isinstance(session_id, str) and session_id:
        return session_id
    return None


def parse_iso_timestamp_to_date(timestamp_str: str) -> date | None:
    """Parses an ISO timestamp string into a date."""
    try:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def extract_session_start_date(event: dict[str, Any]) -> date | None:
    """Extracts session start date from session metadata event."""
    event_type = event.get("type")
    if event_type not in {"session_meta", "session_metadata"}:
        return None

    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None

    payload_timestamp = payload.get("timestamp")
    if isinstance(payload_timestamp, str):
        parsed = parse_iso_timestamp_to_date(payload_timestamp)
        if parsed is not None:
            return parsed

    event_timestamp = event.get("timestamp")
    if isinstance(event_timestamp, str):
        return parse_iso_timestamp_to_date(event_timestamp)
    return None


def file_matches_min_date(file_path: Path, min_date: date | None) -> bool:
    """Returns True if file should be included under the min-date filter."""
    if min_date is None:
        return True

    with file_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue

            session_date = extract_session_start_date(event)
            if session_date is None:
                continue
            return session_date >= min_date

    # If date is unavailable, keep file in scope instead of dropping data.
    return True


def extract_total_tokens(event: dict[str, Any]) -> int | None:
    """Extracts `total_token_usage.total_tokens` from token_count event."""
    if event.get("type") != "event_msg":
        return None

    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "token_count":
        return None

    info = payload.get("info")
    if not isinstance(info, dict):
        return None

    total_usage = info.get("total_token_usage")
    if not isinstance(total_usage, dict):
        return None

    return safe_int(total_usage.get("total_tokens"))


def parse_logs(sessions_root: Path, min_date: date | None = None) -> ParseResult:
    """Parses session logs and collects turn-context segment statistics.

    Args:
        sessions_root: Root folder of Codex sessions.

    Returns:
        ParseResult containing all computed segments and counters.
    """
    segments: list[TurnContextSegment] = []
    files_scanned = 0
    sessions_found: set[str] = set()
    orphan_token_count_events = 0
    parse_errors = 0
    sessions_missing_metadata = 0
    files_filtered_out = 0

    files = sorted(sessions_root.rglob("*.jsonl"))
    for file_path in files:
        files_scanned += 1
        if not file_matches_min_date(file_path=file_path, min_date=min_date):
            files_filtered_out += 1
            continue

        session_id: str | None = None
        current_segment: TurnContextSegment | None = None
        segments_for_file: list[TurnContextSegment] = []

        with file_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                stripped = raw_line.strip()
                if not stripped:
                    continue

                try:
                    event = json.loads(stripped)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                if not isinstance(event, dict):
                    continue

                maybe_session_id = extract_session_id(event)
                if maybe_session_id is not None and session_id is None:
                    session_id = maybe_session_id
                    sessions_found.add(session_id)
                    for item in segments_for_file:
                        item.session_id = session_id

                event_type = event.get("type")
                if event_type == "turn_context":
                    payload = event.get("payload")
                    if not isinstance(payload, dict):
                        payload = {}

                    timestamp = event.get("timestamp")
                    if not isinstance(timestamp, str):
                        timestamp = None

                    model_code = payload.get("model")
                    if not isinstance(model_code, str) or not model_code:
                        model_code = None

                    segment = TurnContextSegment(
                        session_id=session_id or "<missing_session_id>",
                        file_path=file_path,
                        turn_context_line=line_number,
                        turn_context_timestamp=timestamp,
                        model_code=model_code,
                    )
                    segments_for_file.append(segment)
                    segments.append(segment)
                    current_segment = segment
                    continue

                if event_type != "event_msg":
                    continue

                payload = event.get("payload")
                if not isinstance(payload, dict):
                    continue
                if payload.get("type") != "token_count":
                    continue

                if current_segment is None:
                    orphan_token_count_events += 1
                    continue

                current_segment.token_count_events_raw += 1
                info = payload.get("info")
                if not isinstance(info, dict):
                    continue

                current_segment.token_count_events_with_info += 1
                total_tokens = extract_total_tokens(event)
                if total_tokens is not None:
                    current_segment.total_tokens_snapshots.add(total_tokens)

        if session_id is None:
            sessions_missing_metadata += 1

    return ParseResult(
        segments=segments,
        files_scanned=files_scanned,
        sessions_found=len(sessions_found),
        orphan_token_count_events=orphan_token_count_events,
        parse_errors=parse_errors,
        sessions_missing_metadata=sessions_missing_metadata,
        files_filtered_out=files_filtered_out,
    )


def parse_boundary_windows(sessions_root: Path, min_date: date | None = None) -> BoundaryParseResult:
    """Parses logs and builds boundary windows around each turn_context."""
    windows: list[BoundaryWindow] = []
    files_scanned = 0
    parse_errors = 0
    sessions_missing_metadata = 0
    sessions_found: set[str] = set()
    files_filtered_out = 0

    for file_path in sorted(sessions_root.rglob("*.jsonl")):
        files_scanned += 1
        if not file_matches_min_date(file_path=file_path, min_date=min_date):
            files_filtered_out += 1
            continue

        session_id: str | None = None
        pending_window: BoundaryWindow | None = None
        last_token_total: int | None = None
        last_token_line: int | None = None
        last_token_timestamp: str | None = None

        with file_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                stripped = raw_line.strip()
                if not stripped:
                    continue

                try:
                    event = json.loads(stripped)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                if not isinstance(event, dict):
                    continue

                maybe_session_id = extract_session_id(event)
                if maybe_session_id is not None and session_id is None:
                    session_id = maybe_session_id
                    sessions_found.add(session_id)
                    if pending_window is not None:
                        pending_window.session_id = session_id

                event_type = event.get("type")
                if event_type == "turn_context":
                    if pending_window is not None:
                        windows.append(pending_window)

                    payload = event.get("payload")
                    if not isinstance(payload, dict):
                        payload = {}

                    timestamp = event.get("timestamp")
                    if not isinstance(timestamp, str):
                        timestamp = None

                    model_code = payload.get("model")
                    if not isinstance(model_code, str) or not model_code:
                        model_code = None

                    pending_window = BoundaryWindow(
                        session_id=session_id or "<missing_session_id>",
                        file_path=file_path,
                        turn_context_line=line_number,
                        turn_context_timestamp=timestamp,
                        model_code=model_code,
                        prev_total_tokens=last_token_total,
                        prev_token_line=last_token_line,
                        prev_token_timestamp=last_token_timestamp,
                    )
                    continue

                total_tokens = extract_total_tokens(event)
                if total_tokens is None:
                    continue

                timestamp = event.get("timestamp")
                if not isinstance(timestamp, str):
                    timestamp = None

                if pending_window is not None:
                    if pending_window.first_after_total_tokens is None:
                        pending_window.first_after_total_tokens = total_tokens
                        pending_window.first_after_line = line_number
                        pending_window.first_after_timestamp = timestamp
                    elif pending_window.second_after_total_tokens is None:
                        pending_window.second_after_total_tokens = total_tokens
                        pending_window.second_after_line = line_number
                        pending_window.second_after_timestamp = timestamp

                last_token_total = total_tokens
                last_token_line = line_number
                last_token_timestamp = timestamp

        if pending_window is not None:
            windows.append(pending_window)

        if session_id is None:
            sessions_missing_metadata += 1

    return BoundaryParseResult(
        windows=windows,
        files_scanned=files_scanned,
        sessions_found=len(sessions_found),
        parse_errors=parse_errors,
        sessions_missing_metadata=sessions_missing_metadata,
        files_filtered_out=files_filtered_out,
    )


def percentile(values: list[int], p: float) -> float:
    """Computes linear-interpolated percentile for values in [0, 1]."""
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])

    ordered = sorted(values)
    index = (len(ordered) - 1) * p
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return float(ordered[lower])

    lower_value = ordered[lower]
    upper_value = ordered[upper]
    weight = index - lower
    return lower_value + (upper_value - lower_value) * weight


def summarize_metric(values: list[int]) -> dict[str, float]:
    """Builds summary statistics for a metric list."""
    if not values:
        return {
            "count": 0.0,
            "mean": 0.0,
            "median": 0.0,
            "p90": 0.0,
            "p95": 0.0,
            "max": 0.0,
        }
    return {
        "count": float(len(values)),
        "mean": float(mean(values)),
        "median": float(median(values)),
        "p90": float(percentile(values, 0.90)),
        "p95": float(percentile(values, 0.95)),
        "max": float(max(values)),
    }


def print_metric_block(title: str, values: list[int], top_k: int) -> None:
    """Prints summary and top-frequency table for one metric."""
    stats = summarize_metric(values)
    print(f"\n{title}")
    print(f"  count:  {int(stats['count'])}")
    print(f"  mean:   {stats['mean']:.2f}")
    print(f"  median: {stats['median']:.2f}")
    print(f"  p90:    {stats['p90']:.2f}")
    print(f"  p95:    {stats['p95']:.2f}")
    print(f"  max:    {stats['max']:.2f}")

    if not values:
        return

    freq = Counter(values)
    print(f"  top {top_k} frequencies (count -> occurrences, share):")
    total = len(values)
    for value, occurrences in freq.most_common(top_k):
        share = occurrences / total * 100
        print(f"    {value:>4} -> {occurrences:>6} ({share:5.2f}%)")


def print_delta_frequency(title: str, deltas: list[int], top_k: int) -> None:
    """Prints frequency table for integer deltas."""
    print(f"\n{title}")
    if not deltas:
        print("  no rows")
        return

    freq = Counter(deltas)
    total = len(deltas)
    print(f"  rows: {total}")
    print(f"  top {top_k} deltas (delta -> occurrences, share):")
    for delta, occurrences in freq.most_common(top_k):
        share = occurrences / total * 100
        print(f"    {delta:>4} -> {occurrences:>6} ({share:5.2f}%)")


def write_csv(output_path: Path, segments: list[TurnContextSegment]) -> None:
    """Writes per-turn_context segment rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "session_id",
                "file_path",
                "turn_context_line",
                "turn_context_timestamp",
                "model_code",
                "token_count_events_raw",
                "token_count_events_with_info",
                "token_count_events_deduped",
            ]
        )
        for segment in segments:
            writer.writerow(
                [
                    segment.session_id,
                    str(segment.file_path),
                    segment.turn_context_line,
                    segment.turn_context_timestamp or "",
                    segment.model_code or "",
                    segment.token_count_events_raw,
                    segment.token_count_events_with_info,
                    segment.token_count_events_deduped,
                ]
            )


def parse_min_date_or_exit(min_date_raw: str | None) -> date | None:
    """Parses min-date string (YYYY-MM-DD) or raises a Typer error."""
    if min_date_raw is None:
        return None
    try:
        return date.fromisoformat(min_date_raw)
    except ValueError as exc:
        raise typer.BadParameter("Invalid --min-date. Expected format: YYYY-MM-DD.") from exc


def run_analysis(
    sessions_root: Path,
    top_k: int,
    output_csv: Path | None,
    fail_on_parse_error: bool,
    min_date: date | None,
) -> None:
    """Runs the analysis script."""
    sessions_root = sessions_root.expanduser()

    if not sessions_root.exists():
        typer.echo(f"Sessions root does not exist: {sessions_root}")
        raise typer.Exit(code=1)

    result = parse_logs(sessions_root=sessions_root, min_date=min_date)
    segments = result.segments

    raw_values = [segment.token_count_events_raw for segment in segments]
    with_info_values = [segment.token_count_events_with_info for segment in segments]
    deduped_values = [segment.token_count_events_deduped for segment in segments]

    typer.echo("Turn-context -> token-count analysis")
    typer.echo(f"sessions_root: {sessions_root}")
    typer.echo(f"min_date: {min_date.isoformat() if min_date is not None else 'NONE'}")
    typer.echo(f"files_scanned: {result.files_scanned}")
    typer.echo(f"files_filtered_out: {result.files_filtered_out}")
    typer.echo(f"sessions_found: {result.sessions_found}")
    typer.echo(f"turn_context_events: {len(segments)}")
    typer.echo(f"sessions_missing_metadata: {result.sessions_missing_metadata}")
    typer.echo(f"orphan_token_count_events: {result.orphan_token_count_events}")
    typer.echo(f"parse_errors: {result.parse_errors}")

    print_metric_block(
        title="token_count events per turn_context (raw)",
        values=raw_values,
        top_k=top_k,
    )
    print_metric_block(
        title="token_count events per turn_context (info != null)",
        values=with_info_values,
        top_k=top_k,
    )
    print_metric_block(
        title="token_count events per turn_context (dedup by total_tokens)",
        values=deduped_values,
        top_k=top_k,
    )

    if output_csv is not None:
        output_csv = output_csv.expanduser()
        write_csv(output_path=output_csv, segments=segments)
        typer.echo(f"\nWrote CSV: {output_csv}")

    if fail_on_parse_error and result.parse_errors > 0:
        raise typer.Exit(code=2)


@APP.command("analyze")
def analyze(
    sessions_root: Annotated[
        Path,
        typer.Option("--sessions-root", help="Root directory containing Codex session logs."),
    ] = Path("~/.codex/sessions"),
    top_k: Annotated[
        int,
        typer.Option(
            "--top-k",
            min=1,
            help="How many most-common counts to print in frequency tables.",
        ),
    ] = 10,
    output_csv: Annotated[
        Path | None,
        typer.Option("--output-csv", help="Optional path to write per-turn_context segment rows."),
    ] = None,
    fail_on_parse_error: Annotated[
        bool,
        typer.Option(
            "--fail-on-parse-error",
            help="Exit with non-zero status if any JSON parse errors are found.",
        ),
    ] = False,
    min_date_raw: Annotated[
        str | None,
        typer.Option(
            "--min-date",
            help="Only include sessions on/after this date (YYYY-MM-DD).",
        ),
    ] = None,
) -> None:
    """Analyzes token_count counts following turn_context events."""
    min_date = parse_min_date_or_exit(min_date_raw)
    run_analysis(
        sessions_root=sessions_root,
        top_k=top_k,
        output_csv=output_csv,
        fail_on_parse_error=fail_on_parse_error,
        min_date=min_date,
    )


@APP.command("verify-boundary-repeat")
def verify_boundary_repeat(
    sessions_root: Annotated[
        Path,
        typer.Option("--sessions-root", help="Root directory containing Codex session logs."),
    ] = Path("~/.codex/sessions"),
    top_k: Annotated[
        int,
        typer.Option("--top-k", min=1, help="How many most-common deltas to show."),
    ] = 10,
    sample_limit: Annotated[
        int,
        typer.Option("--sample-limit", min=1, help="Number of mismatch samples to print."),
    ] = 10,
    output_csv: Annotated[
        Path | None,
        typer.Option("--output-csv", help="Optional path to write per-boundary rows."),
    ] = None,
    fail_on_parse_error: Annotated[
        bool,
        typer.Option("--fail-on-parse-error", help="Exit non-zero if parse errors are found."),
    ] = False,
    min_date_raw: Annotated[
        str | None,
        typer.Option(
            "--min-date",
            help="Only include sessions on/after this date (YYYY-MM-DD).",
        ),
    ] = None,
) -> None:
    """Verifies whether first token_count after turn_context repeats previous cumulative total."""
    sessions_root = sessions_root.expanduser()
    min_date = parse_min_date_or_exit(min_date_raw)
    if not sessions_root.exists():
        typer.echo(f"Sessions root does not exist: {sessions_root}")
        raise typer.Exit(code=1)

    result = parse_boundary_windows(sessions_root=sessions_root, min_date=min_date)
    windows = result.windows

    windows_with_prev = [item for item in windows if item.prev_total_tokens is not None]
    windows_with_first = [item for item in windows if item.first_after_total_tokens is not None]
    eligible = [item for item in windows if item.has_prev_and_first]
    first_matches = [item for item in eligible if item.first_matches_previous]
    first_mismatches = [item for item in eligible if not item.first_matches_previous]

    eligible_with_second = [item for item in eligible if item.second_after_total_tokens is not None]
    second_is_new = [item for item in eligible_with_second if item.second_is_new]
    second_not_new = [item for item in eligible_with_second if not item.second_is_new]

    typer.echo("Boundary verification: turn_context -> token_count")
    typer.echo(f"sessions_root: {sessions_root}")
    typer.echo(f"min_date: {min_date.isoformat() if min_date is not None else 'NONE'}")
    typer.echo(f"files_scanned: {result.files_scanned}")
    typer.echo(f"files_filtered_out: {result.files_filtered_out}")
    typer.echo(f"sessions_found: {result.sessions_found}")
    typer.echo(f"turn_context_windows: {len(windows)}")
    typer.echo(f"sessions_missing_metadata: {result.sessions_missing_metadata}")
    typer.echo(f"parse_errors: {result.parse_errors}")
    typer.echo(f"windows_with_previous_token: {len(windows_with_prev)}")
    typer.echo(f"windows_with_first_after_token: {len(windows_with_first)}")
    typer.echo(f"eligible_windows(prev+first): {len(eligible)}")

    if eligible:
        first_match_rate = len(first_matches) / len(eligible) * 100
        typer.echo(f"first_after_total == prev_total: {len(first_matches)} / {len(eligible)} ({first_match_rate:.2f}%)")
    else:
        typer.echo("first_after_total == prev_total: no eligible windows")

    if eligible_with_second:
        second_new_rate = len(second_is_new) / len(eligible_with_second) * 100
        typer.echo(
            "second_after_total > first_after_total: "
            f"{len(second_is_new)} / {len(eligible_with_second)} ({second_new_rate:.2f}%)"
        )
    else:
        typer.echo("second_after_total > first_after_total: no eligible windows with second token")

    first_delta = [item.first_after_total_tokens - item.prev_total_tokens for item in eligible]
    second_delta = [
        item.second_after_total_tokens - item.first_after_total_tokens
        for item in eligible_with_second
        if item.first_after_total_tokens is not None and item.second_after_total_tokens is not None
    ]
    print_delta_frequency("delta(first_after_total - prev_total)", first_delta, top_k=top_k)
    print_delta_frequency("delta(second_after_total - first_after_total)", second_delta, top_k=top_k)

    if first_mismatches:
        typer.echo(f"\nSample first-step mismatches (limit {sample_limit}):")
        for item in first_mismatches[:sample_limit]:
            typer.echo(
                f"  {item.file_path}:{item.turn_context_line} "
                f"session={item.session_id} model={item.model_code or 'NONE'} "
                f"prev={item.prev_total_tokens} first={item.first_after_total_tokens}"
            )

    if second_not_new:
        typer.echo(f"\nSample second-step non-increasing rows (limit {sample_limit}):")
        for item in second_not_new[:sample_limit]:
            typer.echo(
                f"  {item.file_path}:{item.turn_context_line} "
                f"session={item.session_id} model={item.model_code or 'NONE'} "
                f"first={item.first_after_total_tokens} second={item.second_after_total_tokens}"
            )

    if output_csv is not None:
        output_csv = output_csv.expanduser()
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        with output_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "session_id",
                    "file_path",
                    "turn_context_line",
                    "turn_context_timestamp",
                    "model_code",
                    "prev_total_tokens",
                    "prev_token_line",
                    "first_after_total_tokens",
                    "first_after_line",
                    "second_after_total_tokens",
                    "second_after_line",
                    "first_matches_previous",
                    "second_is_new",
                ]
            )
            for item in windows:
                writer.writerow(
                    [
                        item.session_id,
                        str(item.file_path),
                        item.turn_context_line,
                        item.turn_context_timestamp or "",
                        item.model_code or "",
                        item.prev_total_tokens,
                        item.prev_token_line,
                        item.first_after_total_tokens,
                        item.first_after_line,
                        item.second_after_total_tokens,
                        item.second_after_line,
                        int(item.first_matches_previous),
                        int(item.second_is_new),
                    ]
                )
        typer.echo(f"\nWrote CSV: {output_csv}")

    if fail_on_parse_error and result.parse_errors > 0:
        raise typer.Exit(code=2)


if __name__ == "__main__":
    APP()
