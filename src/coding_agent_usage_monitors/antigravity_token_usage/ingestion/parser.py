"""Parser for AntiGravity SQLite databases and protobuf usage blobs."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from datetime import UTC, datetime
from dataclasses import dataclass
from typing import Any

from .errors import InvalidDataError, SQLiteSchemaError, ProtobufParseError
from .schemas import RawUsageEvent, DiscoveredDatabase
from .protobuf import (
    parse_protobuf,
    get_bytes_field,
    get_string_field,
    get_varint_field,
    get_all_bytes_fields,
)

NUMERIC_MODEL_MAP: dict[int, str] = {
    246: "gemini-2.5-pro",
    312: "gemini-2.5-flash",
    313: "gemini-2.5-flash-thinking",
    329: "gemini-2.5-flash-thinking",
    330: "gemini-2.5-flash-lite",
    281: "claude-4-sonnet",
    282: "claude-4-sonnet",
    290: "claude-4-opus",
    291: "claude-4-opus",
    333: "claude-4.5-sonnet",
    334: "claude-4.5-sonnet",
    340: "claude-4.5-haiku",
    341: "claude-4.5-haiku",
    342: "gpt-oss-120b-medium",
    1071: "gemini-3.6-flash-high",
    1072: "gemini-3.6-flash-medium",
    1073: "gemini-3.6-flash-low",
    1298: "gemini-3.7-flash-high",
    1299: "gemini-3.7-flash-medium",
    1300: "gemini-3.7-flash-low",
    1318: "gemini-3.8-flash-high",
    1319: "gemini-3.8-flash-medium",
    1320: "gemini-3.8-flash-low",
}

BASE_ALIASES: dict[str, str] = {
    "gemini 3.8 flash": "gemini-3.8-flash",
    "gemini 3.8 flash thinking": "gemini-3.8-flash",
    "gemini 3.7 flash": "gemini-3.7-flash",
    "gemini 3.7 flash thinking": "gemini-3.7-flash",
    "gemini 3.7 pro": "gemini-3.7-pro",
    "gemini 3.7 pro thinking": "gemini-3.7-pro",
    "gemini 3.6 flash": "gemini-3.6-flash",
    "gemini 3 flash": "gemini-3.6-flash",
    "gemini 3.6 pro": "gemini-3.6-pro",
    "gemini 3 pro": "gemini-3-pro",
    "gemini 3 pro thinking": "gemini-3-pro",
    "gemini 2.5 flash": "gemini-2.5-flash",
    "gemini 2.5 pro": "gemini-2.5-pro",
    "gemini 2.0 flash": "gemini-2.0-flash",
    "gemini 2 flash": "gemini-2.0-flash",
    "gemini 2.0 pro": "gemini-2.0-pro",
    "gemini 1.5 flash": "gemini-1.5-flash",
    "gemini 1.5 pro": "gemini-1.5-pro",
    "model_placeholder_m318": "gemini-3.8-flash-high",
    "model_placeholder_m319": "gemini-3.8-flash-medium",
    "model_placeholder_m320": "gemini-3.8-flash-low",
    "model_placeholder_m298": "gemini-3.7-flash-high",
    "model_placeholder_m299": "gemini-3.7-flash-medium",
    "model_placeholder_m300": "gemini-3.7-flash-low",
    "model_placeholder_m71": "gemini-3.6-flash-high",
    "model_placeholder_m72": "gemini-3.6-flash-medium",
    "model_placeholder_m73": "gemini-3.6-flash-low",
    "model_placeholder_m26": "claude-opus-4-6",
    "model_placeholder_m35": "claude-sonnet-4-6",
    "model_placeholder_m36": "gemini-3.1-pro",
    "model_placeholder_m37": "gemini-3.1-pro",
    "model_placeholder_m16": "gemini-3.1-pro",
    "model_placeholder_m18": "gemini-3-flash-preview",
    "model_placeholder_m84": "gemini-3-flash-preview",
    "model_placeholder_m47": "gemini-3-flash-preview",
    "model_placeholder_m132": "gemini-3.5-flash-high",
    "model_placeholder_m133": "gemini-3.5-flash-high",
    "model_placeholder_m187": "gemini-3.5-flash-extra-low",
    "model_placeholder_m20": "gemini-3.5-flash-medium",
    "model_openai_gpt_oss_120b_medium": "gpt-oss-120b-medium",
    "gemini-pro-default": "gemini-3.1-pro",
    "gemini-pro-agent": "gemini-3.1-pro",
    "gemini-3-flash-agent": "gemini-3.5-flash-high",
    "gemini-3-flash-agent-a": "gemini-3.5-flash-high",
    "gemini-3-flash-agent-b": "gemini-3.5-flash-high",
    "gemini-3-flash-a": "gemini-3.5-flash-high",
    "gemini-3-flash-b": "gemini-3.5-flash-high",
    "gemini-3-flash-c": "gemini-3-flash-preview",
    "gemini-3-flash": "gemini-3-flash-preview",
    "gemini-3.5-flash-low": "gemini-3.5-flash-medium",
    "gemini-3.1-pro-high": "gemini-3.1-pro",
    "gemini-3.1-pro-low": "gemini-3.1-pro",
    "gemini-3-pro-high": "gemini-3-pro",
    "gemini-3-pro-low": "gemini-3-pro",
    "claude 3.7 sonnet": "claude-3-7-sonnet",
    "claude 3.7 sonnet thinking": "claude-3-7-sonnet",
    "claude 3.5 sonnet": "claude-3-5-sonnet",
    "claude 3.5 haiku": "claude-3-5-haiku",
    "claude 3 opus": "claude-3-opus",
}

EFFORT_REGEX: re.Pattern[str] = re.compile(r"^gemini\s+([3-9]\.[0-9]+)\s+flash\s*\((high|medium|low)\)$", re.IGNORECASE)


@dataclass(frozen=True)
class ParseResult:
    """Output of parsing one SQLite database."""

    events: list[RawUsageEvent]
    missing_optional_tables: int


def resolve_numeric_model_id(model_id: int) -> str:
    """Map numeric AntiGravity model ID to a model string.

    Args:
        model_id (int): Numeric model ID.

    Returns:
        str: Canonical name or placeholder model string.
    """
    if model_id in NUMERIC_MODEL_MAP:
        return NUMERIC_MODEL_MAP[model_id]
    if model_id < 1000:
        return f"antigravity-model-{model_id}"
    return f"model_placeholder_m{model_id - 1000}"


def normalize_antigravity_model(model_name: str | None) -> str | None:
    """Normalize a raw or mapped model string according to ccusage rules.

    Args:
        model_name (str | None): Model string to normalize.

    Returns:
        str | None: Normalized model string, or None if empty.
    """
    if not model_name:
        return None
    trimmed = model_name.strip()
    if not trimmed:
        return None

    match = EFFORT_REGEX.match(trimmed)
    if match:
        version, effort = match.group(1), match.group(2).lower()
        return f"gemini-{version}-flash-{effort}"

    base = trimmed.split("(")[0].strip()
    base_lower = base.lower()
    if base_lower in BASE_ALIASES:
        return BASE_ALIASES[base_lower]

    converted = base.replace(" ", "-")
    if converted.lower().startswith(("gemini-", "claude-", "gpt-")):
        return converted.lower()

    return base


def resolve_model_code(
    usage_model_id: int | None,
    raw_model_name: str | None,
    meta_model_id: int | None,
    inherited_model: str | None,
) -> str:
    """Resolve model code in order of precedence: usage_model_id, raw_model_name, meta_model_id, inherited.

    Args:
        usage_model_id (int | None): ModelUsage field 1 ID.
        raw_model_name (str | None): Enclosing raw model string.
        meta_model_id (int | None): Enclosing metadata numeric model ID.
        inherited_model (str | None): Inherited generation model string.

    Returns:
        str: Resolved canonical model string.
    """
    if usage_model_id is not None and usage_model_id != 0:
        mapped = resolve_numeric_model_id(usage_model_id)
        norm = normalize_antigravity_model(mapped)
        if norm:
            return norm

    if raw_model_name:
        norm = normalize_antigravity_model(raw_model_name)
        if norm:
            return norm

    if meta_model_id is not None and meta_model_id != 0:
        mapped = resolve_numeric_model_id(meta_model_id)
        norm = normalize_antigravity_model(mapped)
        if norm:
            return norm

    if inherited_model:
        norm = normalize_antigravity_model(inherited_model)
        if norm:
            return norm

    return "gemini-internal-model"


def decode_timestamp_message(data: bytes) -> datetime | None:
    """Decode Protobuf TimestampMessage (field 1 seconds, field 2 nanos).

    Args:
        data (bytes): Protobuf TimestampMessage bytes.

    Returns:
        datetime | None: UTC datetime or None if invalid.
    """
    try:
        fields = parse_protobuf(data)
        seconds = get_varint_field(fields, 1)
        nanos = get_varint_field(fields, 2) or 0
        if seconds is None:
            return None
        epoch_ms = (seconds * 1000) + (nanos // 1_000_000)
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)
    except (ProtobufParseError, ValueError, TypeError):
        return None


def normalize_tokens(
    raw_input: int,
    raw_visible_output: int,
    raw_reasoning: int,
    raw_total_output: int,
    raw_cache_creation: int,
    raw_cache_read: int,
    database_path: Path,
    table: str,
    row_idx: int,
) -> tuple[int, int, int, int, int, int, int]:
    """Reconcile and calculate canonical token counters.

    Args:
        raw_input (int): Raw input tokens.
        raw_visible_output (int): Raw visible output tokens.
        raw_reasoning (int): Raw reasoning tokens.
        raw_total_output (int): Raw total output tokens.
        raw_cache_creation (int): Raw cache creation tokens.
        raw_cache_read (int): Raw cache read tokens.
        database_path (Path): Associated database path for error context.
        table (str): Associated table name for error context.
        row_idx (int): Associated row index for error context.

    Returns:
        tuple[int, int, int, int, int, int, int]:
            (input_tokens, output_tokens, reasoning_tokens, total_output_tokens,
             cache_creation_tokens, cache_read_tokens, total_tokens)

    Raises:
        InvalidDataError: If any raw token counter is negative.
    """
    for val, name in [
        (raw_input, "input_tokens"),
        (raw_visible_output, "visible_output_tokens"),
        (raw_reasoning, "reasoning_tokens"),
        (raw_total_output, "total_output_tokens"),
        (raw_cache_creation, "cache_creation_tokens"),
        (raw_cache_read, "cache_read_tokens"),
    ]:
        if val < 0:
            raise InvalidDataError(
                f"Negative token counter '{name}': {val}",
                database_path=database_path,
                table=table,
                row_index=row_idx,
            )

    total_output = max(raw_total_output, raw_visible_output + raw_reasoning)
    output = max(raw_visible_output, total_output - raw_reasoning)
    reasoning = max(raw_reasoning, total_output - output)
    total = raw_input + raw_cache_creation + raw_cache_read + total_output

    return (
        raw_input,
        max(0, output),
        max(0, reasoning),
        max(0, total_output),
        raw_cache_creation,
        raw_cache_read,
        max(0, total),
    )


def is_token_bearing(
    input_tokens: int,
    output_tokens: int,
    reasoning_tokens: int,
    total_output_tokens: int,
    cache_creation_tokens: int,
    cache_read_tokens: int,
) -> bool:
    """Check whether a usage contains any non-zero token counter.

    Args:
        input_tokens (int): Input tokens.
        output_tokens (int): Output tokens.
        reasoning_tokens (int): Reasoning tokens.
        total_output_tokens (int): Total output tokens.
        cache_creation_tokens (int): Cache creation tokens.
        cache_read_tokens (int): Cache read tokens.

    Returns:
        bool: True if at least one token counter > 0, False otherwise.
    """
    return (
        input_tokens
        + output_tokens
        + reasoning_tokens
        + total_output_tokens
        + cache_creation_tokens
        + cache_read_tokens
    ) > 0


def _parse_model_usage(usage_bytes: bytes) -> dict[str, Any]:
    """Parse ModelUsage submessage bytes.

    Args:
        usage_bytes (bytes): ModelUsage protobuf bytes.

    Returns:
        dict[str, Any]: Extracted raw fields dictionary.
    """
    fields = parse_protobuf(usage_bytes)
    return {
        "model_id": get_varint_field(fields, 1),
        "input_tokens": get_varint_field(fields, 2) or 0,
        "total_output_tokens": get_varint_field(fields, 3) or 0,
        "cache_creation_tokens": get_varint_field(fields, 4) or 0,
        "cache_read_tokens": get_varint_field(fields, 5) or 0,
        "provider_id": get_varint_field(fields, 6),
        "message_id": get_string_field(fields, 7),
        "reasoning_tokens": get_varint_field(fields, 9) or 0,
        "visible_output_tokens": get_varint_field(fields, 10) or 0,
        "response_id": get_string_field(fields, 11),
        "provider_assigned_message_id": get_string_field(fields, 12),
    }


def parse_database(discovered_db: DiscoveredDatabase) -> ParseResult:
    """Query and parse an AntiGravity SQLite database into candidate raw events.

    Args:
        discovered_db (DiscoveredDatabase): Discovered database descriptor.

    Returns:
        ParseResult: Parsed events list and missing optional tables counter.

    Raises:
        InvalidDataError: If session_id is empty or token counts are negative.
        SQLiteSchemaError: On SQLite open/query/busy errors or missing gen_metadata.
        ProtobufParseError: On protobuf wire-format decoding errors.
    """
    if not discovered_db.session_id or not discovered_db.session_id.strip():
        raise InvalidDataError(
            "Discovered database has empty session_id stem",
            database_path=discovered_db.database_path,
        )

    db_path = discovered_db.database_path.resolve()
    db_uri = f"{db_path.as_uri()}?mode=ro"

    try:
        connection = sqlite3.connect(db_uri, uri=True, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 5000")
    except sqlite3.Error as err:
        raise SQLiteSchemaError(
            f"Failed to open SQLite database read-only: {err}",
            database_path=db_path,
        ) from err

    try:
        connection.execute("BEGIN")
        connection.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchall()

        table_rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        table_names = {row[0] for row in table_rows}

        if "gen_metadata" not in table_names:
            raise SQLiteSchemaError(
                "Required table 'gen_metadata' is missing",
                database_path=db_path,
                table="gen_metadata",
            )

        gen_rows = connection.execute("SELECT idx, data FROM gen_metadata ORDER BY idx ASC").fetchall()

        steps_present = "steps" in table_names
        steps_rows: list[tuple[int, bytes]] = []
        if steps_present:
            steps_rows = connection.execute(
                "SELECT idx, metadata FROM steps WHERE metadata IS NOT NULL ORDER BY idx ASC"
            ).fetchall()

        traj_present = "trajectory_metadata_blob" in table_names
        traj_rows: list[tuple[bytes]] = []
        if traj_present:
            traj_rows = connection.execute("SELECT data FROM trajectory_metadata_blob ORDER BY rowid ASC").fetchall()

        connection.execute("COMMIT")
    except sqlite3.Error as err:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise SQLiteSchemaError(
            f"SQLite transaction or schema query failed: {err}",
            database_path=db_path,
        ) from err
    except Exception as err:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        if isinstance(err, (SQLiteSchemaError, InvalidDataError, ProtobufParseError)):
            raise
        raise SQLiteSchemaError(
            f"Unexpected error during SQLite read transaction: {err}",
            database_path=db_path,
        ) from err
    finally:
        connection.close()

    missing_optional_tables = (0 if steps_present else 1) + (0 if traj_present else 1)
    file_mtime = datetime.fromtimestamp(db_path.stat().st_mtime, tz=UTC)

    trajectory_timestamp: datetime | None = None
    for row in traj_rows:
        blob = row[0]
        if blob:
            try:
                traj_fields = parse_protobuf(blob)
                ts_bytes = get_bytes_field(traj_fields, 2)
                if ts_bytes:
                    ts_val = decode_timestamp_message(ts_bytes)
                    if ts_val is not None:
                        trajectory_timestamp = ts_val
                        break
            except (ProtobufParseError, ValueError, TypeError):
                continue

    events: list[RawUsageEvent] = []
    canonical_path_str = str(db_path)
    last_resolved_gen_model: str | None = None

    for gen_idx, gen_blob in gen_rows:
        if not gen_blob:
            continue
        try:
            gen_fields = parse_protobuf(gen_blob)
        except ProtobufParseError as err:
            err.database_path = db_path
            err.table = "gen_metadata"
            err.row_index = gen_idx
            raise

        chat_model_bytes = get_bytes_field(gen_fields, 1)
        if not chat_model_bytes:
            continue

        try:
            chat_fields = parse_protobuf(chat_model_bytes)
        except ProtobufParseError as err:
            err.database_path = db_path
            err.table = "gen_metadata"
            err.row_index = gen_idx
            raise

        gen_meta_model_id = get_varint_field(chat_fields, 3)
        gen_raw_model_name = get_string_field(chat_fields, 19) or get_string_field(chat_fields, 21)
        primary_usage_bytes = get_bytes_field(chat_fields, 4)
        gen_info_bytes = get_bytes_field(chat_fields, 9)
        retry_blobs = get_all_bytes_fields(chat_fields, 17)

        gen_explicit_ts: datetime | None = None
        if gen_info_bytes:
            try:
                gen_info_fields = parse_protobuf(gen_info_bytes)
                ts_msg_bytes = get_bytes_field(gen_info_fields, 4)
                if ts_msg_bytes:
                    gen_explicit_ts = decode_timestamp_message(ts_msg_bytes)
            except (ProtobufParseError, ValueError, TypeError):
                pass

        if gen_explicit_ts is not None:
            gen_ts, gen_ts_rank = gen_explicit_ts, 3
        elif trajectory_timestamp is not None:
            gen_ts, gen_ts_rank = trajectory_timestamp, 1
        else:
            gen_ts, gen_ts_rank = file_mtime, 0

        # Update last_resolved_gen_model
        resolved_row_model = resolve_model_code(
            usage_model_id=None,
            raw_model_name=gen_raw_model_name,
            meta_model_id=gen_meta_model_id,
            inherited_model=last_resolved_gen_model,
        )
        if resolved_row_model != "gemini-internal-model":
            last_resolved_gen_model = resolved_row_model

        if primary_usage_bytes:
            u_dict = _parse_model_usage(primary_usage_bytes)
            if is_token_bearing(
                u_dict["input_tokens"],
                u_dict["visible_output_tokens"],
                u_dict["reasoning_tokens"],
                u_dict["total_output_tokens"],
                u_dict["cache_creation_tokens"],
                u_dict["cache_read_tokens"],
            ):
                model_code = resolve_model_code(
                    usage_model_id=u_dict["model_id"],
                    raw_model_name=gen_raw_model_name,
                    meta_model_id=gen_meta_model_id,
                    inherited_model=last_resolved_gen_model,
                )
                (
                    inp,
                    out,
                    reas,
                    tot_out,
                    c_create,
                    c_read,
                    tot,
                ) = normalize_tokens(
                    raw_input=u_dict["input_tokens"],
                    raw_visible_output=u_dict["visible_output_tokens"],
                    raw_reasoning=u_dict["reasoning_tokens"],
                    raw_total_output=u_dict["total_output_tokens"],
                    raw_cache_creation=u_dict["cache_creation_tokens"],
                    raw_cache_read=u_dict["cache_read_tokens"],
                    database_path=db_path,
                    table="gen_metadata",
                    row_idx=gen_idx,
                )
                events.append(
                    RawUsageEvent(
                        session_id=discovered_db.session_id,
                        discovery_source=discovered_db.discovery_source,
                        event_timestamp=gen_ts,
                        timestamp_rank=gen_ts_rank,
                        model_code=model_code,
                        provider_id=u_dict["provider_id"],
                        input_tokens=inp,
                        output_tokens=out,
                        reasoning_tokens=reas,
                        total_output_tokens=tot_out,
                        cache_creation_tokens=c_create,
                        cache_read_tokens=c_read,
                        total_tokens=tot,
                        response_id=u_dict["response_id"],
                        provider_assigned_message_id=u_dict["provider_assigned_message_id"],
                        message_id=u_dict["message_id"],
                        fallback_key=f"fallback:{canonical_path_str}:gen_metadata:{gen_idx}:primary",
                    )
                )

        for ordinal, retry_item in enumerate(retry_blobs):
            try:
                r_fields = parse_protobuf(retry_item)
                retry_usage_bytes = get_bytes_field(r_fields, 2)
                if not retry_usage_bytes:
                    continue
                u_dict = _parse_model_usage(retry_usage_bytes)
                if is_token_bearing(
                    u_dict["input_tokens"],
                    u_dict["visible_output_tokens"],
                    u_dict["reasoning_tokens"],
                    u_dict["total_output_tokens"],
                    u_dict["cache_creation_tokens"],
                    u_dict["cache_read_tokens"],
                ):
                    model_code = resolve_model_code(
                        usage_model_id=u_dict["model_id"],
                        raw_model_name=gen_raw_model_name,
                        meta_model_id=gen_meta_model_id,
                        inherited_model=last_resolved_gen_model,
                    )
                    (
                        inp,
                        out,
                        reas,
                        tot_out,
                        c_create,
                        c_read,
                        tot,
                    ) = normalize_tokens(
                        raw_input=u_dict["input_tokens"],
                        raw_visible_output=u_dict["visible_output_tokens"],
                        raw_reasoning=u_dict["reasoning_tokens"],
                        raw_total_output=u_dict["total_output_tokens"],
                        raw_cache_creation=u_dict["cache_creation_tokens"],
                        raw_cache_read=u_dict["cache_read_tokens"],
                        database_path=db_path,
                        table="gen_metadata",
                        row_idx=gen_idx,
                    )
                    events.append(
                        RawUsageEvent(
                            session_id=discovered_db.session_id,
                            discovery_source=discovered_db.discovery_source,
                            event_timestamp=gen_ts,
                            timestamp_rank=gen_ts_rank,
                            model_code=model_code,
                            provider_id=u_dict["provider_id"],
                            input_tokens=inp,
                            output_tokens=out,
                            reasoning_tokens=reas,
                            total_output_tokens=tot_out,
                            cache_creation_tokens=c_create,
                            cache_read_tokens=c_read,
                            total_tokens=tot,
                            response_id=u_dict["response_id"],
                            provider_assigned_message_id=u_dict["provider_assigned_message_id"],
                            message_id=u_dict["message_id"],
                            fallback_key=f"fallback:{canonical_path_str}:gen_metadata:{gen_idx}:retry_{ordinal}",
                        )
                    )
            except (ProtobufParseError, ValueError, TypeError):
                continue

    for step_idx, step_blob in steps_rows:
        if not step_blob:
            continue
        try:
            step_fields = parse_protobuf(step_blob)
        except ProtobufParseError as err:
            err.database_path = db_path
            err.table = "steps"
            err.row_index = step_idx
            raise

        ts_bytes = get_bytes_field(step_fields, 8) or get_bytes_field(step_fields, 1)
        step_explicit_ts: datetime | None = None
        if ts_bytes:
            step_explicit_ts = decode_timestamp_message(ts_bytes)

        if step_explicit_ts is not None:
            step_ts, step_ts_rank = step_explicit_ts, 3
        elif trajectory_timestamp is not None:
            step_ts, step_ts_rank = trajectory_timestamp, 1
        else:
            step_ts, step_ts_rank = file_mtime, 0

        step_raw_name: str | None = None
        step_meta_model_id: int | None = None
        step_provider_id: int | None = None

        model_info_bytes = get_bytes_field(step_fields, 24)
        if model_info_bytes:
            mi_fields = parse_protobuf(model_info_bytes)
            step_meta_model_id = get_varint_field(mi_fields, 1)
            step_provider_id = get_varint_field(mi_fields, 7)
            step_raw_name = get_string_field(mi_fields, 12) or get_string_field(mi_fields, 8)

        primary_usage_bytes = get_bytes_field(step_fields, 9)
        retry_blobs = get_all_bytes_fields(step_fields, 28)

        if primary_usage_bytes:
            u_dict = _parse_model_usage(primary_usage_bytes)
            if is_token_bearing(
                u_dict["input_tokens"],
                u_dict["visible_output_tokens"],
                u_dict["reasoning_tokens"],
                u_dict["total_output_tokens"],
                u_dict["cache_creation_tokens"],
                u_dict["cache_read_tokens"],
            ):
                model_code = resolve_model_code(
                    usage_model_id=u_dict["model_id"],
                    raw_model_name=step_raw_name,
                    meta_model_id=step_meta_model_id,
                    inherited_model=last_resolved_gen_model,
                )
                provider_id = u_dict["provider_id"] if u_dict["provider_id"] is not None else step_provider_id
                (
                    inp,
                    out,
                    reas,
                    tot_out,
                    c_create,
                    c_read,
                    tot,
                ) = normalize_tokens(
                    raw_input=u_dict["input_tokens"],
                    raw_visible_output=u_dict["visible_output_tokens"],
                    raw_reasoning=u_dict["reasoning_tokens"],
                    raw_total_output=u_dict["total_output_tokens"],
                    raw_cache_creation=u_dict["cache_creation_tokens"],
                    raw_cache_read=u_dict["cache_read_tokens"],
                    database_path=db_path,
                    table="steps",
                    row_idx=step_idx,
                )
                events.append(
                    RawUsageEvent(
                        session_id=discovered_db.session_id,
                        discovery_source=discovered_db.discovery_source,
                        event_timestamp=step_ts,
                        timestamp_rank=step_ts_rank,
                        model_code=model_code,
                        provider_id=provider_id,
                        input_tokens=inp,
                        output_tokens=out,
                        reasoning_tokens=reas,
                        total_output_tokens=tot_out,
                        cache_creation_tokens=c_create,
                        cache_read_tokens=c_read,
                        total_tokens=tot,
                        response_id=u_dict["response_id"],
                        provider_assigned_message_id=u_dict["provider_assigned_message_id"],
                        message_id=u_dict["message_id"],
                        fallback_key=f"fallback:{canonical_path_str}:steps:{step_idx}:primary",
                    )
                )

        for ordinal, retry_item in enumerate(retry_blobs):
            try:
                r_fields = parse_protobuf(retry_item)
                retry_usage_bytes = get_bytes_field(r_fields, 2)
                if not retry_usage_bytes:
                    continue
                u_dict = _parse_model_usage(retry_usage_bytes)
                if is_token_bearing(
                    u_dict["input_tokens"],
                    u_dict["visible_output_tokens"],
                    u_dict["reasoning_tokens"],
                    u_dict["total_output_tokens"],
                    u_dict["cache_creation_tokens"],
                    u_dict["cache_read_tokens"],
                ):
                    model_code = resolve_model_code(
                        usage_model_id=u_dict["model_id"],
                        raw_model_name=step_raw_name,
                        meta_model_id=step_meta_model_id,
                        inherited_model=last_resolved_gen_model,
                    )
                    provider_id = u_dict["provider_id"] if u_dict["provider_id"] is not None else step_provider_id
                    (
                        inp,
                        out,
                        reas,
                        tot_out,
                        c_create,
                        c_read,
                        tot,
                    ) = normalize_tokens(
                        raw_input=u_dict["input_tokens"],
                        raw_visible_output=u_dict["visible_output_tokens"],
                        raw_reasoning=u_dict["reasoning_tokens"],
                        raw_total_output=u_dict["total_output_tokens"],
                        raw_cache_creation=u_dict["cache_creation_tokens"],
                        raw_cache_read=u_dict["cache_read_tokens"],
                        database_path=db_path,
                        table="steps",
                        row_idx=step_idx,
                    )
                    events.append(
                        RawUsageEvent(
                            session_id=discovered_db.session_id,
                            discovery_source=discovered_db.discovery_source,
                            event_timestamp=step_ts,
                            timestamp_rank=step_ts_rank,
                            model_code=model_code,
                            provider_id=provider_id,
                            input_tokens=inp,
                            output_tokens=out,
                            reasoning_tokens=reas,
                            total_output_tokens=tot_out,
                            cache_creation_tokens=c_create,
                            cache_read_tokens=c_read,
                            total_tokens=tot,
                            response_id=u_dict["response_id"],
                            provider_assigned_message_id=u_dict["provider_assigned_message_id"],
                            message_id=u_dict["message_id"],
                            fallback_key=f"fallback:{canonical_path_str}:steps:{step_idx}:retry_{ordinal}",
                        )
                    )
            except (ProtobufParseError, ValueError, TypeError):
                continue

    return ParseResult(events=events, missing_optional_tables=missing_optional_tables)
