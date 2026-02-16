# Token Ingestion Technical Design (Codex Sessions -> DuckDB)

Created: 2026-02-16

## 1. Scope

This document defines the data-ingestion design for extracting token usage from local Codex session logs at `~/.codex/sessions/` and loading it into DuckDB.

In scope:

- Parse session logs (`.jsonl`) in a streaming-safe way.
- Extract session metadata.
- Extract `token_count` usage events and deduplicate robustly.
- Attribute token usage to model code from `turn_context`, including mid-session model switches.
- Persist idempotently to DuckDB.

Out of scope:

- Final reporting/aggregation CLI UX.
- Cost computation/pricing.

## 2. Source Log Observations

Observed in local logs (2025-2026):

- Session event type is `session_meta`.
- Model context event type is `turn_context` with `payload.model`.
- Token usage event type is `event_msg` where `payload.type == "token_count"`.
- `token_count` can have `payload.info == null` and must be ignored for ingestion.
- Non-null `payload.info` contains:
  - `total_token_usage`: cumulative totals.
  - `last_token_usage`: incremental usage for the latest step.
- Duplicate `token_count` rows are frequent.
  - Verified boundary pattern: the first `token_count` after a `turn_context` often repeats the last cumulative total before that `turn_context`.
  - In filtered recent sessions (`--min-date 2026-01-01`), this boundary-repeat pattern was 100% in local data.
- Some sessions contain multiple models in one file (model switch mid-session).
- Some sessions have metadata but no token events.

## 3. Data Model

### 3.1 `codex_session_metadata`

One row per session (session ID from first `session_meta` event).

```sql
CREATE TABLE IF NOT EXISTS codex_session_metadata (
    session_id UUID PRIMARY KEY,
    session_timestamp TIMESTAMPTZ,
    cwd VARCHAR,
    session_file_path VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.2 `codex_session_details`

One deduplicated token event per unique cumulative total within a session.

Field mapping:

- `total_tokens_cumulative` comes from `payload.info.total_token_usage.total_tokens`.
- `input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, and `total_tokens` come from `payload.info.last_token_usage` (incremental usage for that event).

Deduplication key:

- `session_id + total_tokens_cumulative` where `total_tokens_cumulative = payload.info.total_token_usage.total_tokens`.

```sql
CREATE TABLE IF NOT EXISTS codex_session_details (
    session_id UUID NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_line_number BIGINT NOT NULL,
    model_code VARCHAR,
    turn_id UUID,

    total_tokens_cumulative BIGINT NOT NULL,

    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_output_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (session_id, total_tokens_cumulative)
);
```

## 4. Parsing and Attribution Rules

### 4.1 Session identity

1. Read file line-by-line (preserve line number).
2. Take the first event with `type = "session_meta"`.
3. Use `payload.id` as `session_id`.
4. If missing, fail file ingestion with a descriptive error.

### 4.2 Model state machine

Maintain a mutable `current_model_context` while scanning:

- On each `turn_context` event:
  - Update `current_model_code = payload.model` if present.
  - Update `current_turn_id = payload.turn_id`.
- On each valid token event:
  - Assign `model_code = current_model_code`.
  - Assign context timestamp and turn ID from the current model context.
  - If `current_model_code` is missing at token event time, fail ingestion for the file.

### 4.3 Token event extraction

A row is a candidate token row only when:

- `type == "event_msg"`
- `payload.type == "token_count"`
- `payload.info != null`
- `payload.info.total_token_usage.total_tokens` exists
- `payload.info.last_token_usage.total_tokens` exists
- For each token field (`input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, `total_tokens`):
  - `payload.info.total_token_usage.<field>` exists
  - `payload.info.last_token_usage.<field>` exists

### 4.4 Deduplication

Within each session, apply deduplication in two stages before DB write:

1. Stream-level dedupe by cumulative total:
   - Keep the first row for each `total_tokens_cumulative`.
   - Drop later rows with the same `total_tokens_cumulative`.
2. Pre-upsert in-memory dedupe safety pass:
   - Ensure the outgoing row batch has unique `total_tokens_cumulative` values for the session.
   - Fail if duplicate cumulative totals remain with conflicting payload values.

Rationale:

- Duplicates are often emitted at turn boundaries.
- Keeping first occurrence removes repeated cumulative snapshots and prevents double counting.

Guardrail:

- If two rows share `total_tokens_cumulative` but differ in other token fields, fail ingestion (data integrity issue).

### 4.5 Monotonicity Assumption

- We assume all models in these sessions share the same tokenizer domain.
- Equality is treated as duplication and handled in Section 4.4 (keep first occurrence).
- After deduplication, `total_tokens_cumulative` must be strictly increasing across unique token events, including across model switches.
- The first unique token event in a session has no prior row and is excluded from delta checks.

Guardrails:

- A decrease in cumulative totals is treated as a hard error and ingestion should fail for that file.
- For each adjacent pair of deduped unique events (`prev`, `curr`), enforce:
  - `curr.total_token_usage[field] - prev.total_token_usage[field] == curr.last_token_usage[field]`
  - Fields checked: `input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, `total_tokens`.
- Any delta inconsistency is a hard error and ingestion should fail for that file.

## 5. Ingestion Workflow

1. Discover files under `~/.codex/sessions/**/*.jsonl` (sorted path order).
2. For each file, compute `(session_file_path, file_size_bytes, file_mtime)`.
3. Check `codex_ingestion_files`:
   - If `(session_file_path, file_size_bytes, file_mtime)` is unchanged since the last successful ingestion, skip the file.
   - If changed or unseen, continue.
4. For changed/unseen files, parse `session_id` from the first `session_meta` event.
5. Load per-session checkpoint from DB (`last_ts`, `last_total_tokens_cumulative`) from the latest ingested row for that `session_id`.
6. Parse stream and build:
   - `session_metadata` row.
   - `candidate_token_rows` filtered by checkpoint:
     - If no checkpoint exists: include all candidate token rows.
     - If checkpoint exists: include rows where
       - `event_timestamp > last_ts`, or
       - `event_timestamp == last_ts AND total_tokens_cumulative >= last_total_tokens_cumulative`.
   - `deduped_token_rows` list (includes boundary row by design so dedupe can remove potential duplicate at resume point).
   - Monotonicity + delta-consistency checks on the deduped unique-cumulative sequence.
7. Write in one transaction per changed file:
   - Upsert `codex_session_metadata`.
   - Insert deduped rows into `codex_session_details` with conflict handling on `(session_id, total_tokens_cumulative)`.
   - Upsert `codex_ingestion_files` with latest `file_size_bytes`, `file_mtime`, and `ingested_at`.

Recommended SQL pattern:

- Use `MERGE` or `INSERT ... ON CONFLICT DO NOTHING` depending on DuckDB version.

## 6. Idempotency and Incremental Re-runs

Use a hybrid strategy:

- File-level change pruning via `codex_ingestion_files`.
- Session-level tail ingestion via checkpoint from `codex_session_details`.

File bookkeeping table:

```sql
CREATE TABLE IF NOT EXISTS codex_ingestion_files (
    session_file_path VARCHAR PRIMARY KEY,
    file_size_bytes BIGINT NOT NULL,
    file_mtime TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

File-change gate:

- Skip file when both size and mtime match the last successful row in `codex_ingestion_files`.
- Process file when unseen or changed.

Checkpoint query:

```sql
SELECT
  event_timestamp AS last_ts,
  total_tokens_cumulative AS last_total_tokens_cumulative
FROM codex_session_details
WHERE session_id = ?
ORDER BY event_timestamp DESC, total_tokens_cumulative DESC
LIMIT 1;
```

Behavior:

- For changed files:
  - If no checkpoint exists for a session, ingest from the beginning of the file.
  - If checkpoint exists, skip older rows and only process tail rows where:
    - `event_timestamp > last_ts`, or
    - `event_timestamp == last_ts AND total_tokens_cumulative >= last_total_tokens_cumulative`.
- The `>=` boundary is intentional: it re-includes the last ingested token row so stream-level dedupe can safely drop boundary duplicates.
- This design assumes logs are append-only in practice (full-file rewrites are out of scope). Head truncation is acceptable because checkpointing is based on token event time + cumulative total, not file line number.
- Idempotency is preserved by dedupe + DB conflict handling on `(session_id, total_tokens_cumulative)`.

## 7. Error Handling

Rules:

- Fail fast on malformed JSON line with file path + line number in error.
- Skip non-token events silently.
- Skip token events with `info == null`.
- Fail if a token event cannot be attributed to a known model (`current_model_code` missing).
- Fail if cumulative totals decrease within a session file.
- Fail if cumulative-total deltas are inconsistent with `last_token_usage` deltas on deduped unique events.
- Fail if duplicate cumulative totals have conflicting token payload values.
- If session ID missing, mark file as failed and continue other files.
- Emit counters:
  - files_scanned
  - files_ingested
  - files_skipped_unchanged
  - sessions_ingested
  - token_rows_raw
  - token_rows_deduped
  - token_rows_skipped_info_null
  - token_rows_skipped_before_checkpoint
  - duplicate_rows_skipped
  - monotonicity_errors
  - delta_consistency_errors
  - parse_errors

## 8. Validation Queries

Sanity checks after ingestion:

```sql
-- No duplicate cumulative totals per session
SELECT session_id, total_tokens_cumulative, COUNT(*) AS c
FROM codex_session_details
GROUP BY 1, 2
HAVING c > 1;
```

```sql
-- Sessions with token data but missing model attribution
SELECT session_id, COUNT(*) AS rows_missing_model
FROM codex_session_details
WHERE model_code IS NULL
GROUP BY 1;
```

```sql
-- Cumulative totals should be unique by PK, and monotonic in file order.
WITH ordered AS (
  SELECT
    session_id,
    event_timestamp,
    event_line_number,
    total_tokens_cumulative,
    LAG(total_tokens_cumulative) OVER (
      PARTITION BY session_id
      ORDER BY event_timestamp, event_line_number
    ) AS prev_total_tokens
  FROM codex_session_details
)
SELECT
  session_id,
  event_timestamp,
  event_line_number,
  prev_total_tokens,
  total_tokens_cumulative
FROM ordered
WHERE prev_total_tokens IS NOT NULL
  AND total_tokens_cumulative <= prev_total_tokens;
```

```sql
-- Delta check: cumulative total delta must equal per-row incremental total_tokens.
WITH ordered AS (
  SELECT
    session_id,
    event_timestamp,
    event_line_number,
    total_tokens_cumulative,
    total_tokens,
    LAG(total_tokens_cumulative) OVER (
      PARTITION BY session_id
      ORDER BY event_timestamp, event_line_number
    ) AS prev_total_tokens
  FROM codex_session_details
)
SELECT
  session_id,
  event_timestamp,
  event_line_number,
  prev_total_tokens,
  total_tokens_cumulative,
  total_tokens
FROM ordered
WHERE prev_total_tokens IS NOT NULL
  AND (total_tokens_cumulative - prev_total_tokens) <> total_tokens;
```

```sql
-- Compare raw vs dedup ratio
SELECT
  COUNT(*) AS deduped_rows,
  COUNT(DISTINCT session_id) AS sessions_with_tokens
FROM codex_session_details;
```

## 9. Implementation Notes (Python Package)

Suggested modules:

- `src/codex_token_usage/ingestion/schemas.py`
- `src/codex_token_usage/ingestion/parser.py`
- `src/codex_token_usage/ingestion/dedupe.py`
- `src/codex_token_usage/ingestion/repository.py`
- `src/codex_token_usage/ingestion/service.py`

Suggested stack:

- JSON parsing: `orjson` (line-by-line)
- DB: `duckdb` Python API
- CLI entrypoint later via `typer`

## 10. UUID Type Guidance

- `session_id` and `turn_id` should be stored as DuckDB native `UUID`.
- Parsing layer should validate UUID format before insert.
- If a value is malformed, fail the file ingestion with a descriptive error.
