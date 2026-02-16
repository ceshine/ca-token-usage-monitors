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
2. Take the first event with `type IN ("session_meta", "session_metadata")`.
3. Use `payload.id` as `session_id`.
4. If missing, fail file ingestion with a descriptive error.

### 4.2 Model state machine

Maintain a mutable `current_model_context` while scanning:

- On each `turn_context` event:
  - Update `current_model_code = payload.model` if present.
  - Update `current_model_timestamp = event.timestamp`.
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
- Therefore `total_tokens_cumulative` is expected to be strictly increasing across unique token events, including across model switches.
- Equal totals are treated as duplicates and deduplicated.
- A decrease in cumulative totals is treated as a hard error and ingestion should fail for that file.

## 5. Ingestion Workflow

1. Discover files under `~/.codex/sessions/**/*.jsonl` (sorted path order).
2. For each file, parse stream and build:
   - `session_metadata` row.
   - `deduped_token_rows` list.
3. Write in one transaction per file:
   - Upsert `codex_session_metadata`.
   - Insert deduped rows into `codex_session_details` with conflict handling on `(session_id, total_tokens_cumulative)`.

Recommended SQL pattern:

- Use `MERGE` or `INSERT ... ON CONFLICT DO NOTHING` depending on DuckDB version.

## 6. Idempotency and Incremental Re-runs

Add file-level bookkeeping:

```sql
CREATE TABLE IF NOT EXISTS codex_ingestion_files (
    session_file_path VARCHAR PRIMARY KEY,
    file_size_bytes BIGINT NOT NULL,
    file_mtime TIMESTAMPTZ NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Behavior:

- On each run, compute `(path, size, mtime, sha256)`.
- Skip file if same path and checksum already ingested.
- If checksum changed, reprocess file and upsert data.

## 7. Error Handling

Rules:

- Fail fast on malformed JSON line with file path + line number in error.
- Skip non-token events silently.
- Skip token events with `info == null`.
- Fail if a token event cannot be attributed to a known model (`current_model_code` missing).
- Fail if cumulative totals decrease within a session file.
- Fail if duplicate cumulative totals have conflicting token payload values.
- If session ID missing, mark file as failed and continue other files.
- Emit counters:
  - files_scanned
  - files_ingested
  - sessions_ingested
  - token_rows_raw
  - token_rows_deduped
  - token_rows_skipped_info_null
  - duplicate_rows_skipped
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
-- Compare raw vs dedup ratio
SELECT
  COUNT(*) AS deduped_rows,
  COUNT(DISTINCT session_id) AS sessions_with_tokens
FROM codex_session_details;
```

## 9. Implementation Notes (Python Package)

Suggested modules:

- `src/coding_agent_token_monitor/ingestion/schemas.py`
- `src/coding_agent_token_monitor/ingestion/parser.py`
- `src/coding_agent_token_monitor/ingestion/dedupe.py`
- `src/coding_agent_token_monitor/ingestion/repository.py`
- `src/coding_agent_token_monitor/ingestion/service.py`

Suggested stack:

- JSON parsing: `orjson` (line-by-line)
- DB: `duckdb` Python API
- CLI entrypoint later via `typer`

## 10. UUID Type Guidance

- `session_id` and `turn_id` should be stored as DuckDB native `UUID`.
- Parsing layer should validate UUID format before insert.
- If a value is malformed, fail the file ingestion with a descriptive error.
