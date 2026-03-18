# Token Ingestion Technical Design (Claude Code Sessions -> DuckDB)

Created: 2026-03-18

## 1. Scope

This document defines the data-ingestion design for extracting token usage from local Claude Code session logs at `~/.claude/projects/` and loading it into DuckDB.

In scope:

- Parse session logs (`.jsonl`) in a streaming-safe way.
- Extract session metadata (session ID, project name, slug, cwd, version).
- Extract `assistant` message usage events and deduplicate robustly.
- Persist idempotently to DuckDB.

Out of scope:

- Final reporting/aggregation CLI UX.
- Cost computation/pricing.

## 2. Source Log Observations

Confirmed from local files at `~/.claude/projects/{project-hash}/{sessionId}.jsonl`:

- **Entry types**: `file-history-snapshot`, `user`, `system`, `assistant`, `last-prompt`.
- **Only `assistant` entries have token usage** in `message.usage`.
- **Usage fields**: `input_tokens`, `output_tokens`, `cache_creation_input_tokens` (optional, default 0), `cache_read_input_tokens` (optional, default 0).
- **`speed` field**: present on some entries, values observed: `null`, `"standard"` (handle `"fast"` when encountered).
- **`costUSD`**: never present in current logs.
- **Models seen**: `claude-opus-4-6`, `claude-sonnet-4-6`, `claude-haiku-4-5-20251001`, `<synthetic>` (all-zero usage, should be skipped).
- **Subagent files**: at `{sessionId}/subagents/agent-{agentId}.jsonl`, have `isSidechain: true` and `agentId` field. Every entry in a subagent file carries the same `agentId`; main session file entries have no `agentId`.
- **Dedup key**: `message.id` + `requestId` (both always present on assistant entries).
- **Streaming intermediates**: when extended thinking is active, Claude Code emits 2–3 assistant entries sharing the same `(message_id, requestId)`. Intermediates have `message.stop_reason == null` and low partial `output_tokens`; the final entry has `stop_reason` set (e.g. `"tool_use"`, `"end_turn"`) and the authoritative token counts.
- **Session ID**: `sessionId` field on every entry (UUID string).
- **No `~/.config/claude/projects/`** directory exists on this system (only `~/.claude/projects/`), but both are checked for portability.

## 3. Data Model

### 3.1 `claude_session_metadata`

One row per session.

```sql
CREATE TABLE IF NOT EXISTS claude_session_metadata (
    session_id VARCHAR PRIMARY KEY,
    project_name VARCHAR,
    slug VARCHAR,
    cwd VARCHAR,
    version VARCHAR,
    session_file_path VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.2 `claude_usage_events`

One row per deduplicated assistant message.

Field mapping:

- `message_id` and `request_id` are required NOT NULL on all non-synthetic assistant entries.
- `model_code` is `message.model`, with `-fast` suffix appended when `speed == "fast"`.
- Token fields come from `message.usage`.

```sql
CREATE TABLE IF NOT EXISTS claude_usage_events (
    session_id VARCHAR NOT NULL,
    message_id VARCHAR NOT NULL,
    request_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_line_number BIGINT NOT NULL,
    model_code VARCHAR NOT NULL,
    is_sidechain BOOLEAN NOT NULL DEFAULT FALSE,
    agent_id VARCHAR,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0,
    cache_read_input_tokens BIGINT NOT NULL DEFAULT 0,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, message_id, request_id)
);
```

### 3.3 `claude_ingestion_files`

File bookkeeping (same pattern as codex).

```sql
CREATE TABLE IF NOT EXISTS claude_ingestion_files (
    session_file_path VARCHAR PRIMARY KEY,
    file_size_bytes BIGINT NOT NULL,
    file_mtime TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## 4. Parsing and Attribution Rules

### 4.1 Session identity

1. Scan entries for the first entry with `sessionId`.
2. Use `sessionId` as `session_id`.
3. Extract `slug`, `cwd`, `version` from the same entry if available.
4. Extract `agentId` from the same entry; present on subagent files, absent (None) on main session files.
5. Derive `project_name` from the directory path after `projects/`.

### 4.2 Assistant entry filtering

An entry is a candidate usage event when:

- `type == "assistant"`
- `message.usage` exists and is non-null
- `message.model` exists and is not `"<synthetic>"`
- `message.usage.input_tokens` and `message.usage.output_tokens` exist

### 4.3 Model code derivation

- Base: `message.model`
- If `speed == "fast"`, append `-fast` suffix

### 4.4 Required fields

- `message.id` and `requestId` are required on all non-synthetic assistant entries with usage.
- If either is missing, raise `ParseError` (fail the file).

### 4.5 Deduplication

Claude Code emits multiple JSONL entries sharing the same `(message_id, request_id)` when a response involves extended thinking or streaming. Intermediates have `message.stop_reason == null` and carry partial token counts; the final entry has `stop_reason` set and the authoritative token counts.

In-memory dedup during parsing:

1. Track seen `(message_id, request_id)` pairs in a `dict[tuple, UsageEventRow]`.
2. Duplicate with same tokens: skip silently (`duplicate_rows_skipped += 1`).
3. Duplicate with different tokens and `stop_reason != null`: **replace** the stored row (final entry supersedes streaming partials).
4. Duplicate with different tokens and `stop_reason == null`: skip silently (`duplicate_rows_skipped += 1`); a final entry is expected later.

DB-level: `INSERT ... ON CONFLICT DO NOTHING` on `(session_id, message_id, request_id)`.

## 5. Ingestion Workflow

1. Discover files under roots (`~/.claude/projects/`, `~/.config/claude/projects/`, optional `CLAUDE_CONFIG_DIR` env var) using `**/*.jsonl` glob.
2. For each file, compute `(session_file_path, file_size_bytes, file_mtime)`.
3. Check `claude_ingestion_files`:
   - If unchanged since last successful ingestion, skip the file.
   - If changed or unseen, continue.
4. Parse session identity from entries.
5. Load checkpoint from DB for `(session_id, agent_id)` — the latest ingested row matching both fields (NULL-safe). This scopes the checkpoint to the individual file: main session files use `agent_id = NULL`, each subagent file uses its own `agent_id`.
6. Parse stream and build:
   - `session_metadata` row.
   - `candidate_usage_rows` filtered by checkpoint:
     - If no checkpoint: include all candidate rows.
     - If checkpoint: include rows where `event_timestamp > last_ts` or `(event_timestamp == last_ts AND (message_id, request_id) >= (last_message_id, last_request_id))`.
7. Write in one transaction per changed file:
   - Upsert `claude_session_metadata`.
   - Insert usage rows into `claude_usage_events` with `ON CONFLICT DO NOTHING`.
   - Upsert `claude_ingestion_files` with latest metadata.

## 6. Idempotency and Incremental Re-runs

Use a hybrid strategy:

- File-level change pruning via `claude_ingestion_files`.
- File-level tail ingestion via checkpoint scoped to `(session_id, agent_id)` from `claude_usage_events`.

Checkpoint query:

```sql
SELECT
  CAST(event_timestamp AS VARCHAR) AS last_ts,
  message_id AS last_message_id,
  request_id AS last_request_id
FROM claude_usage_events
WHERE session_id = ? AND agent_id IS NOT DISTINCT FROM ?
ORDER BY event_timestamp DESC, message_id DESC, request_id DESC
LIMIT 1;
```

The `IS NOT DISTINCT FROM` operator handles NULL equality correctly: a NULL `agent_id` (main session file) only matches rows where `agent_id IS NULL`, and a non-NULL `agent_id` (subagent file) only matches rows for that specific agent. This prevents a main session file's checkpoint from filtering out earlier-timestamped events in a subagent file that shares the same `session_id`.

## 7. Error Handling

Rules:

- Fail fast on malformed JSON line with file path + line number in error.
- Skip non-assistant entries silently.
- Skip `<synthetic>` model entries silently.
- Skip assistant entries without `message.usage`.
- Duplicate dedup keys with conflicting tokens: keep first if new entry has `stop_reason == null`; replace with new entry if `stop_reason != null`.
- If session ID missing, mark file as failed and continue other files.
- Emit counters:
  - files_scanned
  - files_ingested
  - files_skipped_unchanged
  - sessions_ingested
  - usage_rows_raw
  - usage_rows_deduped
  - usage_rows_skipped_synthetic
  - usage_rows_skipped_before_checkpoint
  - duplicate_rows_skipped
  - parse_errors

## 8. Validation Queries

```sql
-- No duplicate (message_id, request_id) per session
SELECT session_id, message_id, request_id, COUNT(*) AS c
FROM claude_usage_events
GROUP BY 1, 2, 3
HAVING c > 1;
```

```sql
-- Sessions with usage data summary
SELECT
  session_id,
  COUNT(*) AS event_count,
  SUM(input_tokens) AS total_input,
  SUM(output_tokens) AS total_output
FROM claude_usage_events
GROUP BY 1
ORDER BY total_input + total_output DESC;
```

## 9. Implementation Notes (Python Package)

Modules:

- `src/coding_agent_usage_monitors/claude_token_usage/ingestion/schemas.py`
- `src/coding_agent_usage_monitors/claude_token_usage/ingestion/errors.py`
- `src/coding_agent_usage_monitors/claude_token_usage/ingestion/parser.py`
- `src/coding_agent_usage_monitors/claude_token_usage/ingestion/repository.py`
- `src/coding_agent_usage_monitors/claude_token_usage/ingestion/service.py`

Stack:

- JSON parsing: `orjson` (line-by-line)
- DB: `duckdb` Python API
- CLI: `typer`
