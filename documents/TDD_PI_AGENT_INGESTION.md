# Token Ingestion Technical Design (Pi Agent Sessions -> DuckDB)

Created: 2026-04-14
Status: Draft

## 1. Scope

This document defines the ingestion design for extracting token usage from local Pi (`pi-mono`, <https://github.com/badlogic/pi-mono>) agent session logs at `~/.pi/agent/sessions/` and loading it into DuckDB.

In scope:

- Parse Pi session JSONL logs in a streaming-safe way.
- Extract session metadata (session ID, cwd, version).
- Extract `assistant` message usage events and persist idempotently to DuckDB.
- Support incremental, checkpointed re-runs.
- Persist Pi's reported per-message cost verbatim as a **reference-only** signal. (The authoritative cost is computed later by the stats layer against `common.model_pricing`, matching the OpenCode pattern; that layer is designed in a separate TDD.)

Out of scope:

- Reporting, aggregation, and cost-recomputation logic (covered by a future stats package, analogous to the OpenCode stats service).
- Re-deriving per-tool-call token attribution from tool-call/tool-result entries.

## 2. Source Log Observations

Observed on local machine (2026-04-14), under `~/.pi/agent/sessions/`:

1. **Layout**: `~/.pi/agent/sessions/<cwd-slug>/<iso-timestamp>_<sessionId>.jsonl`
   - `<cwd-slug>` encodes the session's working directory by replacing `/` with `-` and bracketing with `--` (e.g. `/home/ceshine/codebases/personal-projects/coding-agent-token-monitors` → `--home-ceshine-codebases-personal-projects-coding-agent-token-monitors--`).
   - `<iso-timestamp>` uses `-` in place of `:` and `.` (e.g. `2026-04-13T15-42-45-133Z`).
   - `<sessionId>` is a UUID that also appears inside the first entry.
2. **Entry types seen**: `session`, `model_change`, `thinking_level_change`, `message`.
3. **Session metadata entry** (first line):
   ```json
   {"type":"session","version":3,"id":"<uuid>","timestamp":"<iso>","cwd":"<abs path>"}
   ```
4. **Message entries** carry `id`, `parentId`, `timestamp` (ISO, entry level), and an inner `message` object. Roles: `user`, `assistant`, `toolResult`.
5. **Only `assistant` message entries carry token usage**, via `message.usage`:
   ```json
   {
     "input": 3057,
     "output": 73,
     "cacheRead": 0,
     "cacheWrite": 0,
     "totalTokens": 3130,
     "cost": {"input":0, "output":0, "cacheRead":0, "cacheWrite":0, "total":0}
   }
   ```
   All five token fields were present on every assistant row in the sample; `totalTokens` was always present. `cost.total` may be `0` when using free providers or when the user is on a provider subscription — i.e. Pi's reported cost is not guaranteed to reflect list prices. This motivates persisting it verbatim as a reference column only (see §3.2); authoritative cost is a stats-layer concern and is not ingested.
6. **Provider/model dimensions** on assistant rows: `message.api` (e.g. `anthropic-messages`), `message.provider` (e.g. `opencode`), `message.model` (e.g. `minimax-m2.5-free`), `message.stopReason` (e.g. `toolUse`, `stop`), `message.responseId`.
7. **No streaming duplicates observed**: every assistant entry has a unique entry-level `id`. Pi appears to log the final response only, not streaming intermediates — so entry-level `id` is a safe primary key.
8. **Timestamps**: entry-level `timestamp` is an ISO-8601 string; `message.timestamp` is a ms-epoch integer. Both are present on assistant rows.
9. **Model / thinking level**: communicated out-of-band via `model_change` and `thinking_level_change` entries. Assistant rows already echo the effective model in `message.model`, so these are dimension-redundant for per-message ingestion but may be useful for future audit.

## 3. Data Model

### 3.1 `pi_session_metadata`

One row per Pi session.

```sql
CREATE TABLE IF NOT EXISTS pi_session_metadata (
    session_id VARCHAR PRIMARY KEY,
    session_version INTEGER NOT NULL,
    cwd VARCHAR NOT NULL,
    session_started_at TIMESTAMPTZ NOT NULL,
    session_file_path VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.2 `pi_usage_events`

One row per assistant message.

```sql
CREATE TABLE IF NOT EXISTS pi_usage_events (
    session_id VARCHAR NOT NULL,
    message_id VARCHAR NOT NULL,
    parent_id VARCHAR,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_line_number BIGINT NOT NULL,

    provider_code VARCHAR,
    model_code VARCHAR,
    stop_reason VARCHAR,

    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL DEFAULT 0,
    cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT,

    -- Reference-only: copied verbatim from Pi's `message.usage.cost.*`. May be
    -- zero under subscriptions / free tiers. Not the authoritative cost; the
    -- stats layer (out of scope) is responsible for recomputation.
    pi_reported_cost_input_usd DOUBLE,
    pi_reported_cost_output_usd DOUBLE,
    pi_reported_cost_cache_read_usd DOUBLE,
    pi_reported_cost_cache_write_usd DOUBLE,
    pi_reported_cost_total_usd DOUBLE,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, message_id)
);
```

Index:

```sql
CREATE INDEX IF NOT EXISTS idx_pi_usage_checkpoint
    ON pi_usage_events (session_id, event_timestamp, message_id);
```

### 3.3 `pi_ingestion_files`

File-level bookkeeping, matching the Claude/Codex pattern.

```sql
CREATE TABLE IF NOT EXISTS pi_ingestion_files (
    session_file_path VARCHAR PRIMARY KEY,
    file_size_bytes BIGINT NOT NULL,
    file_mtime TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## 4. Parsing and Attribution Rules

### 4.1 Session identity

Pi has a canonical carrier for session metadata (the line-1 `session` entry), so we trust it and fail fast on structural violations. However, `cwd` is *also* encoded in the parent directory name, which gives us a second independent source; we use it as a defensive fallback for that single field.

1. Parse line 1 of the file. If `type != "session"`, raise `ParseError` — malformed Pi session file. No row-scanning fallback: the Pi format guarantees the session entry is line 1 and no other entry type carries `id` / `version` / `cwd` / session-`timestamp`.
2. From the `session` entry, read:
   - `id` → `session_id` (required).
   - `version` → `session_version` (required).
   - `timestamp` → `session_started_at` (required, ISO-8601 → TIMESTAMPTZ).
   - `cwd` → `cwd` (see step 4 for fallback).
3. Cross-check: the `sessionId` in the filename suffix MUST equal the `session` entry's `id`. Mismatch raises `ParseError`.
4. **`cwd` resolution (defensive)**:
   - If the `session` entry contains a non-empty `cwd`, use it.
   - Otherwise, decode `cwd` from the parent directory name: strip the leading and trailing `--` brackets, then replace `-` with `/` (inverse of Pi's encoding, e.g. `--home-ceshine-codebases-personal-projects-foo--` → `/home/ceshine/codebases/personal-projects/foo`). Log a warning with the file path noting that `cwd` was recovered from the directory layout.
   - If both sources are unavailable (malformed directory name *and* missing field), raise `ParseError`.
   - Rationale: Claude Code's logs were later found to omit some metadata fields that the parser assumed were always present. Pi's format has a different shape — `cwd` has a canonical carrier **and** a redundant encoding in the filesystem layout — so we can harden this specific field cheaply without extending the scan past line 1. We do **not** add row-scanning for the other session fields, because no other Pi entry type carries them and a scan would silently mask structurally malformed files.

### 4.2 Assistant entry filtering

An entry contributes a usage row when all hold:

- `type == "message"`.
- `message.role == "assistant"`.
- `message.usage` exists and is a dict.
- `message.usage.input` and `message.usage.output` are present.

Non-assistant and non-message entries are skipped silently.

### 4.3 Required fields on assistant rows

- `id` (entry-level): required, used as `message_id`.
- `timestamp` (entry-level, ISO-8601): required, parsed to aware UTC `TIMESTAMPTZ`.
- `message.usage.input`: required int.
- `message.usage.output`: required int.

Missing any of the above fails the file with `ParseError` (file path + line number).

### 4.4 Optional fields

- `message.usage.cacheRead`, `message.usage.cacheWrite`: default to `0` when absent.
- `message.usage.totalTokens`: stored as NULL when absent.
- `message.usage.cost.*`: stored as NULL when absent; when present copied verbatim into the `pi_reported_cost_*_usd` columns as reference data only.
- `message.provider`, `message.model`, `message.stopReason`, `parentId`: nullable.

### 4.5 Model code

`model_code = message.model` as-is. No suffix derivation (Pi does not expose a separate "speed" dimension in the observed data).

### 4.6 Deduplication

- In-file: entry-level `id` is observed unique; if a duplicate `id` is seen within a single file, raise `ParseError` (structural invariant violation).
- Cross-run: DB-level PK on `(session_id, message_id)` with `INSERT ... ON CONFLICT DO NOTHING`.

## 5. Ingestion Workflow

1. Discover `~/.pi/agent/sessions/**/*.jsonl` (root configurable via CLI; env override `PI_AGENT_DIR` if set).
2. For each file, stat `(session_file_path, file_size_bytes, file_mtime)`.
3. Check `pi_ingestion_files`:
   - Unchanged → skip.
   - Changed / unseen → continue.
4. Parse the first line to obtain session identity.
5. Load per-session checkpoint:
   ```sql
   SELECT
     CAST(event_timestamp AS VARCHAR) AS last_ts,
     message_id AS last_message_id
   FROM pi_usage_events
   WHERE session_id = ?
   ORDER BY event_timestamp DESC, message_id DESC
   LIMIT 1;
   ```
6. Stream remaining lines:
   - Accumulate session_metadata row.
   - For each assistant row, keep only when `event_timestamp > last_ts` or `(event_timestamp == last_ts AND message_id > last_message_id)`; otherwise count under `usage_rows_skipped_before_checkpoint`.
7. Write in a single transaction per file:
   - Upsert `pi_session_metadata`.
   - Insert usage rows into `pi_usage_events` with `ON CONFLICT DO NOTHING`.
   - Upsert `pi_ingestion_files` with observed stat tuple.
8. Emit ingestion counters and fail non-zero on parse errors.

## 6. Idempotency and Incremental Guarantees

- File-level change pruning via `pi_ingestion_files` (fast skip of untouched files).
- Row-level dedup via `(session_id, message_id)` PK.
- Per-session checkpoint `(event_timestamp, message_id)` limits re-reads of appended JSONL tails.
- `--full-refresh` mode bypasses both checkpoint and file-stat skip, re-upserting every assistant row (useful after schema/parser changes).

## 7. Error Handling

Fail fast with file path + line number on:

- Malformed JSON line.
- Line 1 is not a `session` entry (or has missing `id` / `version` / `timestamp`).
- `sessionId` mismatch between filename and `session` entry.
- `cwd` absent from both the `session` entry and the parent directory name (malformed layout).
- Assistant entry missing required fields (`id`, `timestamp`, `usage.input`, `usage.output`).
- Duplicate `id` within the same file.

Soft handling:

- Skip non-`message` entries silently.
- Skip non-assistant message entries silently.
- Missing optional fields → NULL / default.
- `cwd` absent from the `session` entry but recoverable from the parent directory name → log a warning and proceed.

Counters emitted:

- `files_scanned`
- `files_ingested`
- `files_skipped_unchanged`
- `sessions_ingested`
- `usage_rows_raw`
- `usage_rows_persisted`
- `usage_rows_skipped_before_checkpoint`
- `sessions_cwd_recovered_from_path` (sessions where `cwd` had to be decoded from the parent directory name)
- `parse_errors`

## 8. CLI Contract

```
pi-token-usage ingest [--source-dir PATH] [--database-path PATH] [--full-refresh] [--verbose]
```

- `--source-dir/-s`: Pi sessions root (default `~/.pi/agent/sessions`).
- `--database-path/-d`: DuckDB path (default `data/token_usage.duckdb`).
- `--full-refresh`: ignore checkpoint and file-stat cache.
- `--verbose/-v`: info-level logging.

## 9. Validation Queries

```sql
-- No duplicate (session_id, message_id)
SELECT session_id, message_id, COUNT(*) AS c
FROM pi_usage_events
GROUP BY 1, 2
HAVING c > 1;
```

```sql
-- Null required token fields (must be zero)
SELECT COUNT(*) AS bad_rows
FROM pi_usage_events
WHERE input_tokens IS NULL OR output_tokens IS NULL;
```

```sql
-- Per-provider/model sanity
SELECT provider_code, model_code, COUNT(*) AS events,
       SUM(input_tokens) AS input_sum,
       SUM(output_tokens) AS output_sum,
       SUM(pi_reported_cost_total_usd) AS pi_reported_cost_sum
FROM pi_usage_events
GROUP BY 1, 2
ORDER BY events DESC;
```

```sql
-- Orphaned usage rows without a session metadata row
SELECT COUNT(*) AS orphan_rows
FROM pi_usage_events e
LEFT JOIN pi_session_metadata s ON s.session_id = e.session_id
WHERE s.session_id IS NULL;
```

## 10. Implementation Plan (Repository)

Mirror the Claude/OpenCode layout:

- `src/coding_agent_usage_monitors/pi_token_usage/__main__.py`
- `src/coding_agent_usage_monitors/pi_token_usage/cli.py`
- `src/coding_agent_usage_monitors/pi_token_usage/ingestion/schemas.py`
- `src/coding_agent_usage_monitors/pi_token_usage/ingestion/errors.py`
- `src/coding_agent_usage_monitors/pi_token_usage/ingestion/parser.py`
- `src/coding_agent_usage_monitors/pi_token_usage/ingestion/repository.py`
- `src/coding_agent_usage_monitors/pi_token_usage/ingestion/service.py`

Tests (`tests/pi_ingestion/`):

- `test_parser.py` — session-identity extraction, assistant-row extraction, missing-field failures, duplicate-id failures, and the `cwd` directory-name fallback (both success and "directory name unparseable" failure paths).
- `test_repository.py` — upsert behavior for metadata / files / usage rows, checkpoint query.
- `test_service.py` — end-to-end file ingestion using a synthetic JSONL fixture with (a) a main session, (b) an appended tail (incremental), (c) a corrupt-line fixture.

Fixtures: small JSONL files under `tests/pi_ingestion/fixtures/` containing a `session` line plus a handful of `message` lines with assistant usage.

## 11. Finalized Decisions

1. Entry-level `message.id` is the sole dedup key per session. No `(message_id, response_id)` composite is needed based on observed data; revisit if streaming intermediates appear in later Pi versions.
2. **Cost policy (ingest-side only)**: persist Pi's reported `cost.*` verbatim into `pi_reported_cost_*_usd` as reference-only. Do **not** recompute or transform costs during ingestion. Rationale: under provider subscriptions / free tiers Pi reports zero cost, so the source value cannot be treated as truth — but it is still worth retaining for audit diffs against list-price estimates computed downstream. Authoritative cost recomputation is a stats-layer concern and is deferred to a separate TDD.
3. `model_change` / `thinking_level_change` entries are not ingested in this phase. The effective `model` is already recorded on each assistant row.
4. Historical append/upsert model (same as OpenCode): if source messages are later deleted/compacted, warehouse rows are retained.
