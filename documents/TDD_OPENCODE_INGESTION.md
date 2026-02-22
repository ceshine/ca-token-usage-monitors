# Token Ingestion Technical Design (OpenCode SQLite -> DuckDB)

Created: 2026-02-22
Status: Draft

## 1. Scope

This document defines the ingestion design for extracting OpenCode token usage from the local OpenCode SQLite database and loading it into DuckDB for analytics in this repository.

In scope:

- Read OpenCode data from local SQLite storage (`opencode.db`).
- Ingest assistant message-level token usage and related dimensions (project/session/model/provider).
- Support idempotent incremental ingestion with checkpointing.
- Handle source evolution and malformed records with explicit error policy.

Out of scope:

- Reconstructing per-tool-call token attribution from `part` records.
- Rebuilding OpenCode internals from filesystem-only JSON blobs.
- Real-time streaming ingestion (this is batch/poll ingestion).

## 2. Research Summary

## 2.1 Internet findings (official docs/changelog)

1. OpenCode has a first-party `db` command that queries SQLite and supports JSON->SQLite migration, indicating SQLite is the supported canonical store in current versions.
2. OpenCode v1.2.0 release notes explicitly call out migration to SQLite and introduction of event model changes around parts/events.
3. OpenCode troubleshooting docs describe persistent local data under OpenCode home and discuss local data folders and migration/cleanup flows.

References:

- OpenCode CLI `db` command docs: <https://opencode.ai/docs/reference/commands/db>
- OpenCode v1.2.0 release notes: <https://github.com/sst/opencode/releases/tag/v1.2.0>
- OpenCode troubleshooting docs: <https://opencode.ai/docs/troubleshooting/common-issues>

## 2.2 Local repository findings (`~/.local/share/opencode/`)

Observed on local machine (queried on 2026-02-22):

1. Storage contains `opencode.db` plus WAL/SHM files and `storage/` JSON directories.
2. SQLite tables include:
   - `message`, `part`, `session`, `project`, `todo`, plus migration/internal tables.
3. `message` table is the broadest canonical source for assistant usage:
   - 3,152 messages total.
   - 2,573 assistant messages.
   - all assistant messages include `data.tokens`.
4. `part` table also has token payloads on `type = "step-finish"`, but coverage starts later and does not cover full history.
5. Message JSON files under `storage/message/` are incomplete vs SQLite:
   - file messages: 2,251
   - DB messages: 3,152
   - 901 messages exist in DB but not as JSON files.
6. Token payload shape in assistant messages:
   - required in practice: `input`, `output`, `reasoning`, `cache.read`, `cache.write`
   - `total` is nullable in many rows (not safe as NOT NULL).
7. `cost` can be `int` or `float`; non-zero values exist.
8. Assistant timestamps:
   - `message.time_created` / `message.time_updated` (ms epoch) always present at row level.
   - `data.time.completed` is missing in a small minority of assistant rows.

Design implication:

- Canonical ingestion source must be SQLite `message` (+ joins to `session`/`project`), not filesystem JSON blobs.

## 3. Product Decisions

1. **Primary source**: OpenCode SQLite database (`opencode.db`) via read-only SQL queries.
2. **Event grain**: one ingestion row per assistant message (`message.id`).
3. **Token schema**:
   - `input_tokens`, `output_tokens`, `reasoning_tokens`, `cache_read_tokens`, `cache_write_tokens` are required integers.
   - `total_tokens` is nullable.
4. **Cost schema**: `cost_usd` is nullable DOUBLE.
5. **Event timestamp**:
   - primary: `message.time_created` (ms epoch -> TIMESTAMPTZ).
   - optional completion timestamp from `data.time.completed` when present.
6. **Dimensions**:
   - `session_id`, `project_id`, `project_worktree`, `model_code`, `provider_code`, `agent`, `mode`, `finish_reason`.
   - session metadata is normalized into `opencode_sessions`.
7. **Incremental checkpoint**:
   - tuple `(last_time_updated_ms, last_message_id)` derived from ingested rows in `opencode_message_usage`.
8. **Idempotency key**: `message_id` primary key in DuckDB with upsert semantics.
9. **Fallback/full sync**:
   - support a periodic full resync mode (`--full-refresh`) to reconcile rare checkpoint drift or historical edits.
10. **Failure semantics**:
   - fail fast on malformed required token fields.
   - skip non-assistant rows by query filter.

## 4. Source Contract

## 4.1 Source DB path

Default source path:

- `~/.local/share/opencode/opencode.db`

Configurable via CLI option.

## 4.2 Required relational shape

Required tables and join contract:

- `message(session_id -> session.id)`
- `session(project_id -> project.id)`
- `project(id)`

## 4.3 Message data JSON contract (assistant rows)

Required for ingestion row creation:

- `data.role == "assistant"`
- `data.tokens.input`: int
- `data.tokens.output`: int
- `data.tokens.reasoning`: int
- `data.tokens.cache.read`: int
- `data.tokens.cache.write`: int

Optional:

- `data.tokens.total`: int | null
- `data.cost`: int | float | null
- `data.modelID`, `data.providerID`, `data.mode`, `data.agent`, `data.finish`
- `data.time.completed`

## 5. DuckDB Data Model

## 5.1 `opencode_sessions`

One row per OpenCode session.

```sql
CREATE TABLE IF NOT EXISTS opencode_sessions (
    session_id VARCHAR PRIMARY KEY,
    project_id VARCHAR NOT NULL,
    project_worktree VARCHAR,
    session_title VARCHAR NOT NULL,
    session_directory VARCHAR NOT NULL,
    session_version VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## 5.2 `opencode_message_usage`

One row per assistant message.

```sql
CREATE TABLE IF NOT EXISTS opencode_message_usage (
    message_id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,

    message_created_at TIMESTAMPTZ NOT NULL,
    message_completed_at TIMESTAMPTZ,

    provider_code VARCHAR,
    model_code VARCHAR,
    agent VARCHAR,
    mode VARCHAR,
    finish_reason VARCHAR,

    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL,
    cache_write_tokens BIGINT NOT NULL,
    total_tokens BIGINT,

    cost_usd DOUBLE,
    source_time_updated_ms BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Indexes:

```sql
-- Checkpoint lookup and incremental ordering
CREATE INDEX IF NOT EXISTS idx_opencode_usage_checkpoint
    ON opencode_message_usage (source_time_updated_ms, message_id);

-- Project-scoped time aggregation
CREATE INDEX IF NOT EXISTS idx_opencode_usage_project_updated
    ON opencode_message_usage (project_id, source_time_updated_ms);

-- Model-scoped time aggregation
CREATE INDEX IF NOT EXISTS idx_opencode_usage_model_updated
    ON opencode_message_usage (model_code, source_time_updated_ms);
```

## 6. Ingestion Workflow

1. Resolve source DB path.
2. Ensure DuckDB schema exists.
3. Load checkpoint from `opencode_message_usage`:

```sql
SELECT source_time_updated_ms AS last_time_updated_ms, message_id AS last_message_id
FROM opencode_message_usage
ORDER BY source_time_updated_ms DESC, message_id DESC
LIMIT 1;
```

4. Optional fast-skip gate:
   - query `SELECT MAX(time_updated) FROM message WHERE json_extract(data, '$.role') = 'assistant'` from SQLite;
   - if no checkpoint exists, ingest;
   - if max source `time_updated` is NULL or `<= last_time_updated_ms`, skip ingestion.
5. Open SQLite read connection and stream assistant rows ordered by `(time_updated, id)`:

```sql
SELECT
    m.id AS message_id,
    m.session_id,
    m.time_created,
    m.time_updated,
    m.data,
    s.project_id,
    s.title,
    s.directory,
    s.version,
    p.worktree
FROM message m
JOIN session s ON s.id = m.session_id
JOIN project p ON p.id = s.project_id
WHERE json_extract(m.data, '$.role') = 'assistant'
  AND (
    ? IS NULL
    OR m.time_updated > ?
    OR (m.time_updated = ? AND m.id > ?)
  )
ORDER BY m.time_updated ASC, m.id ASC;
```

6. For each row:
   - parse `m.data` JSON;
   - validate required token fields;
   - convert ms-epoch timestamps to aware datetimes;
   - map session rows and message metrics.
7. Write in batches (e.g., 1,000 rows) with `INSERT ... ON CONFLICT (message_id) DO UPDATE`.
8. Track max `(time_updated, id)` processed in memory.
9. In same transaction:
   - upsert session rows into `opencode_sessions`;
   - upsert usage rows.
10. Emit ingestion counters and fail non-zero on parse failures.

## 7. Idempotency and Incremental Guarantees

1. `message_id` primary key makes re-runs idempotent.
2. Checkpoint tuple `(time_updated, id)` ensures deterministic pagination and resume order.
3. Upsert on conflict handles historical edits where a message’s JSON payload changes.
4. `--full-refresh` mode re-reads all assistant messages and re-upserts rows, useful after parser/schema changes.

## 8. Error Handling

Fail fast with descriptive errors on:

- Source DB missing/unreadable.
- Required tables missing.
- Invalid JSON in `message.data`.
- Missing required token fields or wrong types for required fields.
- Invalid timestamp conversions.

Soft handling:

- Nullable `tokens.total` accepted.
- Missing optional fields (`modelID`, `providerID`, `time.completed`) are stored as NULL.

## 9. CLI Contract

Proposed command:

```bash
opencode-token-usage ingest [--source-db PATH] [--database-path PATH] [--full-refresh] [--verbose]
```

Options:

- `--source-db/-s`: OpenCode SQLite path (default `~/.local/share/opencode/opencode.db`).
- `--database-path/-d`: DuckDB path (default `data/token_usage.duckdb`).
- `--full-refresh`: ignore checkpoint and re-upsert all assistant rows.
- `--verbose/-v`: info-level logging.

## 10. Implementation Plan (Repository)

1. Add new package:
   - `src/opencode_token_usage/__main__.py`
   - `src/opencode_token_usage/cli.py`
   - `src/opencode_token_usage/ingestion/schemas.py`
   - `src/opencode_token_usage/ingestion/repository.py`
   - `src/opencode_token_usage/ingestion/source_reader.py`
   - `src/opencode_token_usage/ingestion/service.py`
   - `src/opencode_token_usage/ingestion/errors.py`
2. Mirror codex/gemini ingestion patterns for counters, transaction boundaries, and summary output.
3. Implement repository upserts for both:
   - `opencode_sessions` (by `session_id`)
   - `opencode_message_usage` (by `message_id`)
   in one transaction.
4. Add tests:
   - `tests/opencode_ingestion/test_source_reader.py`
   - `tests/opencode_ingestion/test_repository.py`
   - `tests/opencode_ingestion/test_service.py`
5. Fixtures:
   - minimal synthetic SQLite fixture with `message/session/project` rows and JSON payload variants.

## 11. Validation Queries

Post-ingestion checks:

```sql
-- duplicate message rows (must be zero)
SELECT message_id, COUNT(*)
FROM opencode_message_usage
GROUP BY message_id
HAVING COUNT(*) > 1;
```

```sql
-- null required token fields (must be zero)
SELECT COUNT(*) AS bad_rows
FROM opencode_message_usage
WHERE input_tokens IS NULL
   OR output_tokens IS NULL
   OR reasoning_tokens IS NULL
   OR cache_read_tokens IS NULL
   OR cache_write_tokens IS NULL;
```

```sql
-- row counts by provider/model for sanity
SELECT provider_code, model_code, COUNT(*) AS c
FROM opencode_message_usage
GROUP BY provider_code, model_code
ORDER BY c DESC;
```

```sql
-- current ingestion checkpoint derived from usage table
SELECT source_time_updated_ms, message_id
FROM opencode_message_usage
ORDER BY source_time_updated_ms DESC, message_id DESC
LIMIT 1;
```

```sql
-- orphaned messages without a session dimension row (must be zero)
SELECT COUNT(*) AS orphan_rows
FROM opencode_message_usage m
LEFT JOIN opencode_sessions s ON s.session_id = m.session_id
WHERE s.session_id IS NULL;
```

## 12. Finalized Decisions

1. `part`-level token/cost events are explicitly out of scope for this phase.
2. For deleted/compacted source messages, retain historical rows in DuckDB (append/upsert historical model), and do not implement tombstoning in this phase.
3. Do not implement `opencode db` CLI fallback; direct SQLite access is required and assumed available.

## 12.1 Deletion strategy comparison

Two viable approaches for source-side deletions/compactions:

1. Tombstone/sync-delete model:
   - Behavior: if a source message no longer exists, mark corresponding DuckDB rows deleted (or physically delete).
   - Pros: warehouse mirrors source current state.
   - Cons: more complex reconciliation logic; can remove historical analytics context.

2. Append/upsert historical model (selected):
   - Behavior: ingest new/changed rows; keep already ingested rows even if source later compacts/deletes them.
   - Pros: simpler incremental pipeline, stable historical analytics, lower operational risk.
   - Cons: warehouse may diverge from source live-state row count after compaction/deletion events.
