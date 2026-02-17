# Token Ingestion Technical Design (Gemini JSONL -> DuckDB)

Created: 2026-02-17

## 1. Scope

This document defines ingestion design for loading Gemini CLI telemetry usage events from preprocessed `telemetry.jsonl` files into DuckDB for `gemini_token_usage`.

In scope:

- Persist usage events from processed Gemini JSONL files.
- Track all known ingestion sources and their `project_id`.
- Support active/inactive source lifecycle.
- Add `ingest` CLI with positional paths and `--all-active`.
- Add optional auto-deactivation for missing active paths.
- Enforce first-line metadata contract in JSONL.

Out of scope:

- Non-interactive confirmation mode (`--yes`).
- Automatic repair for malformed metadata.

## 2. Finalized Product Decisions

1. Project identity is stored in the first line of each `telemetry.jsonl`.
2. Metadata line format:
   `{"record_type":"gemini_cli.project_metadata","schema_version":1,"project_id":"<uuid>"}`
3. Path tracking granularity is canonical resolved JSONL file path.
4. All path comparisons use canonical path resolution.
5. Ingestion accepts positional paths and `--all-active` together; union + dedupe canonical paths before processing.
6. Ingestion requires preprocessed JSONL only; no raw `.log` ingestion.
7. If a path is new, confirm interactively before adding source tracking row.
8. If a tracked path is inactive and requested, confirm before reactivating.
9. If a metadata `project_id` matches an existing row with a different path, confirm before in-place path update.
10. If old path still exists and contains a valid JSONL with same `project_id`, fail hard (manual resolution required).
11. If two active paths share one `project_id`, fail hard immediately.
12. Declining a required confirmation exits with non-zero.
13. Malformed metadata line fails fast; user must fix manually.
14. `--auto-deactivate` controls active-flag updates for missing active paths. Default run does not change `active`.

## 3. JSONL Metadata Contract

## 3.1 Record shape

First line must be a JSON object with:

- `record_type == "gemini_cli.project_metadata"`
- `schema_version == 1`
- `project_id` parseable as UUID

## 3.2 Preprocess responsibilities

`preprocess` must guarantee metadata exists and remains at line 1:

1. New JSONL output: write metadata line first.
2. Existing JSONL without metadata line: prepend metadata line.
3. Existing JSONL with valid metadata line: keep unchanged.
4. Existing JSONL with malformed metadata line: fail.

## 3.3 Simplify responsibilities

`simplify` must preserve line 1 metadata exactly and only simplify event lines after metadata.

## 4. CLI Contract

## 4.1 New command

```bash
gemini-token-usage ingest [PATH ...] [--all-active] [--auto-deactivate] [--database-path PATH]
```

Options:

- `PATH ...`: optional list of directories or file paths, resolved similarly to `preprocess` path handling.
- `--all-active`: include all active tracked sources from DB.
- `--auto-deactivate`: while running `--all-active`, mark missing active paths as inactive.
- `--database-path/-d`: DuckDB path (default `data/token_usage.duckdb`).

## 4.2 Path resolution rules

For each positional path:

1. Resolve to canonical absolute path.
2. If directory: resolve `telemetry.jsonl`, then `.gemini/telemetry.jsonl`.
3. If `.jsonl` file: use directly.
4. If `.log`, missing JSONL, or unsupported type: fail with exact suggestion:
   `gemini-token-usage preprocess <original_path>`

## 4.3 Confirmation flows

Required interactive prompts:

1. New source path registration.
2. Reactivation of inactive source.
3. In-place path update for existing `project_id` with new path.

If user declines any required prompt: exit code 1.

## 5. DuckDB Data Model

## 5.1 `gemini_ingestion_sources`

Tracks all known source files and source lifecycle.

```sql
CREATE TABLE IF NOT EXISTS gemini_ingestion_sources (
    project_id UUID PRIMARY KEY,
    jsonl_file_path VARCHAR NOT NULL UNIQUE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    file_size_bytes BIGINT,
    file_mtime TIMESTAMPTZ,
    last_ingested_line_number BIGINT NOT NULL DEFAULT 0,
    last_ingested_event_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Notes:

- `project_id` is globally unique in the table.
- Path is canonical and unique.
- `active` controls `--all-active` selection.
- File metadata supports unchanged-file skipping.

## 5.2 `gemini_usage_events`

Stores ingested API response usage rows.

```sql
CREATE TABLE IF NOT EXISTS gemini_usage_events (
    project_id UUID NOT NULL,
    event_line_number BIGINT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    model_code VARCHAR NOT NULL,
    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    thoughts_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    usd_cost DOUBLE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, event_line_number)
);
```

Notes:

- Event uniqueness is `(project_id, event_line_number)` under append-only JSONL assumption.
- `usd_cost` is computed using shared `model_pricing.get_price_spec`.

## 6. Source Reconciliation Logic

For each resolved candidate JSONL path:

1. Read and validate metadata line to get `project_id`.
2. Lookup by canonical `jsonl_file_path`.
3. Lookup by `project_id`.
4. Apply cases:
   - Path row exists and project row is same: continue.
   - Path row exists but project mismatch: fail hard.
   - Project row exists with different path:
     - prompt for in-place path update.
     - before update, validate old path:
       - if old path exists and is valid JSONL with same `project_id`, fail hard (copy/inconsistency).
       - otherwise update row path to new canonical path.
   - Neither exists:
     - prompt for new source registration.
     - if confirmed, insert active row.

Inactive handling:

- If row exists but inactive and source was explicitly targeted or resolved from positional inputs, prompt for reactivation.
- On confirmation set `active = TRUE`; on decline exit 1.

Collision guard:

- If query finds more than one active source for the same `project_id`, fail ingestion immediately.

## 7. Ingestion Workflow

1. Ensure schema exists.
2. Build candidate source set:
   - positional resolved paths.
   - plus active DB paths when `--all-active`.
   - dedupe by canonical path.
3. If `--all-active --auto-deactivate`:
   - for each active tracked path that does not exist, set `active = FALSE`.
4. Reconcile each candidate source using Section 6.
5. For each reconciled source:
   - stat file (`size`, `mtime`).
   - if unchanged vs DB bookkeeping, skip.
   - parse JSONL stream with line numbers.
   - line 1 must be valid metadata and match source `project_id`.
   - ingest only usage rows where `attributes.event.name == "gemini_cli.api_response"`.
   - checkpoint filter: `line_number > last_ingested_line_number`.
   - parse token fields and timestamp with strict validation.
   - compute per-event cost from shared pricing.
   - insert rows with conflict-ignore semantics.
   - update source bookkeeping (`file_size_bytes`, `file_mtime`, `last_ingested_line_number`, `last_ingested_event_timestamp`, `updated_at`) in same transaction.

## 8. Error Handling Rules

Fail fast with descriptive error for:

- Missing or malformed metadata line.
- Invalid UUID in metadata.
- Metadata `project_id` mismatch with tracked source mapping.
- Required confirmation declined.
- Unsupported input path type for ingest.
- Missing resolved JSONL with preprocess command suggestion.
- Active project collision.

Append-only guardrails:

- If current file line count is less than tracked `last_ingested_line_number`, fail (likely rewrite/truncation).
- If file content was rewritten in a way that breaks append-only assumptions, require manual remediation.

## 9. Implementation Plan (Code)

1. Add `src/gemini_token_usage/ingestion/` package:
   - `schemas.py`
   - `repository.py`
   - `parser.py`
   - `service.py`
   - `errors.py`
2. Extend `src/gemini_token_usage/cli.py`:
   - new `ingest` command and options.
   - interactive confirmation prompts.
3. Update preprocessing:
   - add metadata utilities in `src/gemini_token_usage/preprocessing/`.
   - update conversion flow to ensure metadata first line.
   - update simplify flow to preserve metadata first line.
4. Add tests:
   - `tests/gemini_ingestion/test_repository.py`
   - `tests/gemini_ingestion/test_parser.py`
   - `tests/gemini_ingestion/test_service.py`
   - `tests/gemini_cli/test_gemini_ingest_cli.py`
   - preprocessing tests for metadata insertion/preservation.

## 10. Test Matrix (Minimum)

1. New path registration confirmation accepted/declined.
2. Inactive path reactivation confirmation accepted/declined.
3. `project_id` path move confirmation accepted/declined.
4. Path move conflict when old path still valid with same `project_id`.
5. `--all-active` only.
6. Positional + `--all-active` union dedupe.
7. `--auto-deactivate` marks missing active paths inactive.
8. Missing JSONL returns preprocess suggestion.
9. Malformed metadata fails.
10. Duplicate active project rows fail.
11. Idempotent rerun skips unchanged files.
12. Tail ingestion inserts only new lines.
