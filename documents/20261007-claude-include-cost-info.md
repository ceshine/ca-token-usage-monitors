# Claude Reported Cost Support — Implementation Plan (2026-10-07)

## Objective

Persist `costUSD` from Claude session logs and expose `--api-reported-costs` on
`claude-token-usage stats` and `claude-token-usage ingest`, matching the reporting
semantics of `pi-token-usage` without changing the default pricing-based reports.

Schema migration is required core work. Historical backfilling via `--full-refresh`
is an optional final step and must not block shipping the core feature.

Unless otherwise noted, source paths below are relative to
`src/coding_agent_usage_monitors/claude_token_usage/`.

## Behavior contract

- Always ingest reported costs when available, regardless of CLI reporting flags.
- Without `--api-reported-costs`, retain existing token-based cost calculations.
- With the flag, use the stored reported cost for each event. Missing costs contribute
  `0.0`, matching Pi; do not silently fall back to calculated costs.
- Preserve missing values as `NULL` in DuckDB and `None` in Python. An explicit zero
  is a known cost, not a missing value.
- The flag on `ingest` affects only its post-ingestion seven-day report.
- Token counts, event counts, date/timezone filters, and `--cwd` filtering remain
  independent of the selected cost source.
- Keep reporting read-only. Schema migration runs through ingestion.
- Do not infer that log-reported amounts necessarily equal actual billed charges.

## Current implementation and constraints

- `ingestion/parser.py` extracts four token counters, but does not read `costUSD`.
- In-memory deduplication uses `(message_id, request_id)` and immediately skips
  entries with matching token counts. That can discard a later cost-bearing entry.
- `ingestion/repository.py` has no cost column and uses `ON CONFLICT ... DO NOTHING`
  for `(session_id, message_id, request_id)`.
- `ingestion/service.py` skips unchanged files and filters older events using
  per-session/per-agent checkpoints.
- `--refresh-metadata` deliberately skips usage ingestion for unchanged files; it
  is not a historical cost backfill mechanism.
- `stats/repository.py` opens DuckDB read-only and has separate filtered/unfiltered
  queries, both of which must support the new field.
- Pi already provides the CLI and aggregation precedent. Its conflict-ignore
  persistence must not be copied as a solution for updating existing costs.

## Step 1 — Confirm the log contract and add parser coverage

Files: `ingestion/parser.py`, `ingestion/schemas.py`,
`tests/claude_ingestion/test_parser.py`.

1. Inspect representative, sanitized Claude assistant entries that contain
   `costUSD`. Confirm its location, whether it is per-message/request rather than
   cumulative, and how it appears across streaming/final entries. The anticipated
   source is the assistant event's top-level `costUSD`; verify before implementing.
   If it is cumulative or attached to a different event type, revise the extraction
   design before proceeding rather than summing it as a per-event amount.
2. Add `reported_cost_total_usd: float | None = None` to `UsageEventRow`, separate
   from `TokenUsageValues`. Claude supplies a total, so do not invent a component
   cost breakdown.
3. Extract and validate the value for eligible usage events:
   - Missing or JSON `null`: `None`.
   - Finite, non-negative integers/floats: normalize to `float`, retaining zero.
   - Boolean, string, container, negative, non-finite, or unrepresentable values:
     raise a contextual `ParseError` with file path and line number.
   - Do not broaden ingestion to synthetic or otherwise ineligible events merely
     because a cost field is present.
4. Make duplicate handling cost-aware, including when token counts are identical:
   - Keep one row per existing deduplication key; never sum duplicate costs.
   - Prefer a final entry's non-null cost over an intermediate entry's cost.
   - A missing cost must not erase a known cost for the same request.
   - Do not let an intermediate entry overwrite an already selected final cost.
   - For equally authoritative entries, use the later non-null cost, including zero.
   - Preserve current authoritative-final token selection while tracking enough
     cost provenance within the parse to apply these rules.
5. Test present/missing/null/zero costs, validation failures, final entries with
   unchanged tokens but new costs, conflicting intermediate/final costs, reversed
   intermediate/final ordering, and duplicate entries that omit a known cost.

## Step 2 — Persist costs and migrate existing databases automatically

File: `ingestion/repository.py`.
Tests: add `tests/claude_ingestion/test_repository.py` and extend
`tests/claude_ingestion/test_service.py`.

1. Add nullable `reported_cost_total_usd DOUBLE` to the new-table definition.
2. Extend `ensure_schema()` with the idempotent migration:

   ```sql
   ALTER TABLE claude_usage_events
   ADD COLUMN IF NOT EXISTS reported_cost_total_usd DOUBLE;
   ```

   No separate migration script is required. Existing rows receive `NULL`, not zero
   or a reconstructed price-table estimate. Do not modify other monitors' tables.
3. Include the cost in event inserts. Replace conflict-ignore behavior with a
   narrowly scoped cost update: use the incoming selected cost when non-null,
   otherwise retain the stored cost. Preserve existing token and identity fields
   on conflict; a general rewrite of historical token accounting is out of scope.
   Update the method name/docstring if needed to make these update semantics clear.
4. Verify migration on an actual temporary DuckDB database created with the old
   schema, including existing rows and an unrelated monitor table. Run
   `ensure_schema()` twice to prove idempotency and preservation of existing data.
5. Test round-trip persistence, null-to-known and known-to-zero updates, missing
   incoming costs preserving known costs, repeated writes without duplicate rows,
   and a later cost-bearing duplicate encountered on a subsequent ingestion run.

This only enriches events that ingestion actually processes. File-state and
checkpoint filtering still prevent a complete historical backfill in the core release.

## Step 3 — Read costs while preserving legacy reporting

Files: `stats/schemas.py`, `stats/repository.py`.
Tests: add `tests/claude_stats/test_claude_stats_repository.py`.

1. Add `reported_cost_total_usd: float | None = None` to `TokenUsageEvent`.
2. Read the new field in both normal and `--cwd`-filtered queries.
3. Detect whether the column exists through schema inspection, not by treating
   every SQL failure as a legacy database. For old schemas, select a typed `NULL`
   expression in its place so default statistics continue to work.
4. Expose the detected schema capability to the reporting service. If reported-cost
   mode is requested on an unmigrated database, raise `StatsRepositoryError` with
   an actionable instruction to run `claude-token-usage ingest` first.
5. Keep the distinction between an absent column and a migrated database with null
   costs: the latter is valid reported-cost input and contributes zero for missing
   values. Explain that schema migration alone does not backfill historical rows.
6. Test both query paths against old and new schemas, preservation of `None` and
   zero, normal reports on old databases, and existing missing-table error behavior.
   Ensure read-only stats never alter the database.

Do not direct users to `--full-refresh` unless the optional final step has shipped.

## Step 4 — Select the cost source during aggregation

File: `stats/service.py`.
Tests: `tests/claude_stats/test_claude_stats_service.py`.

1. Add `use_api_reported_costs: bool = False` to `StatsService`.
2. In reported-cost mode, validate schema support and use each event's stored cost
   when it is not `None`, otherwise `0.0`.
3. Keep `calculate_event_cost()` and its default-mode behavior unchanged, including
   model resolution, fast-model pricing, and long-context pricing.
4. Avoid calling `get_price_spec()` in reported-cost mode. Reading stored costs
   should not require a pricing cache, network access, or a known model name.
5. Test differing reported/calculated totals, explicit zero, missing costs, unknown
   models with reported costs, daily/model/overall aggregates, and unchanged token
   and event counts. Retain filter coverage and add a test asserting that the
   pricing loader is not called in reported-cost mode.

## Step 5 — Wire both CLI commands and document the behavior

File: `cli.py`.
Tests: add `tests/claude_cli/test_cli.py` using Typer's `CliRunner` and mocked pricing.
Documentation: `README_CLAUDE_CODE.md`.

1. Add `--api-reported-costs` to `stats` and `ingest`, defaulting to false.
2. Pass the selection through `_collect_stats_report()` and
   `_emit_last_7_days_stats()` into `StatsService`. Do not pass it into the ingestion
   parser as a switch controlling whether costs are stored.
3. State in option help that missing reported costs count as zero, not estimates.
4. Test flag forwarding for both commands, default behavior, help output, and the
   actionable old-schema error. Add a temporary-log/database integration test
   covering ingestion followed by both cost-reporting modes.
5. Document usage examples with `uv run`, automatic schema migration, and the
   limitations of existing data:
   - Normal ingestion migrates the schema and records costs for processed events.
   - Historical rows can remain `NULL` because unchanged files and old events are
     not reprocessed; zeros in a report do not prove a request was free.
   - `--refresh-metadata` does not backfill costs.
   - Deleted logs or logs without `costUSD` cannot supply missing historical costs.
6. Keep the existing table renderer unless a minimal cost-source label is needed;
   a report-layout redesign or changes to Pi are not part of this feature.

## Step 6 — Validate and ship the core feature

Write focused regression tests alongside each step, then run:

```bash
uv run pytest tests/claude_ingestion tests/claude_stats tests/claude_cli
uvx ruff check .
uvx ruff format --line-length 120
uv run pytest
```

Mock external pricing access and use temporary session directories/databases. Do
not validate by modifying the user's live database. Review any formatter changes
and keep unrelated source changes out of this feature.

Core acceptance criteria:

- [ ] Supported Claude log costs survive parsing, deduplication, and persistence.
- [ ] Automatic schema migration is repeatable and preserves existing data.
- [ ] Both CLI commands support the flag with Pi-compatible missing-cost behavior.
- [ ] Default calculated reports remain unchanged, including on legacy schemas.
- [ ] Reported-cost mode works without loading pricing data and preserves filters.
- [ ] Old-schema reported-cost requests fail with a useful migration instruction.
- [ ] Tests and documentation explicitly distinguish migration from backfilling.

The core feature is complete without `--full-refresh`. No dedicated migration or
backfill script is required for this release.

## Step 7 — Optional: add `--full-refresh` for historical cost backfilling

Implement only after the core feature is complete. This is an additive convenience
for recovering costs from still-available historical logs, not a prerequisite for
reported-cost support or schema migration.

Files: `cli.py`, `ingestion/service.py`, and repository changes only if needed.
Tests: `tests/claude_ingestion/test_service.py`, `tests/claude_cli/test_cli.py`.
Documentation: `README_CLAUDE_CODE.md`.

1. Add `full_refresh: bool = False` to ingestion orchestration and expose
   `--full-refresh` on `claude-token-usage ingest`.
2. When enabled, bypass both unchanged-file checks and per-session/per-agent
   checkpoints. Parse every discovered session file with `checkpoint=None`.
3. When combined with `--refresh-metadata`, full refresh takes precedence over its
   metadata-only fast path: refresh metadata and process usage events.
4. Reuse Step 2's cost-update semantics to fill historical null costs and accept
   selected non-null corrections without duplicating rows or clearing known costs.
   Do not delete/recreate the shared database or promise a general token rebuild.
5. Retain per-file transactions, error reporting, and file-state updates after
   successful persistence. A failure must not mark an unprocessed file as current.
6. Test unchanged-file backfill, rows older than checkpoints, main/subagent files,
   idempotent repeated refreshes, both refresh flags together, preservation of
   known costs when logs omit them, and failed-file bookkeeping.
7. Document and test the completed upgrade workflow:

   ```bash
   uv run claude-token-usage ingest --full-refresh --api-reported-costs
   uv run claude-token-usage stats --api-reported-costs
   ```

   Include `--database-path` when upgrading a non-default database. Recommend a
   database backup before bulk backfilling; never imply that missing source logs
   can be reconstructed from token counts. Once this option ships, update migration
   guidance to distinguish `ingest` (schema upgrade) from `ingest --full-refresh`
   (schema upgrade plus available historical cost backfill).
8. Rerun the focused and full validation commands from Step 6 before shipping this
   optional feature.
