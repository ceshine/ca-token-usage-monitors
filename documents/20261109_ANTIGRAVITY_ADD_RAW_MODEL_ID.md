# AntiGravity Raw Model ID Persistence & Upsert Plan (2026-11-09)

## Objective

1. Persist the raw numeric `model_id` alongside the normalized `model_code` in `antigravity_usage_events` so future model mapping updates can re-resolve model codes without relying on local SQLite conversation files still existing on disk.
2. Replace the destructive full-table delete-and-insert (`replace_all_events`) in `IngestionRepository` with an idempotent upsert (`update_all_events`) so historical events from removed local SQLite files are preserved across future ingestion runs.
3. Provide a one-time migration script to backfill `model_id` and re-normalize placeholder `model_code` values for all previously ingested events in DuckDB.

## Planned Changes

### 1. Schema & Repository Upsert (`antigravity_token_usage/ingestion`)

- **`src/coding_agent_usage_monitors/antigravity_token_usage/ingestion/schemas.py`**:
  - Add `model_id: int | None = None` to `RawUsageEvent` and `UsageEventRow`.
- **`src/coding_agent_usage_monitors/antigravity_token_usage/ingestion/repository.py`**:
  - In `ensure_schema()`:
    - Include `model_id BIGINT` in the `CREATE TABLE IF NOT EXISTS antigravity_usage_events` DDL.
    - Execute `ALTER TABLE antigravity_usage_events ADD COLUMN IF NOT EXISTS model_id BIGINT` so pre-existing databases automatically receive the column during normal ingestion.
  - Rename `replace_all_events(events: list[UsageEventRow])` to `update_all_events(events: list[UsageEventRow])`:
    - Remove `DELETE FROM antigravity_usage_events`.
    - Upsert rows using `INSERT INTO antigravity_usage_events (...) VALUES (...) ON CONFLICT (event_key) DO UPDATE SET ...`, updating `discovery_source`, `session_id`, `event_timestamp`, `model_code`, `model_id` (using `COALESCE(EXCLUDED.model_id, antigravity_usage_events.model_id)`), `provider_id`, token counters, message/response IDs, and `ingested_at = NOW()`.
- **`src/coding_agent_usage_monitors/antigravity_token_usage/ingestion/service.py`**:
  - Update `IngestionService.ingest()` to call `self._repository.update_all_events(canonical_events)`.

### 2. Parser & Deduplication (`antigravity_token_usage/ingestion`)

- **`src/coding_agent_usage_monitors/antigravity_token_usage/ingestion/parser.py`**:
  - Add `resolve_raw_model_id(usage_model_id: int | None, meta_model_id: int | None, inherited_model_id: int | None = None) -> int | None`:
    - Returns the first non-zero numeric ID in precedence order (`usage_model_id` -> `meta_model_id` -> `inherited_model_id`).
  - In `parse_database()`:
    - Track `last_resolved_gen_model_id: int | None` alongside `last_resolved_gen_model`.
    - Populate `model_id` on every emitted `RawUsageEvent` across `gen_metadata` (primary and retry usages) and `steps` (primary and retry usages).
- **`src/coding_agent_usage_monitors/antigravity_token_usage/ingestion/dedupe.py`**:
  - In `merge_events()`:
    - Select `winning_model_id` from the merged candidate events, preferring the `model_id` from events with non-default `model_code` (`model_code != "gemini-internal-model"`), falling back to the first non-`None` `model_id`.
    - Pass `model_id=winning_model_id` into `UsageEventRow`.

### 3. One-Time Migration Script (`scripts/one_time_scripts/backfill_antigravity_model_id.py`)

Create a standalone Typer CLI script (`uv run python scripts/one_time_scripts/backfill_antigravity_model_id.py [--database-path ...] [--dry-run]`) that operates directly on the DuckDB database in a single transaction without requiring local SQLite conversation files:

1. **Ensure Schema**:
   - Run `ALTER TABLE antigravity_usage_events ADD COLUMN IF NOT EXISTS model_id BIGINT`.
2. **Backfill `model_id` from `model_code` where `model_id IS NULL`**:
   - **Placeholder codes (`model_placeholder_m<N>`)**: Parse `<N>` and set `model_id = 1000 + N`.
   - **Legacy placeholder codes (`antigravity-model-<N>`)**: Parse `<N>` and set `model_id = N`.
   - **Normalized `model_code` values**: Build a reverse mapping `PRIMARY_MODEL_ID_BY_CODE: dict[str, int]` from `NUMERIC_MODEL_MAP` (and `model_placeholder_m<N>` entries in `BASE_ALIASES`), taking the primary (first/lowest) numeric ID when multiple IDs map to the same canonical `model_code`:
     - e.g., `gemini-2.5-flash-thinking` -> `313`, `claude-4-sonnet` -> `281`, `claude-4-opus` -> `290`, `claude-4.5-sonnet` -> `333`, `claude-4.5-haiku` -> `340`, `claude-opus-4-5` -> `1011`, `gemini-3.1-pro` -> `1036`, `gemini-3.6-flash-high` -> `1071`, `gemini-3.6-flash-medium` -> `1072`, `gemini-3.6-flash-low` -> `1073`, `gemini-3.5-flash-high` -> `1132`, plus all 1-to-1 mappings (`1318` for `gemini-3.8-flash-high`, `1418` for `gemini-4-argon-medium`, etc.).
3. **Re-normalize `model_code`**:
   - For any row whose `model_id` (or existing `model_code`) now resolves to a canonical model name via `resolve_model_code(usage_model_id=model_id, raw_model_name=model_code, meta_model_id=None, inherited_model=None)` that differs from the stored `model_code` (such as `model_placeholder_m418` -> `gemini-4-argon-medium`), update `model_code` to the canonical value.
4. **Summary Output**:
   - Render a `rich` summary table showing total rows, rows with `model_id` backfilled, rows with `model_code` updated, and breakdown by `(model_code, model_id)`.

### 4. Tests & Documentation

- **`tests/antigravity_ingestion/test_parser.py`**:
  - Verify `event.model_id` is extracted for `gen_metadata`, retries, `steps`, and inherited generation models.
- **`tests/antigravity_ingestion/test_dedupe.py`**:
  - Verify `merge_events()` preserves `model_id` from the winning model event.
- **`tests/antigravity_ingestion/test_repository.py`**:
  - Rename `replace_all_events` tests to `update_all_events`.
  - Verify `model_id` is persisted, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS model_id` works on legacy table schemas, and `update_all_events` preserves pre-existing rows whose `event_key` is not in the new batch while updating conflicting `event_key` rows.
- **`tests/antigravity_ingestion/test_service.py`**:
  - Update tests for `update_all_events` semantics (e.g., an ingestion run with zero discovered databases preserves existing rows rather than deleting them).
- **`documents/TDD_ANTIGRAVITY_INGESTION.md`**:
  - **Section 1 (Scope)** & **Section 3 (Package Layout)**: Replace full-table replacement descriptions with transactional upsert semantics (`update_all_events`).
  - **Section 2 (Finalized Product Decisions)**: Update Decision 8 (full replacement -> idempotent upsert preserving historical rows when source `.db` files are deleted) and Decisions 15 & 23 (aborting the upsert on any per-database parse failure).
  - **Section 5 (DuckDB Data Model)**: Add `model_id BIGINT` to the `antigravity_usage_events` table DDL and column notes.
  - **Section 9.2 (Model resolution)** & **Section 10.3 (Merge rules)**: Document raw `model_id` extraction precedence (`usage_model_id` -> `meta_model_id` -> `inherited_model_id`) and winning `model_id` selection during deduplication.
  - **Section 10.4 (Full replacement transaction -> Upsert transaction)**: Replace the `DELETE` + `INSERT` workflow with `INSERT ... ON CONFLICT (event_key) DO UPDATE` (`update_all_events`), and specify that zero discovered databases leaves existing rows intact.
  - **Section 12 (Error Handling)** & **Section 13.3 / 13.6 (Test Plan)**: Update error-handling and service/repository test narratives from table replacement/emptying to upsert preservation.

## Expected Outcomes

- Every newly ingested AntiGravity usage event stores both its raw numeric `model_id` and normalized `model_code`.
- Historical events in DuckDB are never lost when local SQLite session databases are cleaned up.
- Existing DuckDB rows have `model_id` backfilled and any placeholder `model_code` strings re-normalized.
