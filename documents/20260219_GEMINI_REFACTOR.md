# Gemini Ingestion Refactor Plan (2026-02-19)

## Objective

Refactor Gemini ingestion to separate source bookkeeping concerns from ingestion execution, while ensuring that paths discovered via `--all-active` are handled identically to manually supplied `input_paths`.

## Planned Changes

1. Create a dedicated bookkeeping module:
   - Add `src/gemini_token_usage/ingestion/source_bookkeeping.py`.
   - Encapsulate source lifecycle and active-source management logic here.

2. Move source lifecycle logic out of `IngestionService` into bookkeeping:
   - Active-source loading.
   - Missing-file detection.
   - `--all-active --auto-deactivate` decision flow.
   - Source reconciliation behavior:
     - new source registration
     - source reactivation
     - project path move handling/conflict checks

3. Keep `IngestionService` focused on ingestion execution:
   - Consume final candidate JSONL paths from CLI/bookkeeping flow.
   - Keep file-state/checkpoint idempotency checks.
   - Parse JSONL usage rows and persist usage events.
   - Update source/file bookkeeping metadata after successful ingestion.

4. Extend repository with a bulk deactivation mutation:
   - In `src/gemini_token_usage/ingestion/repository.py`, add:
     - `deactivate_sources(project_ids: list[UUID]) -> int` (or similar).
   - Implement as a single SQL update for efficiency.
   - Keep repository DB-focused; do not move filesystem existence checks into repository.

5. Unify CLI path handling and preprocessing:
   - In `src/gemini_token_usage/cli.py`, build a unified path set composed of:
     - explicit `input_paths`
     - paths selected through `--all-active` bookkeeping flow
   - Run the same preprocessing pipeline over the full unified set.
   - This ensures `--all-active` paths receive the same preprocessing behavior as manual paths.
   - Pass only preprocessed/resolved JSONL paths into ingestion execution.

6. Update and expand tests:
   - Add focused tests for the new bookkeeping module.
   - Adjust ingestion service tests to reflect narrowed service responsibilities.
   - Add/extend CLI tests to verify parity between explicit `input_paths` and `--all-active` paths.
   - Add repository tests for bulk deactivation behavior.

## Expected Outcomes

- Clear separation of concerns between source lifecycle bookkeeping and ingestion execution.
- Consistent preprocessing behavior across explicit and `--all-active` paths.
- Repository remains persistence-focused while supporting efficient bulk mutation APIs.
- Improved testability and maintainability of the ingestion code path.
