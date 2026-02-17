# Gemini Token Usage Refactor Plan

Created: 2026-02-17

## Objective

Refactor `src/gemini_token_usage` to mirror the structure and layering style used by `src/codex_token_usage`, while preparing for a future DuckDB ingestion pipeline.

Required constraints:

1. Create a new `preprocessing` subpackage for Gemini log processing (do **not** use `ingestion`).
2. Reuse shared `model_pricing` for price lookup (remove Gemini-specific pricing implementation from the runtime path).
3. Rename CLI command `stats` to `preprocess`.
4. Add optional `--stats` flag on `preprocess` to print token usage/cost statistics for the processed JSONL file.
5. Do not implement DuckDB ingestion in this change (reserve `ingestion` name for future work).

## Current State Summary

`src/codex_token_usage` is organized by concern:

- `cli.py` for command wiring and option parsing.
- `ingestion/` subpackage for discovery/parsing/dedupe/persistence.
- `stats/` subpackage for schemas/repository/service/render.
- Uses shared `model_pricing.get_price_spec` in stats service.

`src/gemini_token_usage` is currently flat:

- `calculate_token_usage.py` mixes CLI orchestration, file resolution, conversion trigger, parsing, aggregation, cost logic, and rendering.
- `convert_logs.py` and `simplify_logs.py` contain preprocessing logic, each with their own CLI entrypoint patterns.
- `price_spec.py` duplicates pricing-fetch/cache logic already available in `src/model_pricing/price_spec.py`.
- `__main__.py` wires Typer commands but delegates into these flat modules.

## Target Structure (Post-Refactor)

```text
src/gemini_token_usage/
  __init__.py
  __main__.py
  cli.py
  preprocessing/
    __init__.py
    convert.py
    simplify.py
    resolve_input.py
    schemas.py
  stats/
    __init__.py
    schemas.py
    service.py
    render.py
```

Notes:

- Keep `preprocessing` strictly for filesystem/log normalization (raw log -> simplified JSONL + event extraction helpers).
- Keep `stats` for usage aggregation and cost computation/rendering.
- `ingestion/` is intentionally absent for now.

## File-by-File Migration Map

1. `src/gemini_token_usage/__main__.py`
   - Reduce to Codex-style module entrypoint:
     - `from .cli import TYPER_APP`
     - `if __name__ == "__main__": TYPER_APP()`

2. `src/gemini_token_usage/cli.py` (new)
   - Move Typer app definition and command registration from current `__main__.py`.
   - Replace command `stats` with `preprocess`.
   - Add `--stats` boolean option to `preprocess` for printing stats after preprocessing.
   - Keep `simplify` command.
   - Move logging setup helper(s) here, similar to Codex `cli.py`.

3. `src/gemini_token_usage/preprocessing/convert.py` (new)
   - Move from `convert_logs.py`:
     - `get_last_timestamp`
     - `convert_log_file`
     - `run_log_conversion`

4. `src/gemini_token_usage/preprocessing/simplify.py` (new)
   - Move from `simplify_logs.py`:
     - `simplify_record`
     - `run_log_simplification`

5. `src/gemini_token_usage/preprocessing/resolve_input.py` (new)
   - Extract directory/file resolution logic currently duplicated in stats/simplify flows:
     - find `telemetry.log` / `.gemini/telemetry.log`
     - find `telemetry.jsonl` / `.gemini/telemetry.jsonl`
   - Return typed result objects for clearer orchestration.

6. `src/gemini_token_usage/preprocessing/schemas.py` (new)
   - Add typed dataclasses for preprocessing outcomes (e.g., resolved paths, conversion counters).

7. `src/gemini_token_usage/stats/service.py` (new)
   - Move from `calculate_token_usage.py`:
     - `UsageStats` (or split into `stats/schemas.py`)
     - `calculate_cost` (renamed to align with Codex style, e.g., `calculate_event_cost`)
     - log parsing/aggregation logic from `process_log_file`
     - top-level orchestration from `analyze_token_usage` (excluding CLI concerns)

8. `src/gemini_token_usage/stats/schemas.py` (new)
   - Introduce typed event and aggregate schemas, mirroring Codex stats pattern.

9. `src/gemini_token_usage/stats/render.py` (new)
   - Move table rendering (`print_usage_table` and daily cost table rendering) out of service.

10. `src/gemini_token_usage/price_spec.py`
    - Stop using this module in Gemini runtime path.
    - Replace calls with `from model_pricing import get_price_spec`.
    - Remove this file after import migration is complete.

11. Legacy flat modules (`calculate_token_usage.py`, `convert_logs.py`, `simplify_logs.py`)
    - Remove these files after logic is moved into `preprocessing/` and `stats/`.
    - Update all imports and entrypoints to the new package layout directly (no compatibility shims).

## Sequenced Implementation Plan

### Phase 1: Package scaffolding and CLI alignment

1. Add `gemini_token_usage/cli.py` and `preprocessing/`, `stats/` packages.
2. Convert `__main__.py` into lightweight entrypoint.
3. Rename `stats` CLI command to `preprocess`.
4. Add `--stats` option wiring on `preprocess`.

### Phase 2: Move preprocessing concerns

1. Relocate conversion/simplification logic into `preprocessing/`.
2. Extract shared input-path resolution into `preprocessing/resolve_input.py`.
3. Replace direct cross-imports (`convert_logs` <-> `simplify_logs`) with intra-subpackage imports.

### Phase 3: Move stats concerns

1. Move aggregation/cost logic into `stats/service.py`.
2. Move dataclasses into `stats/schemas.py`.
3. Move rich table rendering into `stats/render.py`.
4. Keep command behavior and output formatting parity.

### Phase 4: Pricing unification

1. Replace Gemini pricing imports with shared `model_pricing.get_price_spec`.
2. Remove runtime dependency on local `gemini_token_usage.price_spec`.
3. Ensure cache-path behavior remains compatible with existing env usage (`PRICE_CACHE_PATH`).

### Phase 5: Cleanup

1. Remove legacy flat modules once migration is complete.
2. Ensure no internal references remain to removed module paths.
3. Keep `project.scripts` stable unless entrypoint symbol path changes.

## Testing Plan

Add/adjust tests to match new package boundaries:

1. `tests/gemini_preprocessing/test_convert.py`
   - incremental append behavior via last timestamp
   - malformed/incomplete JSON handling
2. `tests/gemini_preprocessing/test_simplify.py`
   - level 0/1/2/3 behavior and filtering
   - archiving vs removal path behavior
3. `tests/gemini_stats/test_service.py`
   - cost calculation including cached tokens and >200k tier
   - date aggregation and timezone handling
4. `tests/gemini_cli/test_cli.py`
   - `preprocess` and `simplify` command wiring
   - `preprocess --stats` prints the statistics tables
   - error-code and parameter-validation behavior
5. Pricing integration test
   - verify Gemini path calls `model_pricing.get_price_spec` (monkeypatch target should be shared module).

Verification commands after implementation:

- `uvx ruff check .`
- `uvx ruff format --line-length 120`
- `uv run pytest`

## Key Decisions and Guardrails

1. Keep CLI output/format stable while changing internal structure; intentional command rename is `stats` -> `preprocess`.
2. Do not maintain backward compatibility for removed flat modules in this project.
3. Isolate filesystem/log mutation code in `preprocessing` to make future DuckDB `ingestion` straightforward.
4. Use shared pricing module as single source of truth to avoid divergence and duplicate network/cache logic.

## Out of Scope for This Refactor

1. Implementing Gemini DuckDB ingestion (`gemini_token_usage/ingestion`).
2. Changing pricing model semantics beyond swapping to shared provider.
3. Redesigning table output format beyond parity-preserving extraction.
