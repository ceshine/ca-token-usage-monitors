# AntiGravity Token Monitor Usage

This document describes how to use the AntiGravity token usage monitor. For installation and shared workflows, see the main [README.md](./README.md).

## AntiGravity Usage

The AntiGravity token usage monitor (`agy-token-usage`) tracks token usage from local AntiGravity SQLite conversation databases (across CLI, IDE, and standalone installations) and aggregates daily usage and estimated costs in DuckDB.

### Ingest command

Ingest AntiGravity SQLite conversation databases into the local DuckDB database:

```bash
uv run agy-token-usage ingest
```

When neither `--data-dir` nor `ANTIGRAVITY_DATA_DIR` is set, `.db` files are auto-discovered recursively from the following conversation directories (in priority order):

- `~/.gemini/antigravity/conversations/` (`default`)
- `~/.gemini/antigravity-cli/conversations/` (`cli`)
- `~/.gemini/antigravity-ide/conversations/` (`ide`)
- `~/.gemini/antigravity-backup/conversations/` (`backup`)
- `~/.config/antigravity/conversations/` (`config`)

When `ANTIGRAVITY_DATA_DIR` is set (or `--data-dir` / `-s` is passed), its comma-separated root directories replace the default roots above and assign `discovery_source = "env"`. For each root, the tool scans `<root>/conversations` if that subdirectory exists, or `<root>` directly otherwise.

Useful options:

- `--database-path`, `-d`: Path to the DuckDB database file (default: `~/.local/share/coding-agent-token-monitors/token_usage.duckdb`, or `$XDG_DATA_HOME/coding-agent-token-monitors/token_usage.duckdb` when `XDG_DATA_HOME` is set).
- `--data-dir`, `-s`: Custom root directory or comma-separated list of roots for AntiGravity databases (overrides `ANTIGRAVITY_DATA_DIR` and replaces default discovery roots).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Ingest from auto-discovered AntiGravity conversation directories
uv run agy-token-usage ingest

# Ingest from a custom AntiGravity data directory or comma-separated roots
uv run agy-token-usage ingest --data-dir /path/to/antigravity-data
uv run agy-token-usage ingest -s /path/to/root-a,/path/to/root-b

# Ingest into a custom database path
uv run agy-token-usage ingest --database-path data/token_usage.duckdb
```

After ingestion, the command prints an ingestion summary (`databases_discovered`, `databases_parsed`, `databases_failed`, `raw_events_extracted`, `canonical_events_written`, `duplicate_events_merged`, and `missing_optional_tables`). If any database fails to parse, the command prints the `failed_database` paths, leaves the DuckDB table unchanged, and exits with code `1`. Otherwise, it prints daily token usage and cost statistics for the last 7 days.

---

### Stats command

Aggregate and print daily AntiGravity token usage and estimated costs from the local DuckDB database:

```bash
uv run agy-token-usage stats
```

Useful options:

- `--database-path`, `-d`: Path to the DuckDB database file (default: `~/.local/share/coding-agent-token-monitors/token_usage.duckdb`).
- `--timezone`, `-tz`: Timezone for daily grouping (e.g., `UTC` or `America/New_York`). Defaults to local system time.
- `--since`: Include only usage on/after a date (`YYYY-MM-DD`).
- `--until`: Include only usage before a date, exclusive (`YYYY-MM-DD`).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Display daily usage stats from the default database
uv run agy-token-usage stats

# Filter daily usage stats by date range and timezone
uv run agy-token-usage stats --since 2026-06-01 --until 2026-07-01 --timezone America/New_York

# Read stats from a custom database path
uv run agy-token-usage stats --database-path data/token_usage.duckdb
```

The command renders three tables—**Daily Token Usage**, **Daily Aggregated Usage**, and **Overall Token Usage by Model**—covering request counts, input, output, reasoning, cache read, cache write, and total tokens, along with estimated USD costs.

---

### Cost calculation

Costs are computed at report time from persisted token counts and model prices (sourced from [models.dev](https://models.dev/)). Normalized `gemini-*` and `claude-*` model codes are mapped to `google/<model>` and `anthropic/<model>` price keys (stripping reasoning-effort suffixes such as `-thinking`, `-xhigh`, `-high`, `-medium`, `-low`, `-extra-low`, `-tiered`, and `-max`).

For each event, cost is calculated across input tokens, combined output and reasoning tokens (`output_tokens + reasoning_tokens` billed at the output token rate), cache read tokens, and cache creation tokens (falling back to the input token rate if no cache-write rate is defined). Models without a matching entry in the pricing catalog report `$0.000000`. In particular, `Orchestrator (Argon)` (`gemini-pro-orchestrator-g1-pro`) uses a mixture of different underlying models, so its cost cannot be estimated from token usage alone and it reports `$0.000000`.

---

## Internal Workflows

### Database Discovery and Read-Only Access

- **Canonical Path Deduplication:** Discovered `*.db` files are resolved via `Path.resolve()` so symlinks or overlapping roots are parsed once, retaining the highest-priority `discovery_source` label (`default` > `cli` > `ide` > `backup` > `config` > `env`).
- **Session Attribution:** Each database's `session_id` is derived from its `.db` filename stem.
- **Read-Only Snapshots:** Source SQLite databases are opened in read-only URI mode (`?mode=ro`) with a 5-second `PRAGMA busy_timeout` inside a single deferred read transaction, avoiding lock contention or writes to active AntiGravity databases.

### Protobuf Parsing, Normalization, and Deduplication

- **Wire-Format Extraction:** A built-in Protocol Buffers wire reader extracts token-bearing primary and retry `ModelUsage` records from the required `gen_metadata` table and optional `steps` table, using `trajectory_metadata_blob` (rank `1`) and file modification time (rank `0`) as timestamp fallbacks when explicit generator/step timestamps (rank `3`) are unavailable.
- **Model Resolution:** Both the normalized `model_code` and raw numeric `model_id` are extracted and stored in DuckDB so historical records can be re-normalized if numeric model ID mappings are updated later.
- **Global Deduplication:** Candidate events across rows, tables, and databases are deduplicated by identity keys in preference order (`response:<response_id>`, `provider:<provider_assigned_message_id>`, `message:<message_id>`), including transitive matches. Merged duplicates take the maximum of each token counter, prefer the highest-priority `discovery_source`, and select the highest-ranked (or earliest, on tie) timestamp. Events without any identity key are kept distinct via fallback keys.

### Transactional Upsert

- **All-or-Nothing Parsing:** If any discovered SQLite database fails to open or parse, ingestion aborts without modifying DuckDB.
- **Idempotent Persistence:** On success, canonical events are written to `antigravity_usage_events` in a single DuckDB transaction using `INSERT ... ON CONFLICT (event_key) DO UPDATE`, preserving historical events even if the original `.db` session files are later removed.
