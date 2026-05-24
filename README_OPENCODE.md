# OpenCode Token Monitor Usage

This document describes how to use the OpenCode token usage monitor. For installation and shared workflows, see the main [README.md](./README.md).

## OpenCode Usage

### Ingest command

Ingest OpenCode assistant message token usage from the OpenCode SQLite database into the local DuckDB database:

```bash
uv run opencode-token-usage ingest
```

Useful options:

- `--source-db`, `-s`: Path to the OpenCode SQLite database (default: `~/.local/share/opencode/opencode.db`).
- `--database-path`, `-d`: Path to DuckDB file (default: `~/.local/share/coding-agent-token-monitors/token_usage.duckdb`).
- `--full-refresh`: Ignore the ingestion checkpoint and re-upsert all assistant rows.
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Ingest from the default OpenCode database
uv run opencode-token-usage ingest

# Ingest from a custom OpenCode database path
uv run opencode-token-usage ingest --source-db /path/to/opencode.db

# Force a full re-ingestion of all records
uv run opencode-token-usage ingest --full-refresh
```

After ingestion, the command prints an ingestion summary and token usage statistics for the last 7 days.

### Stats command

Aggregate and print daily OpenCode token usage and costs from the local DuckDB database:

```bash
uv run opencode-token-usage stats
```

Useful options:

- `--database-path`, `-d`: Path to DuckDB file (default: `~/.local/share/coding-agent-token-monitors/token_usage.duckdb`).
- `--timezone`, `-tz`: Timezone for daily grouping, e.g. `UTC` or `America/New_York`. Defaults to local system time.
- `--since`: Include only usage on/after a date (`YYYY-MM-DD`).
- `--until`: Include only usage before a date, exclusive (`YYYY-MM-DD`).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Stats from the default database
uv run opencode-token-usage stats

# Filter by date range and timezone
uv run opencode-token-usage stats --since 2026-01-01 --timezone America/New_York
```
