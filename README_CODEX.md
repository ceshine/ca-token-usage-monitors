# Codex Token Monitor Usage

This document describes how to use the Codex token usage monitor. For installation and shared workflows, see the main [README.md](./README.md).

## Codex Usage

### Ingest command

Ingest Codex session logs into the local DuckDB database:

```bash
uv run codex-token-usage ingest
```

Useful options:

- `--database-path`, `-d`: Path to DuckDB file (default: `data/token_usage.duckdb`).
- `--sessions-root`, `-s`: Codex sessions directory (default: `~/.codex/sessions`).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Ingest from default Codex sessions path
uv run codex-token-usage ingest

# Ingest from a custom sessions path and database
uv run codex-token-usage ingest \
  --sessions-root /path/to/.codex/sessions \
  --database-path data/token_usage.duckdb
```

After ingestion, the command prints an ingestion summary and token usage statistics for the last 7 days.

### Stats command

Run daily token/cost statistics from the local DuckDB database:

```bash
uv run codex-token-usage stats
```

Useful options:

- `--database-path`, `-d`: Path to DuckDB file (default: `data/token_usage.duckdb`).
- `--timezone`, `-tz`: Timezone for daily grouping, e.g. `UTC` or `America/New_York`.
- `--since`: Include usage on/after a date (`YYYY-MM-DD`).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Stats from existing database (read-only)
uv run codex-token-usage stats

# Filter by date and timezone
uv run codex-token-usage stats --since 2026-01-01 --timezone America/New_York
```
