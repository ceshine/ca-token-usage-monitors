# Coding Agent Token Monitor

CLI tools for monitoring token usage of coding agent. Currently supports Codex.

## Installation

1. Clone this repository:

```bash
git clone https://github.com/ceshine/ca-token-usage-monitors.git
cd ca-token-usage-monitors
```

2. Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Sync dependencies:

```bash
uv sync --frozen
```

## Codex Usage

### Stats command

Run daily token/cost statistics from the local DuckDB database:

```bash
uv run codex-token-usage stats
```

Useful options:

- `--ingest`: Ingest session logs before computing stats.
- `--database-path`, `-d`: Path to DuckDB file (default: `data/token_usage.duckdb`).
- `--sessions-root`, `-s`: Codex sessions directory (used with `--ingest`, default: `~/.codex/sessions`).
- `--timezone`, `-tz`: Timezone for daily grouping, e.g. `UTC` or `America/New_York`.
- `--since`: Include usage on/after a date (`YYYY-MM-DD`).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Stats from existing database
uv run codex-token-usage stats

# Ingest latest sessions first, then show stats
uv run codex-token-usage stats --ingest

# Use a custom sessions path and custom database
uv run codex-token-usage stats --ingest \
  --sessions-root /path/to/.codex/sessions \
  --database-path data/token_usage.duckdb

# Filter by date and timezone
uv run codex-token-usage stats --since 2026-01-01 --timezone America/New_York
```
