# Claude Code Token Monitor Usage

This document describes how to use the Claude Code token usage monitor. For installation and shared workflows, see the main [README.md](./README.md).

## Claude Code Usage

### Ingest command

Ingest Claude Code session token usage into the local DuckDB database:

```bash
uv run claude-token-usage ingest
```

Session files are auto-discovered from the following locations (in order):

- `~/.claude/projects/`
- `~/.config/claude/projects/`
- Paths from the `CLAUDE_CONFIG_DIR` environment variable (comma-separated)

Useful options:

- `--database-path`, `-d`: Path to DuckDB file (default: `~/.local/share/coding-agent-token-monitors/token_usage.duckdb`).
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Ingest from auto-discovered Claude session directories
uv run claude-token-usage ingest

# Ingest into a custom database path
uv run claude-token-usage ingest --database-path data/token_usage.duckdb
```

After ingestion, the command prints an ingestion summary and token usage statistics for the last 7 days.

### Stats command

Aggregate and print daily Claude Code token usage and costs from the local DuckDB database:

```bash
uv run claude-token-usage stats
```

Useful options:

- `--database-path`, `-d`: Path to DuckDB file (default: `~/.local/share/coding-agent-token-monitors/token_usage.duckdb`).
- `--timezone`, `-tz`: Timezone for daily grouping, e.g. `UTC` or `America/New_York`. Defaults to local system time.
- `--since`: Include only usage on/after a date (`YYYY-MM-DD`).
- `--until`: Include only usage before a date, exclusive (`YYYY-MM-DD`).
- `--cwd`: Restrict to sessions whose working directory exactly matches this path.
- `--verbose`, `-v`: Enable info-level logs.

Examples:

```bash
# Stats from the default database
uv run claude-token-usage stats

# Filter by date range and timezone
uv run claude-token-usage stats --since 2026-01-01 --until 2026-02-01 --timezone America/New_York

# Show stats only for a specific project
uv run claude-token-usage stats --cwd /path/to/my-project
```

### Cost calculation

Costs are computed at report time from token counts and model prices (sourced from models.dev), so changes to the pricing rules apply retroactively to all ingested data.

Prompt-cache writes are priced as 1-hour writes (2× the input token price), matching Claude Code's default for plan subscribers. The session logs and the models.dev price data do not distinguish 5-minute writes (1.25× the input token price) from 1-hour writes, so sessions that actually used 5-minute caching are slightly overestimated. Cache-write prices from models.dev are ignored; the cache-write price is derived from the input token price (including the long-context tier above 200k tokens).

### Known limitations

**Context compaction is not tracked.** When Claude Code compacts a conversation (manually via `/compact` or automatically), the API call that produces the summary is not written to the session logs as an `assistant` entry with token usage. The logs only record a `compact_boundary` system entry (with context-size metadata such as `preTokens`/`postTokens`) followed by the summary as a user message. Because the monitor only counts usage that the logs report, token counts and costs for sessions with compactions are underestimated. Estimating compaction usage from the boundary metadata was evaluated and rejected because it did not reliably close the gap against Claude Code's own totals (see [TDD_CLAUDE_INGESTION.md](./documents/TDD_CLAUDE_INGESTION.md#46-untracked-usage-context-compaction)).
