# Coding Agent Token Monitor

CLI tools for monitoring token usage of coding agents. Currently supports Claude Code, OpenCode, Codex, Gemini, Pi, and AntiGravity.

(The Gemini tool is migrated from [ceshine/gemini-token-usage](https://github.com/ceshine/gemini-token-usage); this repository supersedes the original (now deprecated) project.)

## Supported Agent Monitors

This repository provides individual CLI tools for tracking, ingesting, and displaying token usage statistics for various coding agents:

| Coding Agent / Tool | Package / Command | Documentation |
| :--- | :--- | :--- |
| **Claude Code** | `claude-token-usage` | [Claude Code Usage Guide](./README_CLAUDE_CODE.md) |
| **OpenCode** | `opencode-token-usage` | [OpenCode Usage Guide](./README_OPENCODE.md) |
| **Codex** | `codex-token-usage` | [Codex Usage Guide](./README_CODEX.md) |
| **Gemini CLI** | `gemini-token-usage` | [Gemini CLI Usage Guide](./README_GEMINI_CLI.md) |
| **Pi** | `pi-token-usage` | [Pi Usage Guide](./README_PI.md) |
| **AntiGravity** | `agy-token-usage` | [AntiGravity Usage Guide](./README_ANTIGRAVITY_CLI.md) |

---

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

---

## Release Notes

### 0.5.0 (breaking)

- **AntiGravity Raw `model_id` Persistence & Upsert:**
  - Persists the raw numeric `model_id` (`BIGINT`) alongside `model_code` in `antigravity_usage_events`.
  - Replaces destructive full-table replacement during ingestion with transactional idempotent upsert (`INSERT ... ON CONFLICT (event_key) DO UPDATE`), ensuring historical events are preserved even if local SQLite session files are removed.
- **Run the Backfill Migration Script:**
  - After upgrading the tool, run the one-time backfill script to populate `model_id` and re-normalize placeholder `model_code` values on historical records:
    ```bash
    uv run python scripts/one_time_scripts/backfill_antigravity_model_id.py
    ```
  - A timestamped backup of the database is automatically created in `cache/<db_name>.YYMMDD-HHmm.duckdb` before applying updates.
  - You can test beforehand without committing changes by adding the `--dry-run` flag.

### 0.4.0 (breaking)

- **Requires DuckDB 2.0.** The `duckdb` dependency floor is raised from `1.4.4` to `2.0.0`.
- **Database files are not backward compatible.** DuckDB 2.0 writes a new on-disk storage format when it creates a database file (e.g., when compacting with `scripts/compact_db.py`). Versions 0.3.x and earlier, which ship DuckDB 1.x, fail to open such files with `IO Error: Trying to read a database file with version number 999`.
- **Upgrade every install that shares the database**, including copies installed via `uv tool install`:

  ```bash
  uv tool install --reinstall .
  ```

---

## Internal Workflows

### Pricing Data Collection

To provide accurate cost estimates, the tools fetch the latest model pricing and context window information from [models.dev](https://models.dev/).

*   **Source:** `https://models.dev/api.json`
*   **Caching:** The pricing data is cached locally (default: `~/.local/share/coding-agent-token-monitors/prices.json`) for 24 hours to reduce network requests and improve performance. An active internet connection is required to update this cache. You can override the cache location by setting the `PRICE_CACHE_PATH` environment variable.

---

## Acknowledgements

- The [AGENTS.md](./AGENTS.md) was adapted from the examples in this blog post: [Getting Good Results from Claude Code](https://www.dzombak.com/blog/2025/08/getting-good-results-from-claude-code/).
- This project uses the [models.dev API](https://models.dev/api.json) to retrieve model pricing and context window data for calculating token costs.
