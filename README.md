# Coding Agent Token Monitor

CLI tools for monitoring token usage of coding agent. Currently supports Codex and Gemini.

(The Gemini tool is migrated from [ceshine/gemini-token-usage](https://github.com/ceshine/gemini-token-usage); this repository supersedes the original (now deprecated) project.)

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

## Gemini Usage

### Prerequisite: Enable OpenTelemetry Support in Gemini CLI

To use this tool, you must enable [local file-based OpenTelemetry support](https://geminicli.com/docs/cli/telemetry/#file-based-output-recommended) in the Gemini CLI.

Add the following `telemetry` configuration to your global settings file (usually at `~/.gemini/settings.json`) or your project-specific `.gemini/settings.json`:

```json
{
  "telemetry": {
    "enabled": true,
    "target": "local",
    "otlpEndpoint": "",
    "outfile": ".gemini/telemetry.log"
  }
}
```

**Important Note**: You may need to create the `.gemini` folder in your project directory if it doesn't already exist.

**Note:** Changes to the `telemetry` configuration in `settings.json` often require restarting your Gemini CLI session for them to take effect.

**Configuration Note:**

The `outfile` setting above (`".gemini/telemetry.log"`) tells the Gemini CLI to save telemetry logs inside the `.gemini` folder of the **current working directory** where the CLI is launched. This is the recommended setup, as it keeps logs specific to each project.

Alternatively, you can set `outfile` to an absolute path (e.g., `"~/.gemini/telemetry.log"`) to aggregate logs from all projects into a single file. However, be aware that this makes it difficult to attribute token usage to specific projects.

### Ingest command

Ingest Gemini usage events into the local DuckDB database. In most cases, you no longer need to run `preprocess` manually; `ingest` preprocesses selected input paths automatically.

```bash
uv run gemini-token-usage ingest [OPTIONS] [INPUT_PATHS]...
```

Arguments:

*   `INPUT_PATHS`: Optional directories or `telemetry.jsonl` files to ingest.
    *   If a **directory** is provided, the tool searches for `telemetry.log` / `telemetry.jsonl` in that directory or its `.gemini` subdirectory.
    *   If a **telemetry.log** file is found, it is converted to `telemetry.jsonl` before ingestion.
    *   If omitted, ingestion can still run with `--all-active` to ingest tracked active sources from the database.

Useful options:

*   `--all-active`: Include all currently active tracked sources from the database.
*   `--auto-deactivate`: With `--all-active`, automatically mark missing active sources as inactive.
*   `-d`, `--database-path PATH`: DuckDB file path (default: `data/token_usage.duckdb`).
*   `--enable-archiving`: Archive raw `telemetry.log` files when preprocessing selected input paths.
*   `--log-simplify-level INTEGER`: Simplification level used while preprocessing `telemetry.log` files (0-3, default: 1).

#### Examples

**Ingest from the current project directory:**

```bash
uv run gemini-token-usage ingest .
```

**Ingest multiple project paths:**

```bash
uv run gemini-token-usage ingest /path/to/project-a /path/to/project-b
```

**Ingest all active tracked sources and auto-deactivate missing ones:**

```bash
uv run gemini-token-usage ingest --all-active --auto-deactivate
```

**Use a custom database path:**

```bash
uv run gemini-token-usage ingest . --database-path data/token_usage.duckdb
```

After ingestion, the command prints a summary and statistics for the last 7 days.

### Stats command

Aggregate and print daily Gemini token usage and costs from the local DuckDB database:

```bash
uv run gemini-token-usage stats [OPTIONS]
```

Useful options:

*   `-d`, `--database-path PATH`: DuckDB file path (default: `data/token_usage.duckdb`).
*   `-tz`, `--timezone TEXT`: Timezone for daily grouping (e.g., `UTC`, `America/New_York`). Defaults to local system time.
*   `--since TEXT`: Include only usage on/after a date (`YYYY-MM-DD`).

#### Examples

**Show stats from the default database:**

```bash
uv run gemini-token-usage stats
```

**Filter by date and timezone:**

```bash
uv run gemini-token-usage stats --since 2026-01-01 --timezone America/New_York
```

### Simplify command (Manually Simplify Logs)

The `simplify` command's main purpose is to increase the simplification level of an existing `.jsonl` log file, making it useful for reducing file size or preparing logs for sharing and archiving.

```bash
uv run gemini-token-usage simplify [OPTIONS] INPUT_FILE_PATH
```

Required argument:

*   `INPUT_FILE_PATH`: The path to the input `.jsonl` file.
    *   If a **directory** is provided, the tool searches for `telemetry.jsonl` within that directory or its `.gemini` subdirectory.


Useful options:

*   `-l`, `--level INTEGER`: The simplification level (0-3). Default: 1.
    *   `0`: No simplification.
    *   `1`: Keep only API requests and responses.
    *   `2`: Level 1 + trim non-essential fields.
    *   `3`: Keep only API responses and essential token usage attributes.
*   `-a`, `--archive-folder PATH`: Folder to archive the original file before simplification. Default: `/tmp`.
*   `-d`, `--disable-archiving`: If set, the original file will be permanently deleted instead of archived. **Use with caution.**

#### Examples

**Simplify logs in the current directory to level 2:**

```bash
uv run gemini-token-usage simplify . -l 2
```

**Simplify a specific file to level 3 (maximum reduction) and disable archiving:**

```bash
uv run gemini-token-usage simplify .gemini/telemetry.jsonl -l 3 --disable-archiving
```

### Managing Disk Space

To prevent OpenTelemetry log files from consuming excessive disk space, especially if you enable a low `log-simplify-level`, it is recommended to periodically process and archive them.

The `--enable-archiving` option moves processed log files to `/tmp` to save space. **Note that files in `/tmp` are typically deleted automatically upon the next system reboot.** This behavior effectively acts as a "trash" mechanism, allowing you to recover the file before a reboot if needed, but ensuring it doesn't permanently occupy disk space.

**⚠️ IMPORTANT WARNING:** Only use `--enable-archiving` if no Gemini CLI instances are currently running and actively writing to the log file. Running with archiving enabled while Gemini CLI is active can lead to file locking issues, data corruption, or data loss.

**Example:**

```bash
uv run gemini-token-usage ingest . --enable-archiving
```

## Internal Workflows

### Gemini Log Conversion and Simplification

The Gemini CLI generates OpenTelemetry logs as a series of concatenated, indented JSON objects (not a valid single JSON array or standard JSON Lines file). This tool handles this specific format by:

*   **Heuristic Parsing:** It reads the raw log file line-by-line, accumulating lines until it detects a closing brace `}` that signifies the end of a JSON object. This allows it to parse the stream of objects efficiently without loading the entire file into memory.
*   **Incremental Processing:** It checks the last timestamp in the target `.jsonl` file to process only new entries, ensuring efficiency.
*   **Simplification:** Based on the `--log-simplify-level` (default: 1), it filters out unnecessary events and fields. Level 1 keeps only API requests and responses, while Level 3 retains only the essential token usage metrics, significantly reducing file size.
*   **Archiving:** If `--enable-archiving` is set, the original raw log file is moved to `/tmp` (or a configured folder) after successful processing. This prevents the raw logs from growing indefinitely.

### Pricing Data Collection

To provide accurate cost estimates, the tools fetch the latest model pricing and context window information from the [LiteLLM repository](https://github.com/BerriAI/litellm).

*   **Source:** `https://raw.githubusercontent.com/BerriAI/litellm/refs/heads/main/model_prices_and_context_window.json`
*   **Caching:** The pricing data is cached locally (default: `~/.gemini/prices.json`) for 24 hours to reduce network requests and improve performance. An active internet connection is required to update this cache. You can override the cache location by setting the `PRICE_CACHE_PATH` environment variable.

## Acknowledgements

- The [AGENTS.md](./AGENTS.md) was adapted from the examples in this blog post: [Getting Good Results from Claude Code](https://www.dzombak.com/blog/2025/08/getting-good-results-from-claude-code/).
- This project uses the ["Model Prices and Context Window" JSON file](https://raw.githubusercontent.com/BerriAI/litellm/refs/heads/main/model_prices_and_context_window.json) from LiteLLM to calculate token costs. LiteLLM's efforts in maintaining this file are greatly appreciated.
