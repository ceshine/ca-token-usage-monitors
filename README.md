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

### Stats command

Run the tool by providing the path to your project directory or a specific log file:

```bash
uv run gemini-token-usage stats [OPTIONS] LOG_FILE_PATH
```

Required Argument:

*   `LOG_FILE_PATH`: The path to the directory containing the logs or a specific log file (`.log` or `.jsonl`).
    *   If a **directory** is provided, the tool automatically searches for `telemetry.log` (to convert) or `telemetry.jsonl` (to analyze) within that directory or its `.gemini` subdirectory.
    *   If a **file** is provided, it processes that specific file.

Useful options:

*   `-tz`, `--timezone TEXT`: Set the timezone for daily aggregation (e.g., `Asia/Tokyo`, `America/New_York`). Defaults to the system's local time.
*   `--enable-archiving`: If set, moves processed log files to `/tmp` to save space. **Only use this if no Gemini CLI instance is currently running** to avoid locking issues.
*   `--log-simplify-level INTEGER`: Controls how much the logs are simplified to save space (Default: 1).
    *   `0`: No simplification.
    *   `1`: Default simplification.
    *   `2`: Trim fields.
    *   `3`: Trim attributes.

#### Examples

**Analyze the current directory (uses default timezone):**

```bash
uv run gemini-token-usage stats .
```

**Recommended: Use the highest level of simplification and pruning if you just want token usage statistics:**

```bash
uv run gemini-token-usage stats . --log-simplify-level 3
```

**Analyze with a specific timezone:**

```bash
uv run gemini-token-usage stats . -tz "Asia/Taipei"
```

**Analyze a specific converted log file:**

```bash
uv run gemini-token-usage stats .gemini/telemetry.jsonl
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
uv run gemini-token-usage . --enable-archiving
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
