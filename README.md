# Coding Agent Token Monitor

CLI tools for monitoring token usage of coding agents. Currently supports Claude Code, OpenCode, Codex, and Gemini.

(The Gemini tool is migrated from [ceshine/gemini-token-usage](https://github.com/ceshine/gemini-token-usage); this repository supersedes the original (now deprecated) project.)

## Supported Agent Monitors

This repository provides individual CLI tools for tracking, ingesting, and displaying token usage statistics for various coding agents:

| Coding Agent / Tool | Package / Command | Documentation |
| :--- | :--- | :--- |
| **Claude Code** | `claude-token-usage` | [Claude Code Usage Guide](./README_CLAUDE_CODE.md) |
| **OpenCode** | `opencode-token-usage` | [OpenCode Usage Guide](./README_OPENCODE.md) |
| **Codex** | `codex-token-usage` | [Codex Usage Guide](./README_CODEX.md) |
| **Gemini CLI** | `gemini-token-usage` | [Gemini CLI Usage Guide](./README_GEMINI_CLI.md) |

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

## Internal Workflows

### Pricing Data Collection

To provide accurate cost estimates, the tools fetch the latest model pricing and context window information from [models.dev](https://models.dev/).

*   **Source:** `https://models.dev/api.json`
*   **Caching:** The pricing data is cached locally (default: `~/.local/share/coding-agent-token-monitors/prices.json`) for 24 hours to reduce network requests and improve performance. An active internet connection is required to update this cache. You can override the cache location by setting the `PRICE_CACHE_PATH` environment variable.

---

## Acknowledgements

- The [AGENTS.md](./AGENTS.md) was adapted from the examples in this blog post: [Getting Good Results from Claude Code](https://www.dzombak.com/blog/2025/08/getting-good-results-from-claude-code/).
- This project uses the [models.dev API](https://models.dev/api.json) to retrieve model pricing and context window data for calculating token costs.
