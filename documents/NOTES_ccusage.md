# Knowledge Transfer: ccusage Behavior and Data Model

This document is a standalone description of the system behavior to reimplement ccusage in another language without needing access to the source code.

## Purpose

ccusage is a CLI that analyzes local Claude Code usage logs (JSONL) and produces reports by day, week, month, session, and 5-hour billing blocks. It outputs either a human-readable table or structured JSON.

## Inputs and Data Locations

Claude Code writes usage logs as JSONL. ccusage reads those files from the following locations:

Default search roots:

- ~/.config/claude/projects/ (new default)
- ~/.claude/projects/ (legacy default)

Override:

- Environment variable CLAUDE_CONFIG_DIR can provide one or more base directories (comma-separated). Each base directory must contain a projects/ subdirectory.

Expected layout:

- projects/{project}/{sessionId}.jsonl

Important: the project name used by ccusage is derived from the directory name immediately after projects/. It is not guaranteed to be the real filesystem path of the project. The sessionId is the JSONL filename (without extension) and should match the sessionId field inside entries.

## Core JSONL Schema (usageDataSchema)

Each line in a JSONL file must parse as an object with the following schema (optional fields are marked with ?):

```
usageDataSchema:
  cwd?: string
  sessionId?: string
  timestamp: ISO timestamp string
  version?: string
  message:
    usage:
      input_tokens: number
      output_tokens: number
      cache_creation_input_tokens?: number
      cache_read_input_tokens?: number
      speed?: "standard" | "fast"
    model?: string
    id?: string
    content?: Array<{ text?: string }>
  costUSD?: number
  requestId?: string
  isApiErrorMessage?: boolean
```

Notes:

- Invalid JSON lines or schema mismatches are skipped silently.
- cwd is present in the schema but not used in aggregation or reporting.
- message.model may be missing.
- message.usage.speed is used to tag the model name with a "-fast" suffix for display and aggregation.

## High-Level Data Flow

1. Resolve Claude data roots (CLAUDE_CONFIG_DIR or defaults).
1. Glob all JSONL files under projects/ recursively.
1. Stream each JSONL file line-by-line to avoid high memory usage.
1. Validate each line with the schema above.
1. Deduplicate entries by message.id + requestId if both exist.
1. Compute per-entry cost based on cost mode (see below).
1. Group and aggregate into one of the report types.
1. Render as table or JSON. JSON can be post-processed with jq if requested.

## Deduplication

Duplicate detection uses a unique hash composed of message.id + requestId. If either field is missing, the entry is not deduplicated. This means duplicates may remain when either id is absent.

## Cost Calculation Modes

There are three modes:

- auto: use costUSD if present; otherwise calculate from tokens and model pricing.
- calculate: always calculate from tokens; ignore costUSD.
- display: always use costUSD; missing costUSD becomes 0.

Pricing is based on LiteLLM model pricing data.

- For Claude models, an offline cached dataset can be used (offline mode).
- Unknown models yield a calculated cost of 0.

## Model Name Handling

- The display/aggregation model name is message.model.
- If message.usage.speed == "fast", append "-fast" to the model name.
- The special model name "<synthetic>" is excluded from totals and breakdowns.

## Report Types and Aggregation

Daily report:

- Group by local date string (YYYY-MM-DD).
- Date grouping always uses a fixed locale to preserve YYYY-MM-DD formatting; display locale may differ.
- Optional grouping by project ("instances" mode).

Weekly report:

- Built from daily buckets.
- Start-of-week is configurable (default Sunday).

Monthly report:

- Built from daily buckets and grouped by YYYY-MM.

Session report:

- Group by filesystem-derived session key: projectPath/sessionId.
- projectPath is the path between projects/ and the session directory.
- lastActivity is the most recent timestamp in the session group.
- versions is the unique list of version values seen in the session.

5-hour billing blocks:

- Entries are sorted by timestamp.
- Each block starts at the entry timestamp floored to the UTC hour.
- A block ends when either time since block start or time since last entry exceeds the block duration (default 5 hours).
- If there is a gap longer than the block duration, insert a gap block.
- A block is active if "now" is within the block duration and the last activity is within the duration.

## Session Block Projections

For active blocks:

- Burn rate is computed from the first and last entry timestamps.
- tokensPerMinute uses all token types.
- tokensPerMinuteForIndicator uses only input + output tokens (excludes cache tokens).
- Projected totals are computed by extrapolating the burn rate to the remaining time in the block.

## Date, Timezone, and Locale Behavior

- Date grouping uses a fixed locale to preserve deterministic YYYY-MM-DD grouping.
- Display formatting can use a user-specified locale.
- Timezone for grouping can be specified; invalid timezones should raise a RangeError equivalent.

## JSON Output Shape (High-Level)

Outputs vary by command but include:

- Per-bucket array (daily/monthly/weekly/session/blocks)
- Totals summary
- Optional per-model breakdowns

If grouping by project for daily/monthly, output is grouped per project plus totals.

## Configuration File Behavior

Configuration files are discovered and merged with CLI args:

Search order (highest priority first):

1. ./.ccusage/ccusage.json
1. ccusage.json under each Claude config root

Merge order (highest priority first):

1. CLI args explicitly provided
1. Per-command config
1. Defaults config
1. Built-in defaults

## Debug Mode

Debug tooling can compare costUSD with calculated cost using pricing data and report mismatches. This is only for diagnostics and does not affect normal aggregation.

## Important Caveats

- "Project" names are derived from Claude log directory names. They do not map to actual filesystem project paths.
- The cwd field exists in logs but is not used.
- Cost calculation depends on exact model name matches in the pricing database.
- Missing or invalid JSON lines are skipped silently.
- Offline pricing only guarantees cached data for Claude models.
- Session blocks are based on UTC hour flooring, which may differ from local time expectations.
- Usage-limit reset time is inferred by parsing error message content, which can be brittle.

## Porting Checklist

- Implement streaming JSONL parsing with schema validation and skip-on-error behavior.
- Replicate the dedup strategy exactly (message.id + requestId).
- Preserve date grouping semantics and locale/timezone split.
- Implement cost modes and LiteLLM-compatible pricing data, including offline Claude-only cache.
- Match 5-hour block logic, gap detection, and active block projection.
- Keep output JSON shapes and model breakdown behavior consistent.
