# Token Ingestion Technical Design (AntiGravity SQLite -> DuckDB)

Created: 2026-06-23
Status: Proposed

## 1. Scope

This document defines ingestion of local AntiGravity conversation SQLite databases into DuckDB for a new `antigravity_token_usage` package.

In scope:

- Discover AntiGravity conversation databases from standard locations and `ANTIGRAVITY_DATA_DIR`.
- Label every discovered database by where it was found.
- Open source SQLite databases read-only.
- Decode documented Protocol Buffers fields without generated `.proto` files.
- Normalize model names, timestamps, and token counters.
- Deduplicate usage events across rows, tables, and databases.
- Persist the canonical event set into DuckDB.
- Replace the complete canonical event set on each successful ingestion run.

Out of scope:

- Statistics rendering, model pricing, and cost calculation.
- A persistent raw-event staging table.
- A persistent source-database or file-bookkeeping table.
- Incremental changed-file detection and tail checkpoints.
- Retaining the filesystem path of the source database in DuckDB.
- Generated Protocol Buffers code or a `.proto` dependency.

## 2. Finalized Product Decisions

1. A dedicated canonical-event field, `discovery_source`, records where the source database was discovered.
2. Supported source labels are `default`, `cli`, `ide`, `backup`, `config`, and `env`.
3. A database located below `~/.gemini/antigravity-cli/conversations/` yields `discovery_source = 'cli'`.
4. Discovery resolves every source database to a canonical path, deduplicates canonical paths, and processes them in deterministic order.
5. If the same physical database is reachable through more than one root, the source label from the first matching root in discovery-priority order wins.
6. Source database paths are transient implementation details. They are used for discovery, parsing, error messages, and fallback event-key creation, but are not stored in DuckDB.
7. The ingestion run parses every currently discovered database. It then globally deduplicates all parsed events in memory.
8. On a successful run, ingestion replaces the complete `antigravity_usage_events` contents in one DuckDB transaction. This favors correctness and a simple data model over incremental optimization.
9. No `antigravity_raw_usage_events` table is created. Existing provider ingestors persist usable canonical events directly and do not use raw-event staging tables.
10. A duplicate event is identified by any documented non-empty identity key, in this preference order: response ID, provider-assigned message ID, then message ID.
11. Events without any identity key are retained as distinct events. They cannot be safely cross-database deduplicated.
12. When duplicates merge, every token counter takes the maximum observed value rather than being summed.
13. Stored `discovery_source` for a merged event is selected by discovery priority: `default`, `cli`, `ide`, `backup`, `config`, then `env`.
14. SQLite source files are always opened read-only.
15. A malformed or unreadable database fails that database for the run but does not prevent processing other discovered databases. The DuckDB canonical event table is not replaced if any database fails.
16. Timestamp precedence follows the ccusage reference implementation: an explicit generator or step timestamp has rank `3`, a trajectory timestamp has rank `1`, and the source database modification time has rank `0`. Higher ranks win; equal ranks select the earlier timestamp.
17. Retry usages follow ccusage behavior: parse every token-bearing retry as a candidate event, without inferring whether it represents a failed attempt. A retry sharing an identity with another event merges through normal identity-based deduplication; one with a distinct or absent identity remains a separate event.
18. A deduplicated event stores exactly one `discovery_source`, selected by source priority. The initial implementation intentionally does not preserve every source location that contained a duplicate event.
19. `ANTIGRAVITY_DATA_DIR` follows ccusage override semantics: when the environment variable is present, its comma-separated roots replace the five default roots entirely. The default roots are scanned only when the variable is absent.
20. A missing primary `ModelUsage` is valid when retry usages are present. Parse and emit every token-bearing retry normally, inheriting enclosing metadata and timestamp fallback context; do not reject or skip the metadata solely because its primary usage field is absent.
21. `session_id` is required for every persisted event and is derived from the source database filename stem. Do not substitute an `unknown` value; a discovered database without a usable non-empty filename stem is a per-database ingestion failure.
22. Model-string normalization follows ccusage's `normalize_antigravity_model` rules exactly. Do not introduce generic provider-prefix stripping, punctuation rewriting, or version-suffix removal beyond those explicit rules.
23. Each source database is read through one explicit deferred read transaction with a bounded five-second SQLite busy timeout. The first schema read establishes a consistent SQLite snapshot for all table checks and source queries. A concurrent successful writer commit does not invalidate that snapshot; it is visible on a later ingestion run. SQLite busy, locked, I/O, or query failures remain per-database failures and prevent canonical-table replacement.

Decision 15 ensures that a partial discovery/parsing failure never silently replaces a previously complete canonical data set with incomplete results.

## 3. Package Layout

Create the following package structure:

```text
src/coding_agent_usage_monitors/antigravity_token_usage/
    __init__.py
    __main__.py
    ingestion/
        __init__.py
        errors.py
        schemas.py
        discovery.py
        protobuf.py
        parser.py
        dedupe.py
        repository.py
        service.py
```

Responsibilities:

- `schemas.py`: frozen typed rows and result/counter models.
- `discovery.py`: root resolution, recursive `.db` collection, canonicalization, deduplication, and source assignment.
- `protobuf.py`: defensive low-level protobuf wire-format reader.
- `parser.py`: read-only SQLite querying and AntiGravity-specific blob extraction.
- `dedupe.py`: event identities, connected-duplicate merging, and canonical-source selection.
- `repository.py`: DuckDB schema creation and transactional full replacement.
- `service.py`: discovery, parse orchestration, error accumulation, deduplication, and persistence.
- `errors.py`: contextual exceptions for discovery, SQLite/schema, and protobuf parsing failures.

A CLI, stats package, pricing rules, and a `pyproject.toml` script entry point can be designed separately once ingestion behavior is agreed.

## 4. Discovery and Source Attribution

### 4.1 Default roots

Search the following conversation directories in this exact order:

| Priority | Conversations directory | `discovery_source` |
| --- | --- | --- |
| 1 | `~/.gemini/antigravity/conversations/` | `default` |
| 2 | `~/.gemini/antigravity-cli/conversations/` | `cli` |
| 3 | `~/.gemini/antigravity-ide/conversations/` | `ide` |
| 4 | `~/.gemini/antigravity-backup/conversations/` | `backup` |
| 5 | `~/.config/antigravity/conversations/` | `config` |

Missing roots are ignored.

### 4.2 `ANTIGRAVITY_DATA_DIR`

When `ANTIGRAVITY_DATA_DIR` is present, it is a comma-separated list of override roots. Its non-empty entries replace the default roots in Section 4.1; do not scan any default root in that case. For each entry:

1. Expand the user directory marker and resolve the root when it exists.
2. If `root/conversations/` exists and is a directory, scan that directory.
3. Otherwise, scan `root` directly if it is a directory.
4. Assign every database found through this configuration `discovery_source = 'env'`.

The roots retain comma-separated input order before final path sorting. An environment variable set to an empty or whitespace-only value is still an override and therefore discovers no roots, matching ccusage behavior.

### 4.3 Collection and canonicalization

For every selected conversations directory:

1. Recursively collect regular files with a case-sensitive `.db` suffix.
2. Canonicalize each file with `Path.resolve()` to resolve symlinks and aliases.
3. Deduplicate using the canonical absolute path.
4. If a canonical path is found by multiple roots, retain the source assignment from the earliest root in Section 4.1/4.2 order.
5. Return candidates sorted by canonical path string.

The internal discovery model is:

```python
@dataclass(frozen=True)
class DiscoveredDatabase:
    database_path: Path
    discovery_source: str
    session_id: str
```

`session_id` is the required, non-empty database filename stem. A database without a usable filename stem is rejected as a per-database ingestion failure. `database_path` must not be persisted in DuckDB.

## 5. DuckDB Data Model

Persist only final, deduplicated usage events:

```sql
CREATE TABLE IF NOT EXISTS antigravity_usage_events (
    event_key VARCHAR PRIMARY KEY,
    discovery_source VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMPTZ,
    model_code VARCHAR NOT NULL,
    provider_id BIGINT,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    reasoning_tokens BIGINT NOT NULL,
    total_output_tokens BIGINT NOT NULL,
    cache_creation_tokens BIGINT NOT NULL,
    cache_read_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    response_id VARCHAR,
    provider_assigned_message_id VARCHAR,
    message_id VARCHAR,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Notes:

- `event_key` is deterministic for events having an identity: it uses the highest-preference identity key available (`response:`, then `provider:`, then `message:`).
- An event without any identity receives an internal fallback key derived from the canonical source path, source table, source row index, and repeated-usage ordinal. The path itself is not stored.
- `session_id` is required and is always the source database filename stem; a source database with no usable non-empty stem fails ingestion rather than storing a synthetic identifier.
- `discovery_source` is the single selected source for a canonical event. It does not attempt to represent every source location that contained a duplicate copy.
- No source-database table and no source-path field are persisted. A future provenance requirement would warrant a separate event-source relation rather than adding a comma-separated path/source field.

## 6. Source SQLite Access

### 6.1 Read-only connections

Use the Python standard library `sqlite3` module and a URI connection in read-only, explicit-transaction mode:

```python
connection = sqlite3.connect(
    f"{database_path.as_uri()}?mode=ro",
    uri=True,
    isolation_level=None,
)
connection.execute("PRAGMA busy_timeout = 5000")
```

`busy_timeout` is measured in milliseconds and causes SQLite to wait and retry briefly only for lock contention. It is not a query timeout and does not prevent a concurrent writer from committing. If the timeout expires, the resulting busy/locked error is a per-database ingestion failure.

The implementation must never write to, migrate, or create a source AntiGravity database.

### 6.2 Consistent read snapshot

Before any schema check or source-table query, execute a deferred `BEGIN` transaction:

```python
connection.execute("BEGIN")
connection.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchall()
```

`BEGIN` does not write a snapshot file and, for a deferred transaction, does not itself necessarily establish the read snapshot. The first read of `sqlite_master` establishes SQLite's in-memory logical snapshot. In WAL mode it pins the reader to the WAL state visible at that point; in rollback-journal mode SQLite provides the equivalent stable page view.

Perform every `sqlite_master` table-existence check and every source-table `SELECT` in this same transaction. Fetch the rows/blobs required for parsing, then commit and close the read-only connection before protobuf parsing and DuckDB persistence. On an exception, roll back if possible and close the connection.

Do not use `BEGIN IMMEDIATE`, `BEGIN EXCLUSIVE`, `immutable=1`, direct filesystem copying of only the `.db` file, mtime/size checks, or `PRAGMA data_version` as a correctness mechanism. They either interfere with writers, can omit WAL state, or cannot reliably prove that no concurrent write occurred. A successful writer commit after the snapshot begins simply appears on the next ingestion run.

### 6.3 Required and optional tables

The parser must query:

```sql
SELECT idx, data FROM gen_metadata ORDER BY idx ASC;
```

`gen_metadata` is required. A missing table is a database parse failure.

Before querying optional tables, inspect `sqlite_master`:

```sql
SELECT 1
FROM sqlite_master
WHERE type = 'table' AND name = ?;
```

If present, query:

```sql
SELECT idx, metadata
FROM steps
WHERE metadata IS NOT NULL
ORDER BY idx ASC;
```

```sql
SELECT data
FROM trajectory_metadata_blob
ORDER BY rowid ASC;
```

Each trajectory blob contains a database-level timestamp fallback; its exact mapping and use are defined in Section 8.3.

## 7. Protocol Buffers Wire Reader

No generated protobuf code is required. `protobuf.py` implements only the documented wire-format primitives.

Each field tag is interpreted as:

```text
tag = (field_number << 3) | wire_type
```

Supported wire types:

| Wire type | Meaning | Action |
| --- | --- | --- |
| 0 | Varint | Read an unsigned LEB128 integer, at most 10 bytes for `uint64`. |
| 1 | Fixed64 | Consume exactly 8 bytes. |
| 2 | Bytes/String | Read a varint length then consume exactly that many bytes. |
| 5 | Fixed32 | Consume exactly 4 bytes. |

The reader must:

- preserve repeated fields and their source order;
- reject protobuf group wire types (`3` and `4`);
- reject truncated tags, values, length prefixes, and fixed-width payloads;
- reject overflowed or unterminated varints;
- reject a length prefix extending beyond the blob;
- decode declared string fields as UTF-8 and raise a contextual parse error on invalid text.

The parser should expose field access helpers that distinguish absent fields, incorrect wire types, and repeated byte fields.

## 8. Protobuf Extraction

### 8.1 Generator metadata

For each `gen_metadata.data` blob:

1. Read root field `1` as the `chat_model` submessage.
2. From `chat_model`, read:
   - field `3` varint: model ID;
   - field `4` bytes: primary `ModelUsage`;
   - field `9` bytes: `GenerationInfo`, whose field `4` is a `TimestampMessage`;
   - field `17` repeated bytes: retry usages, where nested field `2` is a `ModelUsage`;
   - field `19`, otherwise field `21`, string: raw model name.
3. Emit one candidate event from the primary usage when it is token-bearing.
4. Emit one candidate event for every token-bearing retry usage, inheriting model/provider/timestamp context when the retry usage lacks it. Do not infer a failure state or discard retry usages merely because they came from retry metadata.

### 8.2 Step metadata

For each `steps.metadata` blob:

1. Read timestamp from field `8`, otherwise field `1`.
2. Read primary usage from field `9`.
3. Read `model_info` from field `24`:
   - field `1` varint: model ID;
   - field `7` varint: provider;
   - field `12`, otherwise field `8`, string: model name.
4. Read field `28` repeated retry usages, with nested field `2` as `ModelUsage`.
5. Emit token-bearing primary and retry candidate events following the same inheritance rules as generator metadata. Do not infer a failure state for retries.

### 8.3 Trajectory metadata

Each `trajectory_metadata_blob.data` value decodes as a `TrajectoryMetadata` message:

```text
TrajectoryMetadata
└── field 2 (bytes): TimestampMessage
    ├── field 1 (varint): Unix epoch seconds
    └── field 2 (varint): nanoseconds
```

Trajectory metadata does not yield usage events and does not contain a `ModelUsage` path. Query rows in ascending `rowid` order and retain the first valid decoded timestamp. Use that single database-level value only as the timestamp fallback for events in that database that do not have an explicit generator or step timestamp.

### 8.4 `ModelUsage`

Decode these fields when present:

| Field | Type | Meaning |
| --- | --- | --- |
| 1 | varint | model ID |
| 2 | varint | input tokens |
| 3 | varint | total output tokens |
| 4 | varint | cache creation tokens |
| 5 | varint | cache read tokens |
| 6 | varint | provider ID |
| 7 | string | message ID |
| 9 | varint | reasoning tokens |
| 10 | varint | visible output tokens |
| 11 | string | response ID |
| 12 | string | provider-assigned message ID |

Absent token fields default to zero. Values must be non-negative integers representable by the DuckDB `BIGINT` target columns.

### 8.5 Timestamps and fallback precedence

Decode `TimestampMessage` as:

| Field | Type | Meaning |
| --- | --- | --- |
| 1 | varint | Unix epoch seconds |
| 2 | varint | nanoseconds |

Convert it to a UTC timestamp using:

```text
epoch_ms = (seconds * 1000) + (nanos / 1_000_000)
```

Assign a timestamp rank to every candidate event:

| Rank | Source |
| --- | --- |
| 3 | Explicit generator or step timestamp |
| 1 | First valid trajectory timestamp for the database |
| 0 | Source database modification time |

Use the highest-rank timestamp while merging duplicate events. If ranks are equal, use the earlier timestamp. The file modification time is a transient parsing input; the database path itself remains unpersisted.

## 9. Normalization

### 9.1 Token counters

For each parsed usage event, calculate canonical token values as follows:

```python
total_output_tokens = max(raw.total_output_tokens, raw.visible_output_tokens + raw.reasoning_tokens)
output_tokens = max(raw.visible_output_tokens, total_output_tokens - raw.reasoning_tokens)
reasoning_tokens = max(raw.reasoning_tokens, total_output_tokens - output_tokens)
total_tokens = (
    input_tokens
    + cache_creation_tokens
    + cache_read_tokens
    + total_output_tokens
)
```

All normalized counts must be non-negative.

### 9.2 Model resolution

Resolve the model for each candidate event in this order, matching ccusage:

1. The candidate `ModelUsage.field 1` numeric model ID, when non-zero.
2. The enclosing normalized raw model string.
3. The enclosing metadata numeric model ID.
4. The inherited generation model, when applicable.
5. `gemini-internal-model` when no preceding source resolves a model.

For generator context, the inherited generation model is established from the row's raw name, then its metadata model ID, then its primary usage model ID, and then the last previously resolved generation model. For step context, it is established from the step raw name, then its metadata model ID, then the latest resolvable model from the generation rows. Thus, an individual retry or primary usage with its own numeric model ID overrides a raw model name inherited from its enclosing generator or step metadata. Numeric IDs resolve through this mapping:

| Model ID | Canonical name |
| --- | --- |
| `246` | `gemini-2.5-pro` |
| `312` | `gemini-2.5-flash` |
| `313`, `329` | `gemini-2.5-flash-thinking` |
| `330` | `gemini-2.5-flash-lite` |
| `281`, `282` | `claude-4-sonnet` |
| `290`, `291` | `claude-4-opus` |
| `333`, `334` | `claude-4.5-sonnet` |
| `340`, `341` | `claude-4.5-haiku` |
| `342` | `gpt-oss-120b-medium` |
| `1071` | `gemini-3.6-flash-high` |
| `1072` | `gemini-3.6-flash-medium` |
| `1073` | `gemini-3.6-flash-low` |
| `1298` | `gemini-3.7-flash-high` |
| `1299` | `gemini-3.7-flash-medium` |
| `1300` | `gemini-3.7-flash-low` |
| `1318` | `gemini-3.8-flash-high` |
| `1319` | `gemini-3.8-flash-medium` |
| `1320` | `gemini-3.8-flash-low` |
| Unlisted ID below `1000` | `antigravity-model-{model_id}` |
| `1000+` | `model_placeholder_m{model_id - 1000}` |

Normalize a resolved model string exactly as ccusage does:

1. Trim surrounding whitespace. An empty result is missing.
2. Match the lowercased string against the explicit aliases below.
3. If no parenthesized effort alias matched, discard the first `(` and everything after it, then trim the remaining base string and match it against the base aliases below.
4. If still unmatched, replace ASCII spaces with hyphens. Return that converted value only when it begins with `gemini-`, `claude-`, or `gpt-`; otherwise preserve the trimmed original string, including its original case and punctuation.

Parenthesized effort aliases are exact for every combination of version `3.6`, `3.7`, or `3.8` and effort `high`, `medium`, or `low`:

```text
gemini <version> flash (<effort>) -> gemini-<version>-flash-<effort>
```

Base aliases are:

| Raw lowercased base | Canonical model code |
| --- | --- |
| `gemini 3.8 flash`, `gemini 3.8 flash thinking` | `gemini-3.8-flash` |
| `gemini 3.7 flash`, `gemini 3.7 flash thinking` | `gemini-3.7-flash` |
| `gemini 3.7 pro`, `gemini 3.7 pro thinking` | `gemini-3.7-pro` |
| `gemini 3.6 flash`, `gemini 3 flash` | `gemini-3.6-flash` |
| `gemini 3.6 pro` | `gemini-3.6-pro` |
| `gemini 3 pro`, `gemini 3 pro thinking` | `gemini-3-pro` |
| `gemini 2.5 flash` | `gemini-2.5-flash` |
| `gemini 2.5 pro` | `gemini-2.5-pro` |
| `gemini 2.0 flash`, `gemini 2 flash` | `gemini-2.0-flash` |
| `gemini 2.0 pro` | `gemini-2.0-pro` |
| `gemini 1.5 flash` | `gemini-1.5-flash` |
| `gemini 1.5 pro` | `gemini-1.5-pro` |
| `model_placeholder_m318` | `gemini-3.8-flash-high` |
| `model_placeholder_m319` | `gemini-3.8-flash-medium` |
| `model_placeholder_m320` | `gemini-3.8-flash-low` |
| `model_placeholder_m298` | `gemini-3.7-flash-high` |
| `model_placeholder_m299` | `gemini-3.7-flash-medium` |
| `model_placeholder_m300` | `gemini-3.7-flash-low` |
| `model_placeholder_m71` | `gemini-3.6-flash-high` |
| `model_placeholder_m72` | `gemini-3.6-flash-medium` |
| `model_placeholder_m73` | `gemini-3.6-flash-low` |
| `model_placeholder_m26` | `claude-opus-4-6` |
| `model_placeholder_m35` | `claude-sonnet-4-6` |
| `model_placeholder_m36`, `model_placeholder_m37`, `model_placeholder_m16` | `gemini-3.1-pro` |
| `model_placeholder_m18`, `model_placeholder_m84`, `model_placeholder_m47` | `gemini-3-flash-preview` |
| `model_placeholder_m132`, `model_placeholder_m133` | `gemini-3.5-flash-high` |
| `model_placeholder_m187` | `gemini-3.5-flash-extra-low` |
| `model_placeholder_m20` | `gemini-3.5-flash-medium` |
| `model_openai_gpt_oss_120b_medium` | `gpt-oss-120b-medium` |
| `gemini-pro-default`, `gemini-pro-agent` | `gemini-3.1-pro` |
| `gemini-3-flash-agent`, `gemini-3-flash-agent-a`, `gemini-3-flash-agent-b`, `gemini-3-flash-a`, `gemini-3-flash-b` | `gemini-3.5-flash-high` |
| `gemini-3-flash-c`, `gemini-3-flash` | `gemini-3-flash-preview` |
| `gemini-3.5-flash-low` | `gemini-3.5-flash-medium` |
| `gemini-3.1-pro-high`, `gemini-3.1-pro-low` | `gemini-3.1-pro` |
| `gemini-3-pro-high`, `gemini-3-pro-low` | `gemini-3-pro` |
| `claude 3.7 sonnet`, `claude 3.7 sonnet thinking` | `claude-3-7-sonnet` |
| `claude 3.5 sonnet` | `claude-3-5-sonnet` |
| `claude 3.5 haiku` | `claude-3-5-haiku` |
| `claude 3 opus` | `claude-3-opus` |

The numeric provider ID is retained as `provider_id`. Known source values are `3` (Vertex AI), `24` (Gemini), and `30` (Evergreen), but no provider display-name column is needed for ingestion.

## 10. Global Event Deduplication and Persistence

### 10.1 Identity keys

For each candidate event, create every available non-empty identity key:

1. `response:<response_id>`
2. `provider:<provider_assigned_message_id>`
3. `message:<message_id>`

The ordered list is used when selecting an `event_key`; all keys participate in duplicate matching.

### 10.2 Deduplication algorithm

1. Parse every discovered database into one in-memory candidate event list.
2. Maintain an identity-key-to-event-slot index.
3. For each event, find all existing slots reached by any of its identity keys.
4. If none match, create a new slot.
5. If one or more slots match, merge all matching slots and the current event into one canonical slot.
6. Re-index every identity from the merged slot to the surviving slot.
7. Events with no identity never match another event and remain independent.

The implementation must handle transitive duplicate relationships. For example, an event matching A by `message:` and B by `response:` merges A and B even if A and B did not initially share a key.

### 10.3 Merge rules

When merging duplicate events:

- Take the maximum for `input_tokens`, `output_tokens`, `cache_creation_tokens`, `cache_read_tokens`, `reasoning_tokens`, and `total_output_tokens`.
- Recompute `total_tokens` from the resulting component values.
- Prefer a non-default model code over `gemini-internal-model`.
- Prefer an explicit provider ID to a missing provider ID.
- Union all identity keys.
- Select message IDs by identity preference: response ID, provider-assigned message ID, then message ID.
- Select `discovery_source` by the priority in Section 2.
- Select timestamps using the rank and equal-rank tie-breaker in Section 8.5.

### 10.4 Full replacement transaction

The repository performs a full replacement only when parsing every discovered database succeeds:

1. Ensure `antigravity_usage_events` exists.
2. Begin a transaction.
3. Delete existing rows from `antigravity_usage_events`.
4. Insert all globally deduplicated canonical rows using `executemany`.
5. Commit the transaction.

If discovery produces zero databases, the successful result is an empty canonical table. If any database fails to parse, do not begin the replacement transaction; return failure counters and leave the prior table unchanged.

## 11. Open Questions

No unresolved ingestion-design questions remain.

## 12. Error Handling and Counters

The service should report at least:

- `databases_discovered`
- `databases_parsed`
- `databases_failed`
- `raw_events_extracted`
- `canonical_events_written`
- `duplicate_events_merged`
- `missing_optional_tables`
- `failed_databases` (canonical paths for CLI/log output only)

Failures must include the canonical source path and enough context to diagnose the issue:

- SQLite open/query errors: source path and SQL/table context.
- Missing `gen_metadata`: source path and explicit required-table message.
- Blob decode errors: source path, table, row index, protobuf field path, and byte offset when available.
- Invalid timestamps or token values: source path, table, row index, and affected field.

The service may log per-database failures and continue parsing other databases. It must not replace the existing DuckDB canonical table when `databases_failed > 0`.

## 13. Test Plan

Create `tests/antigravity_ingestion/`.

### 13.1 Discovery tests

- Every default root maps to the correct `discovery_source`.
- Files under `antigravity-cli/conversations` receive `cli`.
- Missing roots do not fail discovery.
- Environment roots select `root/conversations` when present and `root` otherwise.
- A present `ANTIGRAVITY_DATA_DIR` replaces default-root discovery; databases in default roots are not returned.
- An empty or whitespace-only `ANTIGRAVITY_DATA_DIR` discovers no roots and does not fall back to default-root discovery.
- Multiple comma-separated environment roots retain deterministic ordering.
- Recursive `.db` discovery includes nested database files.
- Canonical-path deduplication collapses symlink and alias paths.
- A duplicate reachable through multiple roots retains the earliest-priority label.
- Output paths are canonical and sorted.

### 13.2 Wire-reader tests

- Decode single and multi-byte varints.
- Decode fixed32, fixed64, and bytes fields.
- Preserve repeated fields in order.
- Reject truncated tags, values, fixed-width values, and byte payloads.
- Reject unterminated or overflowing varints.
- Reject invalid length prefixes and unsupported group wire types.
- Reject invalid UTF-8 in fields designated as strings.

### 13.3 SQLite/parser tests

Build temporary SQLite databases in test fixtures using encoded wire-format blobs; tests must not require real user databases.

- Parse a minimum valid `gen_metadata` row.
- Derive and persist `session_id` from the database filename stem.
- Reject a database with no usable non-empty filename stem without replacing the existing canonical event set.
- Parse model string and numeric-model fallback.
- Parse a primary usage plus repeated retry usages.
- Parse and emit token-bearing retry usages when the enclosing generator or step metadata lacks a primary `ModelUsage`.
- Skip a primary or retry usage only when all token counters are zero.
- Parse optional `steps` when present.
- Accept absent optional tables.
- Reject absent required `gen_metadata`.
- Verify read-only behavior by using a source database that cannot be modified by the test process.
- Verify source-table existence checks and reads occur in one explicit deferred read transaction.
- In a WAL-mode two-connection fixture, verify rows committed after the reader's first schema read are absent from that run's snapshot and visible to a later run.
- Verify a busy/locked source connection after the bounded timeout is a per-database failure and leaves the existing canonical DuckDB event set unchanged.
- Verify timestamps, usage IDs, provider IDs, and all token counters.
- Decode a trajectory timestamp from root field `2` and verify that the first valid trajectory row is retained.
- Verify trajectory timestamps apply only when an event has no explicit generator or step timestamp.
- Verify source database modification time is the final timestamp fallback.

### 13.4 Normalization tests

- Cover every listed numeric model-ID mapping, including the unlisted-ID fallback.
- Cover the `1000+` placeholder rule and its explicit placeholder aliases.
- Cover the `gemini-internal-model` fallback when no usage or context model resolves.
- Cover every explicit ccusage string-normalization alias, every parenthesized effort alias, and the recognized-family fallback.
- Verify that an unrecognized raw string is returned in trimmed original form rather than receiving generic punctuation, prefix, or suffix rewriting.
- Verify that a candidate `ModelUsage.model_id` overrides an enclosing raw model string.
- Cover total-output/output/reasoning reconciliation when reported values conflict.
- Verify total tokens include input, both cache categories, and normalized total output.

### 13.5 Deduplication tests

- Deduplicate a generator and step event sharing each supported identity type.
- Verify a retry sharing an identity with its primary usage merges under the normal token-maximum rules.
- Verify a token-bearing retry with a distinct or absent identity remains a separate event.
- Deduplicate events across separate `cli` and `ide` databases.
- Merge transitive identity connections.
- Select maximum counters field by field.
- Preserve a better model/provider when the first event lacks it.
- Select the source label using priority.
- Keep no-identity events distinct.
- Produce deterministic canonical event keys.
- Verify explicit generator/step timestamps (rank `3`) beat trajectory timestamps (rank `1`) and source file modification times (rank `0`).
- Verify equal-rank timestamps select the earlier value.

### 13.6 Service/repository tests

- A successful run replaces stale event rows with the current complete canonical set.
- A run with no discovered databases successfully empties the event table.
- A run where any database fails preserves the prior event table unchanged.
- Counters and failed-path output correctly distinguish successful and failed databases.
- Repeated successful runs are idempotent.

## 14. Validation Queries

```sql
-- Every persisted event has an allowed discovery source.
SELECT discovery_source, COUNT(*) AS event_count
FROM antigravity_usage_events
GROUP BY discovery_source
ORDER BY discovery_source;
```

```sql
-- Identity keys used as canonical event keys should remain unique.
SELECT event_key, COUNT(*) AS count
FROM antigravity_usage_events
GROUP BY event_key
HAVING count > 1;
```

```sql
-- Inspect canonical totals by model and source.
SELECT
    discovery_source,
    model_code,
    COUNT(*) AS event_count,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(reasoning_tokens) AS reasoning_tokens,
    SUM(cache_creation_tokens) AS cache_creation_tokens,
    SUM(cache_read_tokens) AS cache_read_tokens,
    SUM(total_tokens) AS total_tokens
FROM antigravity_usage_events
GROUP BY discovery_source, model_code
ORDER BY discovery_source, model_code;
```
