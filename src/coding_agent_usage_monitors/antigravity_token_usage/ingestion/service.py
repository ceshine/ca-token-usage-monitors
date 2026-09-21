"""High-level ingestion service for AntiGravity token usage."""

from __future__ import annotations

import logging

from .dedupe import deduplicate_events
from .errors import AntiGravityIngestionError
from .parser import parse_database
from .schemas import RawUsageEvent, IngestionCounters
from .discovery import discover_databases
from .repository import IngestionRepository

LOGGER = logging.getLogger(__name__)


class IngestionService:
    """Orchestrates discovery, parsing, deduplication, and persistence for AntiGravity databases."""

    def __init__(self, repository: IngestionRepository) -> None:
        """Initialize IngestionService.

        Args:
            repository (IngestionRepository): DuckDB persistence repository.
        """
        self._repository: IngestionRepository = repository

    def ingest(self, override_env: dict[str, str] | None = None) -> IngestionCounters:
        """Run complete AntiGravity token usage ingestion workflow.

        Args:
            override_env (dict[str, str] | None, optional): Optional environment overrides for discovery. Defaults to None.

        Returns:
            IngestionCounters: Statistics and status counters for the ingestion run.
        """
        counters = IngestionCounters()
        discovered_dbs = discover_databases(override_env=override_env)
        counters.databases_discovered = len(discovered_dbs)

        all_candidate_events: list[RawUsageEvent] = []

        for db in discovered_dbs:
            canonical_path_str = str(db.database_path.resolve())
            try:
                parse_result = parse_database(db)
                counters.databases_parsed += 1
                counters.raw_events_extracted += len(parse_result.events)
                counters.missing_optional_tables += parse_result.missing_optional_tables
                all_candidate_events.extend(parse_result.events)
            except AntiGravityIngestionError as err:
                LOGGER.warning("Failed to parse AntiGravity database %s: %s", canonical_path_str, err)
                counters.databases_failed += 1
                counters.failed_databases.append(canonical_path_str)
            except Exception as err:  # noqa: BLE001
                LOGGER.error(
                    "Unexpected failure while parsing AntiGravity database %s: %s",
                    canonical_path_str,
                    err,
                )
                counters.databases_failed += 1
                counters.failed_databases.append(canonical_path_str)

        if counters.databases_failed > 0:
            LOGGER.error(
                "Ingestion aborted due to %d failed database(s); canonical DuckDB table left unchanged.",
                counters.databases_failed,
            )
            return counters

        canonical_events = deduplicate_events(all_candidate_events)
        counters.canonical_events_written = len(canonical_events)
        counters.duplicate_events_merged = counters.raw_events_extracted - counters.canonical_events_written

        self._repository.replace_all_events(canonical_events)
        return counters
