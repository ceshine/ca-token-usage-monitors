"""Custom exceptions for Gemini ingestion pipeline failures."""


class IngestionError(Exception):
    """Base exception for ingestion pipeline errors."""


class PathResolutionError(IngestionError):
    """Raised when an ingest input path cannot be resolved to a preprocessed JSONL."""


class MetadataValidationError(IngestionError):
    """Raised when line-1 metadata is missing, malformed, or inconsistent."""


class ParseError(IngestionError):
    """Raised when usage event payload parsing fails."""


class DuplicateEventError(IngestionError):
    """Raised when duplicate `(event_timestamp, model_code)` keys are found in one JSONL source."""


class SourceConflictError(IngestionError):
    """Raised when source-path and project-id mappings are inconsistent."""


class ConfirmationDeclinedError(IngestionError):
    """Raised when a required interactive confirmation is declined."""


class AppendOnlyViolationError(IngestionError):
    """Raised when a JSONL file tail regresses behind stored checkpoint state."""
