"""Custom exceptions for ingestion pipeline failures."""


class IngestionError(Exception):
    """Base exception for ingestion errors."""


class ParseError(IngestionError):
    """Raised when input log parsing fails."""


class SessionIdentityError(IngestionError):
    """Raised when session identity cannot be resolved."""


class ModelAttributionError(IngestionError):
    """Raised when token usage cannot be attributed to a model context."""


class DataIntegrityError(IngestionError):
    """Raised when token usage data violates integrity checks."""


class DuplicateConflictError(DataIntegrityError):
    """Raised when duplicate cumulative totals disagree in token payload fields."""


class MonotonicityError(DataIntegrityError):
    """Raised when cumulative token totals decrease or fail monotonicity checks."""


class DeltaConsistencyError(DataIntegrityError):
    """Raised when cumulative deltas differ from incremental usage values."""
