"""Custom exceptions for Claude Code ingestion pipeline failures."""


class IngestionError(Exception):
    """Base exception for ingestion errors."""


class ParseError(IngestionError):
    """Raised when input log parsing fails."""


class SessionIdentityError(IngestionError):
    """Raised when session identity cannot be resolved."""


class DataIntegrityError(IngestionError):
    """Raised when token usage data violates integrity checks."""


class DuplicateConflictError(DataIntegrityError):
    """Raised when duplicate dedup keys disagree in token payload fields."""
