"""Custom exceptions for OpenCode ingestion pipeline failures."""


class IngestionError(Exception):
    """Base exception for OpenCode ingestion errors."""


class SourceDatabaseError(IngestionError):
    """Raised when the source SQLite database cannot be opened or read."""


class SourceSchemaError(IngestionError):
    """Raised when required source tables are missing."""


class ParseError(IngestionError):
    """Raised when message payload parsing or validation fails."""
