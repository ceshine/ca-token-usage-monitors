"""Custom exceptions for Pi agent ingestion pipeline failures."""


class IngestionError(Exception):
    """Base exception for Pi ingestion errors."""


class ParseError(IngestionError):
    """Raised when Pi session log parsing or structural validation fails."""
