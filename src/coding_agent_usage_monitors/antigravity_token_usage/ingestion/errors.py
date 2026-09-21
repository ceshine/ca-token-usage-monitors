"""Exception classes for AntiGravity token usage ingestion."""

from __future__ import annotations

from pathlib import Path


class AntiGravityIngestionError(Exception):
    """Base exception for AntiGravity token ingestion errors."""

    def __init__(
        self,
        message: str,
        database_path: Path | str | None = None,
        table: str | None = None,
        row_index: int | None = None,
        field_path: str | None = None,
        byte_offset: int | None = None,
    ) -> None:
        """Initialize AntiGravityIngestionError.

        Args:
            message (str): Description of the error.
            database_path (Path | str | None, optional): Associated SQLite database path. Defaults to None.
            table (str | None, optional): Associated SQLite table name. Defaults to None.
            row_index (int | None, optional): Associated row index. Defaults to None.
            field_path (str | None, optional): Associated Protobuf field path. Defaults to None.
            byte_offset (int | None, optional): Associated byte offset in binary payload. Defaults to None.
        """
        super().__init__(message)
        self.message: str = message
        self.database_path: Path | str | None = database_path
        self.table: str | None = table
        self.row_index: int | None = row_index
        self.field_path: str | None = field_path
        self.byte_offset: int | None = byte_offset

    def __str__(self) -> str:
        """Return formatted contextual string representation of the exception.

        Returns:
            str: Error message with context appended if present.
        """
        context_parts: list[str] = []
        if self.database_path is not None:
            context_parts.append(f"db={self.database_path}")
        if self.table is not None:
            context_parts.append(f"table={self.table}")
        if self.row_index is not None:
            context_parts.append(f"row={self.row_index}")
        if self.field_path is not None:
            context_parts.append(f"field={self.field_path}")
        if self.byte_offset is not None:
            context_parts.append(f"offset={self.byte_offset}")

        if context_parts:
            return f"{self.message} ({', '.join(context_parts)})"
        return self.message


class DiscoveryError(AntiGravityIngestionError):
    """Error raised during database discovery."""


class SQLiteSchemaError(AntiGravityIngestionError):
    """Error raised when SQLite database opening or schema checks fail."""


class ProtobufParseError(AntiGravityIngestionError):
    """Error raised when decoding Protobuf wire format blobs fails."""


class InvalidDataError(AntiGravityIngestionError):
    """Error raised when extracted values are invalid or corrupted."""
