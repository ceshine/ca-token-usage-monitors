"""Shared CLI utility helpers."""

from __future__ import annotations

from datetime import date

import typer


def parse_since_date(since: str | None) -> date | None:
    """Parse ``--since`` option value into a date.

    Args:
        since: ISO date string (YYYY-MM-DD) or None.

    Returns:
        Parsed date, or None if input is None.

    Raises:
        typer.BadParameter: If the string is not a valid ISO date.
    """
    if since is None:
        return None
    try:
        return date.fromisoformat(since)
    except ValueError as exc:
        raise typer.BadParameter(f"Invalid --since value: {since}. Expected YYYY-MM-DD.") from exc


def parse_until_date(until: str | None) -> date | None:
    """Parse ``--until`` option value into a date (exclusive upper bound).

    Args:
        until: ISO date string (YYYY-MM-DD) or None.

    Returns:
        Parsed date, or None if input is None.

    Raises:
        typer.BadParameter: If the string is not a valid ISO date.
    """
    if until is None:
        return None
    try:
        return date.fromisoformat(until)
    except ValueError as exc:
        raise typer.BadParameter(f"Invalid --until value: {until}. Expected YYYY-MM-DD.") from exc
