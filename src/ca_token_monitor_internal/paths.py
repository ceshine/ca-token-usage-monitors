"""Shared path utilities for coding-agent-token-monitor."""

from __future__ import annotations

import os
from pathlib import Path


def get_default_database_path() -> Path:
    """Return the default DuckDB path following XDG data directory conventions."""
    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        base_data_dir = Path(xdg_data_home).expanduser()
    else:
        base_data_dir = Path("~/.local/share").expanduser()
    return base_data_dir / "coding-agent-token-monitor" / "token_usage.duckdb"


def get_default_price_cache_path() -> Path:
    """Return the default cache path following XDG conventions."""
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        base_cache_dir = Path(xdg_cache_home).expanduser()
    else:
        base_cache_dir = Path("~/.cache").expanduser()
    return base_cache_dir / "coding-agent-token-monitor" / "price_cache.json"


def get_default_opencode_db_path() -> Path:
    """Return the default OpenCode SQLite path following XDG data directory conventions."""
    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        base_data_dir = Path(xdg_data_home).expanduser()
    else:
        base_data_dir = Path("~/.local/share").expanduser()
    return base_data_dir / "opencode" / "opencode.db"
