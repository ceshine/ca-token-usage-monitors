"""Discovery module for AntiGravity SQLite conversation databases."""

from __future__ import annotations

import os
from pathlib import Path

from .schemas import DiscoveredDatabase

DEFAULT_ROOT_SPECS: list[tuple[str, str]] = [
    ("~/.gemini/antigravity/conversations", "default"),
    ("~/.gemini/antigravity-cli/conversations", "cli"),
    ("~/.gemini/antigravity-ide/conversations", "ide"),
    ("~/.gemini/antigravity-backup/conversations", "backup"),
    ("~/.config/antigravity/conversations", "config"),
]


def discover_databases(override_env: dict[str, str] | None = None) -> list[DiscoveredDatabase]:
    """Discover AntiGravity SQLite conversation databases from environment or default locations.

    Args:
        override_env (dict[str, str] | None, optional): Custom environment dict for testing.
            If None, `os.environ` is used. Defaults to None.

    Returns:
        list[DiscoveredDatabase]: Deduplicated candidates sorted by canonical path string.
    """
    env_source = override_env if override_env is not None else os.environ

    discovered_by_canonical: dict[Path, DiscoveredDatabase] = {}

    if "ANTIGRAVITY_DATA_DIR" in env_source:
        data_dir_str = env_source["ANTIGRAVITY_DATA_DIR"]
        raw_roots = [item.strip() for item in data_dir_str.split(",") if item.strip()]
        for root_str in raw_roots:
            p = Path(root_str).expanduser()
            target_dir: Path | None = None
            if (p / "conversations").is_dir():
                target_dir = p / "conversations"
            elif p.is_dir():
                target_dir = p

            if target_dir is not None:
                _scan_directory(target_dir, "env", discovered_by_canonical)
    else:
        for path_template, source_label in DEFAULT_ROOT_SPECS:
            p = Path(path_template).expanduser()
            if p.is_dir():
                _scan_directory(p, source_label, discovered_by_canonical)

    sorted_candidates = sorted(
        discovered_by_canonical.values(),
        key=lambda item: str(item.database_path),
    )
    return sorted_candidates


def _scan_directory(
    directory: Path,
    source_label: str,
    discovered_by_canonical: dict[Path, DiscoveredDatabase],
) -> None:
    """Recursively scan a directory for .db files and populate discovered_by_canonical.

    Args:
        directory (Path): Root directory to recursively scan.
        source_label (str): Discovery source label to assign.
        discovered_by_canonical (dict[Path, DiscoveredDatabase]): Map tracking discovered paths.
    """
    for entry in directory.rglob("*.db"):
        if entry.is_file() and entry.name.endswith(".db"):
            canonical_path = entry.resolve()
            if canonical_path not in discovered_by_canonical:
                session_id = canonical_path.stem
                discovered_by_canonical[canonical_path] = DiscoveredDatabase(
                    database_path=canonical_path,
                    discovery_source=source_label,
                    session_id=session_id,
                )
