"""Discovery module for AntiGravity SQLite conversation databases."""

from __future__ import annotations

import os
from pathlib import Path
from collections.abc import Mapping

from .schemas import DiscoveredDatabase

DEFAULT_GEMINI_SUBDIR_SPECS: list[tuple[str, str]] = [
    ("antigravity/conversations", "default"),
    ("antigravity-cli/conversations", "cli"),
    ("antigravity-ide/conversations", "ide"),
    ("antigravity-acp/conversations", "acp"),
    ("antigravity-backup/conversations", "backup"),
]

DEFAULT_ROOT_SPECS: list[tuple[str, str]] = [
    *(("~/.gemini/" + subdir, source_label) for subdir, source_label in DEFAULT_GEMINI_SUBDIR_SPECS),
    ("~/.config/antigravity/conversations", "config"),
]


def _resolve_default_roots(env_source: Mapping[str, str]) -> list[tuple[Path, str]]:
    """Resolve default conversation root directories, honoring GEMINI_HOME when set.

    Args:
        env_source (Mapping[str, str]): Environment variables mapping.

    Returns:
        list[tuple[Path, str]]: Ordered list of `(conversations_dir, source_label)` pairs.
    """
    gemini_home = env_source.get("GEMINI_HOME", "").strip()
    if gemini_home:
        gemini_base = Path(gemini_home).expanduser()
        return [
            *((gemini_base / subdir, source_label) for subdir, source_label in DEFAULT_GEMINI_SUBDIR_SPECS),
            (Path("~/.config/antigravity/conversations").expanduser(), "config"),
        ]

    return [(Path(path_template).expanduser(), source_label) for path_template, source_label in DEFAULT_ROOT_SPECS]


def discover_databases(override_env: Mapping[str, str] | None = None) -> list[DiscoveredDatabase]:
    """Discover AntiGravity SQLite conversation databases from environment or default locations.

    Args:
        override_env (Mapping[str, str] | None, optional): Custom environment mapping for testing.
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
        for root_path, source_label in _resolve_default_roots(env_source):
            if root_path.is_dir():
                _scan_directory(root_path, source_label, discovered_by_canonical)

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
