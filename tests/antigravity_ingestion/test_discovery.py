"""Tests for AntiGravity database discovery module."""

from __future__ import annotations

from pathlib import Path

import pytest

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.discovery import discover_databases


def test_default_roots_mapping_and_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify default roots map to correct discovery_source labels and priority."""
    home_dir = tmp_path / "home"
    config_dir = tmp_path / "config"

    # Priority 1: default (~/.gemini/antigravity/conversations)
    default_root = home_dir / ".gemini" / "antigravity" / "conversations"
    default_root.mkdir(parents=True)
    db1 = default_root / "session1.db"
    db1.touch()

    # Priority 2: cli (~/.gemini/antigravity-cli/conversations)
    cli_root = home_dir / ".gemini" / "antigravity-cli" / "conversations"
    cli_root.mkdir(parents=True)
    db2 = cli_root / "session2.db"
    db2.touch()

    # Priority 3: ide (~/.gemini/antigravity-ide/conversations)
    ide_root = home_dir / ".gemini" / "antigravity-ide" / "conversations"
    ide_root.mkdir(parents=True)
    db3 = ide_root / "session3.db"
    db3.touch()

    # Priority 4: acp (~/.gemini/antigravity-acp/conversations)
    acp_root = home_dir / ".gemini" / "antigravity-acp" / "conversations"
    acp_root.mkdir(parents=True)
    db4 = acp_root / "session4.db"
    db4.touch()

    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_dir))

    discovered = discover_databases(override_env={})
    sources = {str(d.database_path.name): d.discovery_source for d in discovered}

    assert sources["session1.db"] == "default"
    assert sources["session2.db"] == "cli"
    assert sources["session3.db"] == "ide"
    assert sources["session4.db"] == "acp"


def test_missing_roots_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify missing root directories do not cause discovery to fail."""
    empty_home = tmp_path / "empty_home"
    empty_home.mkdir()
    monkeypatch.setenv("HOME", str(empty_home))

    discovered = discover_databases(override_env={})
    assert discovered == []


def test_env_override_replaces_default_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify present ANTIGRAVITY_DATA_DIR replaces default roots entirely."""
    home_dir = tmp_path / "home"
    default_root = home_dir / ".gemini" / "antigravity" / "conversations"
    default_root.mkdir(parents=True)
    (default_root / "default_session.db").touch()

    env_root = tmp_path / "custom_env_dir"
    env_root.mkdir()
    (env_root / "env_session.db").touch()

    discovered = discover_databases(override_env={"ANTIGRAVITY_DATA_DIR": str(env_root)})
    assert len(discovered) == 1
    assert discovered[0].discovery_source == "env"
    assert discovered[0].database_path.name == "env_session.db"


def test_env_override_selects_conversations_subdir_if_present(tmp_path: Path) -> None:
    """Verify environment root selects root/conversations if present, else root."""
    root1 = tmp_path / "root1"
    conv1 = root1 / "conversations"
    conv1.mkdir(parents=True)
    (conv1 / "sub_session.db").touch()

    root2 = tmp_path / "root2"
    root2.mkdir(parents=True)
    (root2 / "direct_session.db").touch()

    env_val = f"{root1},{root2}"
    discovered = discover_databases(override_env={"ANTIGRAVITY_DATA_DIR": env_val})

    names = {d.database_path.name for d in discovered}
    assert names == {"sub_session.db", "direct_session.db"}
    assert all(d.discovery_source == "env" for d in discovered)


def test_empty_or_whitespace_env_override_discovers_no_roots(tmp_path: Path) -> None:
    """Verify empty/whitespace ANTIGRAVITY_DATA_DIR discovers no roots and does not fallback."""
    home_dir = tmp_path / "home"
    default_root = home_dir / ".gemini" / "antigravity" / "conversations"
    default_root.mkdir(parents=True)
    (default_root / "default_session.db").touch()

    discovered = discover_databases(override_env={"ANTIGRAVITY_DATA_DIR": "   "})
    assert discovered == []


def test_recursive_db_discovery_and_sorting(tmp_path: Path) -> None:
    """Verify recursive .db file collection and path sorting."""
    env_root = tmp_path / "root"
    nested_dir = env_root / "nested" / "deep"
    nested_dir.mkdir(parents=True)

    db_b = env_root / "b_session.db"
    db_a = nested_dir / "a_session.db"
    db_b.touch()
    db_a.touch()

    discovered = discover_databases(override_env={"ANTIGRAVITY_DATA_DIR": str(env_root)})
    assert len(discovered) == 2
    # Sorted by canonical path
    paths = [d.database_path for d in discovered]
    assert paths == sorted(paths)


def test_symlink_deduplication_and_earliest_priority_label(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify canonical deduplication collapses symlinks and retains earliest priority label."""
    home_dir = tmp_path / "home"

    # Default root (Priority 1)
    default_root = home_dir / ".gemini" / "antigravity" / "conversations"
    default_root.mkdir(parents=True)
    real_db = default_root / "real_session.db"
    real_db.touch()

    # CLI root (Priority 2) with a symlink to real_db
    cli_root = home_dir / ".gemini" / "antigravity-cli" / "conversations"
    cli_root.mkdir(parents=True)
    symlink_db = cli_root / "symlink_session.db"
    symlink_db.symlink_to(real_db)

    monkeypatch.setenv("HOME", str(home_dir))

    discovered = discover_databases(override_env={})
    # Should deduplicate to 1 database with discovery_source='default'
    assert len(discovered) == 1
    assert discovered[0].database_path == real_db.resolve()
    assert discovered[0].discovery_source == "default"
    assert discovered[0].session_id == "real_session"


def test_gemini_home_overrides_all_gemini_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify GEMINI_HOME replaces ~/.gemini for all antigravity* roots when ANTIGRAVITY_DATA_DIR is absent."""
    home_dir = tmp_path / "home"
    default_acp = home_dir / ".gemini" / "antigravity-acp" / "conversations"
    default_acp.mkdir(parents=True)
    (default_acp / "ignored_home_session.db").touch()

    custom_gemini = tmp_path / "custom_gemini"
    for subdir, filename in [
        ("antigravity/conversations", "s_default.db"),
        ("antigravity-cli/conversations", "s_cli.db"),
        ("antigravity-ide/conversations", "s_ide.db"),
        ("antigravity-acp/conversations", "s_acp.db"),
        ("antigravity-backup/conversations", "s_backup.db"),
    ]:
        target = custom_gemini / subdir
        target.mkdir(parents=True)
        (target / filename).touch()

    monkeypatch.setenv("HOME", str(home_dir))

    discovered = discover_databases(override_env={"GEMINI_HOME": str(custom_gemini)})
    sources = {d.database_path.name: d.discovery_source for d in discovered}
    assert sources == {
        "s_default.db": "default",
        "s_cli.db": "cli",
        "s_ide.db": "ide",
        "s_acp.db": "acp",
        "s_backup.db": "backup",
    }

    # ANTIGRAVITY_DATA_DIR still takes precedence over GEMINI_HOME
    env_dir = tmp_path / "override_env_dir"
    env_dir.mkdir()
    (env_dir / "s_env.db").touch()
    env_discovered = discover_databases(
        override_env={"GEMINI_HOME": str(custom_gemini), "ANTIGRAVITY_DATA_DIR": str(env_dir)}
    )
    assert [(d.database_path.name, d.discovery_source) for d in env_discovered] == [("s_env.db", "env")]


def test_empty_gemini_home_falls_back_to_default_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify whitespace/empty GEMINI_HOME falls back to ~/.gemini."""
    home_dir = tmp_path / "home"
    acp_root = home_dir / ".gemini" / "antigravity-acp" / "conversations"
    acp_root.mkdir(parents=True)
    (acp_root / "home_acp.db").touch()
    monkeypatch.setenv("HOME", str(home_dir))

    discovered = discover_databases(override_env={"GEMINI_HOME": "   "})
    assert [(d.database_path.name, d.discovery_source) for d in discovered] == [("home_acp.db", "acp")]
