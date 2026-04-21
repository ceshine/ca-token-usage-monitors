"""Tests for shared model pricing fetch/cache utilities."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import orjson
import pytest

from coding_agent_usage_monitors.common.model_pricing import price_spec as price_spec_module


def test_get_price_spec_uses_fresh_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh cache should be returned without attempting a remote fetch."""
    cache_file = tmp_path / "prices.json"
    cached_data = {"gpt-5": {"input_cost_per_token": 0.001}}
    cache_file.write_bytes(orjson.dumps(cached_data))

    def _unexpected_fetch(url: str) -> dict[str, Any]:
        raise AssertionError(f"Unexpected fetch for {url}")

    monkeypatch.setattr(price_spec_module, "_fetch_from_url", _unexpected_fetch)
    monkeypatch.setattr(price_spec_module, "_load_opencode_zen_pricing", lambda: {})

    result = price_spec_module.get_price_spec(update_interval_seconds=86400, cache_path=cache_file)

    assert result == cached_data


def test_get_price_spec_refreshes_stale_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stale cache should be refreshed and rewritten with fetched data."""
    cache_file = tmp_path / "prices.json"
    stale_data = {"old": {"input_cost_per_token": 0.1}}
    refreshed_data = {"new": {"input_cost_per_token": 0.2}}
    cache_file.write_bytes(orjson.dumps(stale_data))
    stale_time = time.time() - 10000
    _ = os.utime(cache_file, (stale_time, stale_time))

    monkeypatch.setattr(price_spec_module, "_fetch_from_url", lambda _: refreshed_data)
    monkeypatch.setattr(price_spec_module, "_load_opencode_zen_pricing", lambda: {})

    result = price_spec_module.get_price_spec(update_interval_seconds=60, cache_path=cache_file)

    assert result == refreshed_data
    assert orjson.loads(cache_file.read_bytes()) == refreshed_data


def test_get_price_spec_falls_back_to_stale_cache_on_fetch_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When fetch fails, stale cache should be returned if readable."""
    cache_file = tmp_path / "prices.json"
    stale_data = {"fallback": {"output_cost_per_token": 0.3}}
    cache_file.write_bytes(orjson.dumps(stale_data))
    stale_time = time.time() - 10000
    _ = os.utime(cache_file, (stale_time, stale_time))

    def _failing_fetch(_: str) -> dict[str, Any]:
        raise RuntimeError("boom")

    monkeypatch.setattr(price_spec_module, "_fetch_from_url", _failing_fetch)
    monkeypatch.setattr(price_spec_module, "_load_opencode_zen_pricing", lambda: {})

    result = price_spec_module.get_price_spec(update_interval_seconds=60, cache_path=cache_file)

    assert result == stale_data


def test_get_price_spec_uses_env_cache_path_when_cache_path_not_provided(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PRICE_CACHE_PATH should be honored when cache_path is omitted."""
    cache_file = tmp_path / "env-prices.json"
    cached_data = {"o3": {"cache_read_input_token_cost": 0.0003}}
    cache_file.write_bytes(orjson.dumps(cached_data))
    monkeypatch.setenv("PRICE_CACHE_PATH", str(cache_file))

    def _unexpected_fetch(url: str) -> dict[str, Any]:
        raise AssertionError(f"Unexpected fetch for {url}")

    monkeypatch.setattr(price_spec_module, "_fetch_from_url", _unexpected_fetch)
    monkeypatch.setattr(price_spec_module, "_load_opencode_zen_pricing", lambda: {})

    result = price_spec_module.get_price_spec(update_interval_seconds=86400)

    assert result == cached_data


def test_get_price_spec_disables_cache_when_cache_path_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit None cache path should bypass all cache reads/writes."""
    fetched_data = {"gpt-4.1": {"input_cost_per_token": 0.004}}
    monkeypatch.setattr(price_spec_module, "_fetch_from_url", lambda _: fetched_data)
    monkeypatch.setattr(price_spec_module, "_load_opencode_zen_pricing", lambda: {})

    result = price_spec_module.get_price_spec(cache_path=None)

    assert result == fetched_data


def test_load_opencode_zen_pricing_returns_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Load bundled OpenCode Zen pricing data."""
    result = price_spec_module._load_opencode_zen_pricing()

    assert isinstance(result, dict)
    assert len(result) > 0
    # Check some known models
    assert "opencode/gpt-5.4" in result
    assert "opencode/claude-opus-4-7" in result
    assert result["opencode/gpt-5.4"]["input_cost_per_token"] == 0.0000025


def test_load_opencode_zen_pricing_with_tiered_pricing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Models with tiered pricing should have above_200k_tokens variants."""
    result = price_spec_module._load_opencode_zen_pricing()

    claude_sonnet = result["opencode/claude-sonnet-4-5"]
    assert "input_cost_per_token_above_200k_tokens" in claude_sonnet
    assert claude_sonnet["input_cost_per_token"] == 0.000003
    assert claude_sonnet["input_cost_per_token_above_200k_tokens"] == 0.000006

    gemini = result["opencode/gemini-3.1-pro"]
    assert "input_cost_per_token_above_200k_tokens" in gemini
    assert gemini["input_cost_per_token"] == 0.000002
    assert gemini["input_cost_per_token_above_200k_tokens"] == 0.000004


def test_merge_pricing_data_opencode_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    """OpenCode Zen data should take precedence over litellm data."""
    litellm_data = {
        "opencode/gpt-5.4": {
            "input_cost_per_token": 0.001,
            "output_cost_per_token": 0.01,
        },
        "other/model": {"input_cost_per_token": 0.002},
    }
    opencode_data = {
        "opencode/gpt-5.4": {
            "input_cost_per_token": 0.0000025,
            "output_cost_per_token": 0.000015,
        },
    }

    result = price_spec_module._merge_pricing_data(litellm_data, opencode_data)

    # OpenCode zen should override
    assert result["opencode/gpt-5.4"]["input_cost_per_token"] == 0.0000025
    # Other models should remain
    assert result["other/model"]["input_cost_per_token"] == 0.002


def test_merge_pricing_data_handles_empty_opencode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Merge should work when OpenCode data is empty."""
    litellm_data = {"gpt-4": {"input_cost_per_token": 0.003}}

    result = price_spec_module._merge_pricing_data(litellm_data, {})

    assert result == litellm_data


def test_get_price_spec_merges_with_opencode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_price_spec should merge OpenCode Zen data with fetched litellm data."""
    cache_file = tmp_path / "prices.json"
    litellm_data = {"gpt-4": {"input_cost_per_token": 0.003}}
    cache_file.write_bytes(orjson.dumps(litellm_data))

    result = price_spec_module.get_price_spec(update_interval_seconds=86400, cache_path=cache_file)

    # Should have both litellm and OpenCode Zen data
    assert "gpt-4" in result
    assert "opencode/gpt-5.4" in result
    # OpenCode data should have correct pricing
    assert result["opencode/gpt-5.4"]["input_cost_per_token"] == 0.0000025


def test_get_price_spec_without_cache_merges_opencode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Even without cache, OpenCodeZen data should be merged."""
    fetched_data = {"gpt-4": {"input_cost_per_token": 0.003}}
    monkeypatch.setattr(price_spec_module, "_fetch_from_url", lambda _: fetched_data)

    result = price_spec_module.get_price_spec(cache_path=None)

    assert "gpt-4" in result
    assert "opencode/gpt-5.4" in result
