"""Tests for shared model pricing fetch/cache utilities."""

from __future__ import annotations

import os
from pathlib import Path
import time
from typing import Any

import orjson
import pytest

from model_pricing import price_spec as price_spec_module


def test_get_price_spec_uses_fresh_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh cache should be returned without attempting a remote fetch."""
    cache_file = tmp_path / "prices.json"
    cached_data = {"gpt-5": {"input_cost_per_token": 0.001}}
    cache_file.write_bytes(orjson.dumps(cached_data))

    def _unexpected_fetch(url: str) -> dict[str, Any]:
        raise AssertionError(f"Unexpected fetch for {url}")

    monkeypatch.setattr(price_spec_module, "_fetch_from_url", _unexpected_fetch)

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

    result = price_spec_module.get_price_spec(update_interval_seconds=86400)

    assert result == cached_data


def test_get_price_spec_disables_cache_when_cache_path_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit None cache path should bypass all cache reads/writes."""
    fetched_data = {"gpt-4.1": {"input_cost_per_token": 0.004}}
    monkeypatch.setattr(price_spec_module, "_fetch_from_url", lambda _: fetched_data)

    result = price_spec_module.get_price_spec(cache_path=None)

    assert result == fetched_data
