"""Price specification fetch and cache helpers."""

from __future__ import annotations

import os
import time
import urllib.request
import logging
from pathlib import Path
from importlib.resources import files as import_resource_files
from dataclasses import dataclass
from typing import Any

import orjson

from coding_agent_usage_monitors.common.paths import get_default_price_cache_path

LOGGER = logging.getLogger(__name__)
DEFAULT_PRICE_SPEC_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/refs/heads/main/model_prices_and_context_window.json"
)
_CACHE_PATH_UNSET = object()


DEFAULT_PRICE_CACHE_PATH = get_default_price_cache_path()


@dataclass(frozen=True)
class PriceSpecConfig:
    """Configuration for fetching and caching model pricing data.

    Attributes:
        url: Remote JSON endpoint that returns pricing metadata.
        update_interval_seconds: Minimum refresh interval for cache updates.
        cache_path: Cache location.
            - `None` disables cache reads/writes.
            - `Path` uses that explicit cache location.
            - Omitted uses `PRICE_CACHE_PATH` env var when present, otherwise
              `DEFAULT_PRICE_CACHE_PATH`.
    """

    url: str = DEFAULT_PRICE_SPEC_URL
    update_interval_seconds: int = 86400
    cache_path: Path | None | object = _CACHE_PATH_UNSET


def _fetch_from_url(url: str) -> dict[str, Any]:
    """Fetch the latest price specification from a URL."""
    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to fetch price spec: HTTP {response.status}")
            return orjson.loads(response.read())
    except Exception as exc:  # pragma: no cover - network failures vary by runtime.
        raise RuntimeError(f"Failed to fetch price spec from {url}") from exc


def _resolve_cache_path(cache_path: Path | str | None | object) -> Path | None:
    """Resolve the effective cache path with env/default compatibility behavior."""
    if cache_path is _CACHE_PATH_UNSET:
        env_cache_path = os.environ.get("PRICE_CACHE_PATH")
        return Path(env_cache_path).expanduser() if env_cache_path else DEFAULT_PRICE_CACHE_PATH
    if cache_path is None:
        return None
    assert isinstance(cache_path, (Path, str)), f"Invalid cache_path: {cache_path}"
    return Path(cache_path).expanduser()


def _load_opencode_zen_pricing() -> dict[str, Any]:
    """Load the bundled OpenCode Zen pricing data."""
    try:
        # Try package resources first (works when installed as package)
        resource = import_resource_files("coding_agent_usage_monitors.common.model_pricing").joinpath(
            "opencode_zen_pricing.json"
        )
        return orjson.loads(resource.read_text())
    except Exception:
        pass

    # Fallback: load from filesystem (works in development)
    package_dir = Path(__file__).parent
    json_path = package_dir / "opencode_zen_pricing.json"
    if json_path.exists():
        try:
            with json_path.open("rb") as handle:
                return orjson.loads(handle.read())
        except Exception as exc:
            LOGGER.error("Failed loading OpenCode Zen pricing: %s", exc)
            raise

    return {}


def _merge_pricing_data(litellm_data: dict[str, Any], opencode_data: dict[str, Any]) -> dict[str, Any]:
    """Merge OpenCode Zen pricing into litellm data with precedence.

    OpenCode Zen data takes precedence for overlapping keys.
    """
    # Copy litellm data first, then update with OpenCode data
    merged = dict(litellm_data)
    merged.update(opencode_data)
    return merged


def get_price_spec(
    update_interval_seconds: int = 86400,
    *,
    cache_path: Path | str | None | object = _CACHE_PATH_UNSET,
    url: str = DEFAULT_PRICE_SPEC_URL,
) -> dict[str, Any]:
    """Fetch and cache model pricing data.

    Args:
        update_interval_seconds: Minimum number of seconds between cache refreshes.
        cache_path: Cache file path configuration.
            - Omitted: use `PRICE_CACHE_PATH` env var if set, else default cache path.
            - `None`: disable cache.
            - `Path` or `str`: use explicit path.
        url: URL to fetch pricing JSON from.

    Returns:
        Model pricing data keyed by model code.

    Raises:
        RuntimeError: If no usable fresh/stale cache exists and remote fetch fails.
    """
    effective_cache_path = _resolve_cache_path(cache_path)
    opencode_zen_pricing = _load_opencode_zen_pricing()

    if effective_cache_path is None:
        json_data = _fetch_from_url(url)
        return _merge_pricing_data(json_data, opencode_zen_pricing)

    if effective_cache_path.exists():
        mtime = effective_cache_path.stat().st_mtime
        if time.time() - mtime < update_interval_seconds:
            try:
                with effective_cache_path.open("rb") as handle:
                    json_data = orjson.loads(handle.read())
                    return _merge_pricing_data(json_data, opencode_zen_pricing)
            except Exception:
                LOGGER.error("Failed reading fresh cache at %s; refetching.", effective_cache_path)

    # Cache miss or stale - fetch from URL and update cache
    try:
        json_data = _fetch_from_url(url)
    except Exception as exc:
        if effective_cache_path.exists():
            LOGGER.warning("Failed fetching from %s; using stale cache at %s.", url, effective_cache_path)
            try:
                with effective_cache_path.open("rb") as handle:
                    json_data = orjson.loads(handle.read())
                    return _merge_pricing_data(json_data, opencode_zen_pricing)
            except Exception:
                LOGGER.error("Failed reading stale cache at %s after fetch error.", effective_cache_path)
        raise RuntimeError(f"Failed to fetch price spec from {url}") from exc

    try:
        effective_cache_path.parent.mkdir(parents=True, exist_ok=True)
        with effective_cache_path.open("wb") as handle:
            handle.write(orjson.dumps(json_data))
    except Exception:
        LOGGER.error("Failed writing price cache at %s.", effective_cache_path)

    return _merge_pricing_data(json_data, opencode_zen_pricing)
