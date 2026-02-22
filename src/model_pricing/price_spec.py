"""Price specification fetch and cache helpers."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import time
from typing import Any
import urllib.request

import orjson

LOGGER = logging.getLogger(__name__)
DEFAULT_PRICE_SPEC_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/refs/heads/main/model_prices_and_context_window.json"
)
_CACHE_PATH_UNSET = object()


def _default_price_cache_path() -> Path:
    """Return the default cache path following XDG conventions on Linux."""
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        base_cache_dir = Path(xdg_cache_home).expanduser()
    else:
        base_cache_dir = Path("~/.cache").expanduser()
    return base_cache_dir / "coding-agent-token-monitor" / "price_cache.json"


DEFAULT_PRICE_CACHE_PATH = _default_price_cache_path()


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

    if effective_cache_path is None:
        return _fetch_from_url(url)

    if effective_cache_path.exists():
        mtime = effective_cache_path.stat().st_mtime
        if time.time() - mtime < update_interval_seconds:
            try:
                with effective_cache_path.open("rb") as handle:
                    return orjson.loads(handle.read())
            except Exception:
                LOGGER.warning("Failed reading fresh cache at %s; refetching.", effective_cache_path)

    try:
        json_data = _fetch_from_url(url)
    except Exception as exc:
        if effective_cache_path.exists():
            try:
                with effective_cache_path.open("rb") as handle:
                    return orjson.loads(handle.read())
            except Exception:
                LOGGER.warning("Failed reading stale cache at %s after fetch error.", effective_cache_path)
        raise RuntimeError(f"Failed to fetch price spec from {url}") from exc

    try:
        effective_cache_path.parent.mkdir(parents=True, exist_ok=True)
        with effective_cache_path.open("wb") as handle:
            handle.write(orjson.dumps(json_data))
    except Exception:
        LOGGER.warning("Failed writing price cache at %s.", effective_cache_path)

    return json_data
