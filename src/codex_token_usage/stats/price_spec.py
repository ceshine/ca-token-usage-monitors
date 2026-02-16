"""Price specification fetch and cache helpers."""

from __future__ import annotations

import logging
import os
import time
import urllib.request
from pathlib import Path
from typing import Any

import orjson

LOGGER = logging.getLogger(__name__)
URL = "https://raw.githubusercontent.com/BerriAI/litellm/refs/heads/main/model_prices_and_context_window.json"
DEFAULT_PRICE_CACHE_PATH = Path("~/.gemini/prices.json").expanduser()

# Keep the same cache-path default behavior used in the reference utility.
os.environ.setdefault("PRICE_CACHE_PATH", str(DEFAULT_PRICE_CACHE_PATH))


def _fetch_from_url() -> dict[str, Any]:
    """Fetch the latest price specification from the remote URL."""
    try:
        with urllib.request.urlopen(URL) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to fetch price spec: HTTP {response.status}")
            return orjson.loads(response.read())
    except Exception as exc:  # pragma: no cover - network failures vary by runtime.
        raise RuntimeError(f"Failed to fetch price spec from {URL}") from exc


def get_price_spec(update_interval_seconds: int = 86400) -> dict[str, Any]:
    """Fetch and cache model pricing data.

    Args:
        update_interval_seconds: Minimum number of seconds between cache refreshes.

    Returns:
        Model pricing data keyed by model code.

    Raises:
        RuntimeError: If no usable fresh/stale cache exists and remote fetch fails.
    """
    cache_path_str = os.environ.get("PRICE_CACHE_PATH")

    if not cache_path_str:
        return _fetch_from_url()

    cache_file = Path(cache_path_str)

    if cache_file.exists():
        mtime = cache_file.stat().st_mtime
        if time.time() - mtime < update_interval_seconds:
            try:
                with cache_file.open("rb") as handle:
                    return orjson.loads(handle.read())
            except Exception:
                LOGGER.warning("Failed reading fresh cache at %s; refetching.", cache_file)

    try:
        json_data = _fetch_from_url()
    except Exception as exc:
        if cache_file.exists():
            try:
                with cache_file.open("rb") as handle:
                    return orjson.loads(handle.read())
            except Exception:
                LOGGER.warning("Failed reading stale cache at %s after fetch error.", cache_file)
        raise RuntimeError(f"Failed to fetch price spec from {URL}") from exc

    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("wb") as handle:
            handle.write(orjson.dumps(json_data))
    except Exception:
        LOGGER.warning("Failed writing price cache at %s.", cache_file)

    return json_data
