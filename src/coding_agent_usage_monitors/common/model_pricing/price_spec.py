"""Price specification fetch and cache helpers."""

from __future__ import annotations

import os
import time
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Any, cast

import orjson
import requests

from coding_agent_usage_monitors.common.paths import get_default_price_cache_path

LOGGER = logging.getLogger(__name__)
DEFAULT_PRICE_SPEC_URL = "https://models.dev/api.json"
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
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return orjson.loads(response.content)
    except Exception as exc:  # pragma: no cover - network failures vary by runtime.
        raise RuntimeError(f"Failed to fetch price spec from {url}") from exc


def _transform_models_dev_format(raw_data: dict[str, Any]) -> dict[str, Any]:
    """Transform models.dev provider-nested format to flat ``provider/model`` keyed format.

    The models.dev API nests model pricing under provider keys (e.g. ``openai``, ``anthropic``)
    and expresses costs in USD per 1M tokens.  This function flattens the structure so that
    top-level keys are ``provider_id/model_id`` and converts costs to USD per token.

    Callers should look up pricing by prepending the canonical provider prefix
    (e.g. ``openai/gpt-4o``, ``anthropic/claude-sonnet-4-6``, ``google/gemini-2.5-flash``).

    Data that is already in the flat format (e.g. from a stale cache) is returned unchanged.

    Args:
        raw_data: Raw JSON from models.dev (provider-keyed) or from a flat-format cache.

    Returns:
        Flat ``provider/model`` keyed pricing dictionary with per-token costs.
    """
    # Detect format: models.dev nests under providers that each contain "id" and "models" keys.
    has_provider_structure = any(isinstance(v, dict) and "models" in v and "id" in v for v in raw_data.values())
    if not has_provider_structure:
        return raw_data  # Already flat; pass through.

    result: dict[str, Any] = {}
    for provider_id, provider_data in raw_data.items():
        if not isinstance(provider_data, dict):
            continue
        provider_name = provider_data.get("id", provider_id)
        models = provider_data.get("models", {})
        if not isinstance(models, dict):
            continue
        for model_id, model_data in models.items():
            if not isinstance(model_data, dict):
                continue

            # Start with a shallow copy of all model metadata fields.
            entry: dict[str, Any] = {k: v for k, v in model_data.items() if k not in ("cost", "limit")}

            # Transform cost entries
            cost = cast(dict[str, Any], model_data.get("cost", {}))
            # Convert $/MTok → $/token.
            if "input" in cost:
                entry["input_cost_per_token"] = cost["input"] / 1_000_000.0
            if "output" in cost:
                entry["output_cost_per_token"] = cost["output"] / 1_000_000.0
            if "cache_read" in cost:
                entry["cache_read_input_token_cost"] = cost["cache_read"] / 1_000_000.0
            # Tiered pricing (>200k tokens).
            context_over = cost.get("context_over_200k", {})
            if isinstance(context_over, dict):
                if "input" in context_over:
                    entry["input_cost_per_token_above_200k_tokens"] = context_over["input"] / 1_000_000.0
                if "output" in context_over:
                    entry["output_cost_per_token_above_200k_tokens"] = context_over["output"] / 1_000_000.0
                if "cache_read" in context_over:
                    entry["cache_read_input_token_cost_above_200k_tokens"] = context_over["cache_read"] / 1_000_000.0

            # Transform limit entries
            limit = cast(dict[str, Any], model_data.get("limit", {}))
            if "context" in limit:
                entry["max_input_tokens"] = limit["context"]
            if "output" in limit:
                entry["max_output_tokens"] = limit["output"]

            # Key format: provider_id/model_id
            result[f"{provider_name}/{model_id}"] = entry

    return result


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
        return _transform_models_dev_format(_fetch_from_url(url))

    if effective_cache_path.exists():
        mtime = effective_cache_path.stat().st_mtime
        if time.time() - mtime < update_interval_seconds:
            try:
                with effective_cache_path.open("rb") as handle:
                    return orjson.loads(handle.read())
            except Exception:
                LOGGER.error("Failed reading fresh cache at %s; refetching.", effective_cache_path)

    # Cache miss or stale - fetch from URL and update cache
    try:
        json_data = _transform_models_dev_format(_fetch_from_url(url))
    except Exception as exc:
        if effective_cache_path.exists():
            LOGGER.warning("Failed fetching from %s; using stale cache at %s.", url, effective_cache_path)
            try:
                with effective_cache_path.open("rb") as handle:
                    return orjson.loads(handle.read())
            except Exception:
                LOGGER.error("Failed reading stale cache at %s after fetch error.", effective_cache_path)
        raise RuntimeError(f"Failed to fetch price spec from {url}") from exc

    try:
        effective_cache_path.parent.mkdir(parents=True, exist_ok=True)
        with effective_cache_path.open("wb") as handle:
            handle.write(orjson.dumps(json_data))
    except Exception:
        LOGGER.error("Failed writing price cache at %s.", effective_cache_path)

    return json_data
