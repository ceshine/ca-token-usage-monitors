# Copyright 2025 Ceshine Lee
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
import urllib.request
from pathlib import Path
from typing import Any

import orjson

URL = "https://raw.githubusercontent.com/BerriAI/litellm/refs/heads/main/model_prices_and_context_window.json"


def _fetch_from_url() -> dict[str, Any]:
    """Fetches the latest price specification from the remote URL.

    Returns:
        A dictionary containing the model pricing information.

    Raises:
        RuntimeError: If the HTTP request fails or returns a non-200 status code.
    """
    try:
        with urllib.request.urlopen(URL) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to fetch price spec: HTTP {response.status}")
            data = response.read()
            return orjson.loads(data)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch price spec from {URL}") from e


def get_price_spec(update_interval_seconds: int = 86400) -> dict[str, Any]:
    """Fetches the LLM price spec mapping from the remote URL, with local caching.

    The cache path is determined by the `PRICE_CACHE_PATH` environment variable.
    If the variable is not set or empty, caching is disabled.

    Args:
        update_interval_seconds (int): The minimum interval in seconds to update the cache.
            Defaults to 86400 (24 hours).

    Returns:
        A dictionary containing the price specification for various models.

    Raises:
        RuntimeError: If fetching from the URL fails and no valid cache is available.
    """
    cache_path_str = os.environ.get("PRICE_CACHE_PATH")

    if not cache_path_str:
        return _fetch_from_url()

    cache_file = Path(cache_path_str)

    # Check if cache exists and is fresh enough
    if cache_file.exists():
        mtime = cache_file.stat().st_mtime
        if time.time() - mtime < update_interval_seconds:
            try:
                with open(cache_file, "rb") as f:
                    return orjson.loads(f.read())
            except Exception:
                # If read fails, fall through to re-fetch
                pass

    # Fetch from URL
    try:
        json_data = _fetch_from_url()
    except Exception as e:
        # If fetch fails but we have a stale cache, try to return that
        if cache_file.exists():
            try:
                with open(cache_file, "rb") as f:
                    return orjson.loads(f.read())
            except Exception:
                pass  # Reraise original error if cache read also fails
        raise RuntimeError(f"Failed to fetch price spec from {URL}") from e

    # Save to cache
    try:
        # Ensure directory exists
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "wb") as f:
            f.write(orjson.dumps(json_data))
    except Exception as e:
        # Warning: Failed to write cache, but we can still return the data
        print(f"Warning: Failed to write to cache at {cache_file}: {e}")

    return json_data
