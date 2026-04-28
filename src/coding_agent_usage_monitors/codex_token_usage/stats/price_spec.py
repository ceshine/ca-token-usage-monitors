"""Compatibility wrapper around shared model pricing utilities."""

from coding_agent_usage_monitors.common.model_pricing.price_spec import (
    get_price_spec,
    DEFAULT_PRICE_SPEC_URL,
    DEFAULT_PRICE_CACHE_PATH,
)

URL = DEFAULT_PRICE_SPEC_URL

__all__ = [
    "DEFAULT_PRICE_CACHE_PATH",
    "DEFAULT_PRICE_SPEC_URL",
    "URL",
    "get_price_spec",
]
