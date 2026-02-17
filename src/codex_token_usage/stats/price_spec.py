"""Compatibility wrapper around shared model pricing utilities."""

from model_pricing.price_spec import (
    DEFAULT_PRICE_CACHE_PATH,
    DEFAULT_PRICE_SPEC_URL,
    PriceSpecConfig,
    get_price_spec,
)

URL = DEFAULT_PRICE_SPEC_URL

__all__ = [
    "DEFAULT_PRICE_CACHE_PATH",
    "DEFAULT_PRICE_SPEC_URL",
    "PriceSpecConfig",
    "URL",
    "get_price_spec",
]
