"""Tests for the crof.ai pricing refresh script."""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest

from scripts import update_crof_ai_pricing


def test_transform_crof_models_converts_prices_and_metadata() -> None:
    """crof.ai response data should be converted to the bundled pricing schema."""
    raw_data = {
        "data": [
            {
                "context_length": 1_000_000,
                "id": "example-model",
                "max_completion_tokens": 131_072,
                "pricing": {
                    "cache_prompt": "0.003",
                    "completion": "0.80",
                    "prompt": "0.35",
                },
                "quantization": "Q8_0",
                "sub_req_cost": 40,
            },
            {
                "context_length": 262_144,
                "id": "vision-model",
                "max_completion_tokens": 262_144,
                "pricing": {
                    "cache_prompt": "0.04",
                    "completion": "1.50",
                    "prompt": "0.20",
                },
                "quantization": "Q4_0",
                "sub_req_cost": 0.75,
                "vision": True,
            },
        ]
    }

    result = update_crof_ai_pricing.transform_crof_models(raw_data)

    assert result == {
        "crofai/example-model": {
            "input_cost_per_token": 3.5e-7,
            "output_cost_per_token": 8e-7,
            "cache_read_input_token_cost": 3e-9,
            "max_input_tokens": 1_000_000,
            "max_output_tokens": 131_072,
            "quantization": "Q8_0",
            "request_multiplier": "40x",
        },
        "crofai/vision-model": {
            "input_cost_per_token": 2e-7,
            "output_cost_per_token": 1.5e-6,
            "cache_read_input_token_cost": 4e-8,
            "max_input_tokens": 262_144,
            "max_output_tokens": 262_144,
            "quantization": "Q4_0",
            "vision": True,
            "request_multiplier": "0.75x",
        },
    }


def test_transform_crof_models_requires_data_list() -> None:
    """Malformed crof.ai responses should fail fast with a useful error."""
    with pytest.raises(ValueError, match="field 'data' to be a list"):
        update_crof_ai_pricing.transform_crof_models({"data": {}})


def test_write_pricing_file_writes_indented_json(tmp_path: Path) -> None:
    """Pricing output should be written as readable JSON."""
    output_path = tmp_path / "nested" / "crof-ai.json"
    pricing_data = {"crofai/example": {"input_cost_per_token": 1e-6}}

    update_crof_ai_pricing.write_pricing_file(pricing_data, output_path)

    assert output_path.read_text().endswith("\n")
    assert orjson.loads(output_path.read_bytes()) == pricing_data
