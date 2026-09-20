"""Tests for model and token normalization functions."""

from __future__ import annotations

from pathlib import Path

import pytest

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.errors import InvalidDataError
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.parser import (
    BASE_ALIASES,
    normalize_tokens,
    NUMERIC_MODEL_MAP,
    resolve_model_code,
    resolve_numeric_model_id,
    normalize_antigravity_model,
)


def test_numeric_model_id_mapping_and_fallbacks() -> None:
    """Cover every listed numeric model ID and fallbacks."""
    for model_id, expected_code in NUMERIC_MODEL_MAP.items():
        assert resolve_numeric_model_id(model_id) == expected_code

    # Unlisted below 1000
    assert resolve_numeric_model_id(999) == "antigravity-model-999"
    assert resolve_numeric_model_id(50) == "antigravity-model-50"

    # Unlisted >= 1000
    assert resolve_numeric_model_id(1071) == "gemini-3.6-flash-high"
    assert resolve_numeric_model_id(1084) == "model_placeholder_m84"


def test_placeholder_1000_rule_and_placeholder_aliases() -> None:
    """Verify unlisted >= 1000 maps to placeholder which then normalizes via base aliases."""
    # Model ID 1084 -> model_placeholder_m84 -> gemini-3-flash-preview
    placeholder_name = resolve_numeric_model_id(1084)
    assert placeholder_name == "model_placeholder_m84"
    assert normalize_antigravity_model(placeholder_name) == "gemini-3-flash-preview"


def test_gemini_internal_model_fallback() -> None:
    """Verify gemini-internal-model fallback when no model sources resolve."""
    code = resolve_model_code(
        usage_model_id=None,
        raw_model_name=None,
        meta_model_id=None,
        inherited_model=None,
    )
    assert code == "gemini-internal-model"


def test_candidate_usage_model_id_overrides_raw_name() -> None:
    """Verify candidate ModelUsage.model_id overrides raw model string."""
    # Model ID 312 -> gemini-2.5-flash
    code = resolve_model_code(
        usage_model_id=312,
        raw_model_name="claude-3-opus",
        meta_model_id=290,
        inherited_model="gemini-3-pro",
    )
    assert code == "gemini-2.5-flash"


def test_parenthesized_effort_aliases() -> None:
    """Verify parenthesized effort aliases for gemini 3.6/3.7/3.8 and high/medium/low."""
    assert normalize_antigravity_model("gemini 3.6 flash (high)") == "gemini-3.6-flash-high"
    assert normalize_antigravity_model("Gemini 3.7 Flash (Medium)") == "gemini-3.7-flash-medium"
    assert normalize_antigravity_model("GEMINI 3.8 FLASH (LOW)") == "gemini-3.8-flash-low"


def test_base_aliases_coverage() -> None:
    """Verify every base alias in BASE_ALIASES normalizes correctly."""
    for raw_alias, expected in BASE_ALIASES.items():
        assert normalize_antigravity_model(raw_alias) == expected


def test_recognized_family_fallback_and_unrecognized_preservation() -> None:
    """Verify space replacement for gemini/claude/gpt families, and original case for unrecognized."""
    # Recognized family with spaces
    assert normalize_antigravity_model("gemini 4 flash custom") == "gemini-4-flash-custom"
    assert normalize_antigravity_model("claude 4 sonnet custom") == "claude-4-sonnet-custom"
    assert normalize_antigravity_model("gpt 5 turbo") == "gpt-5-turbo"

    # Unrecognized family -> preserved trimmed original
    assert normalize_antigravity_model("  Custom-LLM-Model v1.0  ") == "Custom-LLM-Model v1.0"
    assert normalize_antigravity_model("llama-3-70b-instruct") == "llama-3-70b-instruct"


def test_token_reconciliation_and_totals() -> None:
    """Verify total output / output / reasoning reconciliation and total token calculation."""
    # Case 1: total_output = 100, visible_output = 70, reasoning = 40 (sum = 110 > total_output)
    # total_output_tokens -> max(100, 110) = 110
    # output_tokens -> max(70, 110 - 40) = 70
    # reasoning_tokens -> max(40, 110 - 70) = 40
    # total_tokens -> 10 (input) + 5 (c_create) + 2 (c_read) + 110 = 127
    _inp, out, reas, tot_out, _c_create, _c_read, tot = normalize_tokens(
        raw_input=10,
        raw_visible_output=70,
        raw_reasoning=40,
        raw_total_output=100,
        raw_cache_creation=5,
        raw_cache_read=2,
        database_path=Path("/tmp/dummy.db"),
        table="gen_metadata",
        row_idx=0,
    )
    assert tot_out == 110
    assert out == 70
    assert reas == 40
    assert tot == 127


def test_negative_token_counter_raises_invalid_data_error() -> None:
    """Verify negative raw token counter raises InvalidDataError."""
    with pytest.raises(InvalidDataError, match="Negative token counter"):
        normalize_tokens(
            raw_input=-1,
            raw_visible_output=0,
            raw_reasoning=0,
            raw_total_output=0,
            raw_cache_creation=0,
            raw_cache_read=0,
            database_path=Path("/tmp/dummy.db"),
            table="gen_metadata",
            row_idx=0,
        )
