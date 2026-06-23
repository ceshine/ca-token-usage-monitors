#!/usr/bin/env python3
"""Refresh the bundled crof.ai model pricing file."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import typer
import orjson
import requests

LOGGER = logging.getLogger(__name__)
DEFAULT_CROF_MODELS_URL = "https://crof.ai/v1/models"
DEFAULT_OUTPUT_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "coding_agent_usage_monitors"
    / "common"
    / "model_pricing"
    / "crof-ai.json"
)
USD_PER_MILLION_TOKENS = Decimal("1000000")


def fetch_crof_models(url: str = DEFAULT_CROF_MODELS_URL) -> dict[str, Any]:
    """Fetch raw model metadata from crof.ai.

    Args:
        url (str): URL of the crof.ai OpenAI-compatible models endpoint.

    Returns:
        dict[str, Any]: Raw JSON response from the endpoint.

    Raises:
        RuntimeError: If the HTTP request or JSON parsing fails.
        ValueError: If the response body is not a JSON object.
    """
    LOGGER.info("Fetching crof.ai models from %s", url)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        raw_data = orjson.loads(response.content)
    except (requests.RequestException, orjson.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to fetch crof.ai model pricing from {url}.") from exc

    if not isinstance(raw_data, dict):
        raise ValueError(f"Expected crof.ai response from {url} to be a JSON object.")
    LOGGER.info("Successfully fetched crof.ai response from %s", url)
    return raw_data


def transform_crof_models(raw_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Transform crof.ai model metadata into the bundled pricing format.

    crof.ai reports token prices in USD per one million tokens. The bundled file stores
    prices in USD per token and keys each entry as ``crofai/{model_id}``.

    Args:
        raw_data (dict[str, Any]): Raw JSON response from the crof.ai models endpoint.

    Returns:
        dict[str, dict[str, Any]]: Pricing data keyed by ``crofai/{model_id}``.

    Raises:
        ValueError: If required model or pricing fields are missing or malformed.
    """
    models = raw_data.get("data")
    if not isinstance(models, list):
        raise ValueError("Expected crof.ai response field 'data' to be a list.")

    LOGGER.info("Transforming %d models from crof.ai response", len(models))
    result: dict[str, dict[str, Any]] = {}
    for index, model_value in enumerate(models):
        model = _as_object(model_value, f"model at index {index}")
        model_id = _required_string(model, "id", f"model at index {index}")
        pricing = _as_object(model.get("pricing"), f"pricing for model {model_id}")

        entry: dict[str, Any] = {
            "input_cost_per_token": _price_per_token(pricing, "prompt", model_id),
            "output_cost_per_token": _price_per_token(pricing, "completion", model_id),
            "cache_read_input_token_cost": _price_per_token(pricing, "cache_prompt", model_id),
            "max_input_tokens": _required_int(model, "context_length", model_id),
            "max_output_tokens": _required_int(model, "max_completion_tokens", model_id),
        }

        quantization = model.get("quantization")
        if quantization is not None:
            if not isinstance(quantization, str):
                raise ValueError(f"Expected quantization for model {model_id} to be a string.")
            entry["quantization"] = quantization

        vision = model.get("vision")
        if vision is not None:
            if not isinstance(vision, bool):
                raise ValueError(f"Expected vision for model {model_id} to be a boolean.")
            entry["vision"] = vision

        request_multiplier = model.get("sub_req_cost")
        if request_multiplier is not None:
            entry["request_multiplier"] = _format_request_multiplier(request_multiplier, model_id)

        key = f"crofai/{model_id}"
        if key in result:
            raise ValueError(f"Duplicate crof.ai model id in response: {model_id}")
        result[key] = entry

    return result


def write_pricing_file(pricing_data: dict[str, dict[str, Any]], output_path: Path) -> None:
    """Write crof.ai pricing data to disk as indented JSON.

    Args:
        pricing_data (dict[str, dict[str, Any]]): Transformed crof.ai pricing data.
        output_path (Path): JSON file path to write.

    Returns:
        None: This function writes the file as a side effect.
    """
    LOGGER.info("Writing %d pricing entries to %s", len(pricing_data), output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _ = output_path.write_bytes(orjson.dumps(pricing_data, option=orjson.OPT_INDENT_2) + b"\n")


def refresh_crof_pricing(
    url: str = DEFAULT_CROF_MODELS_URL,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> dict[str, dict[str, Any]]:
    """Fetch, transform, and write crof.ai pricing data.

    Args:
        url (str): URL of the crof.ai models endpoint.
        output_path (Path): JSON file path to refresh.

    Returns:
        dict[str, dict[str, Any]]: Transformed pricing data that was written.
    """
    raw_data = fetch_crof_models(url)
    pricing_data = transform_crof_models(raw_data)
    write_pricing_file(pricing_data, output_path)
    LOGGER.info("Refresh complete: %d pricing entries written to %s", len(pricing_data), output_path)
    return pricing_data


def _as_object(value: Any, context: str) -> dict[str, Any]:
    """Validate that a value is a JSON object.

    Args:
        value (Any): Value to validate.
        context (str): Human-readable context for error messages.

    Returns:
        dict[str, Any]: The validated JSON object.

    Raises:
        ValueError: If the value is not a dictionary.
    """
    if not isinstance(value, dict):
        raise ValueError(f"Expected {context} to be an object.")
    return value


def _required_string(data: dict[str, Any], field_name: str, context: str) -> str:
    """Read and validate a required string field.

    Args:
        data (dict[str, Any]): JSON object containing the field.
        field_name (str): Field name to validate.
        context (str): Human-readable context for error messages.

    Returns:
        str: The validated non-empty string value.

    Raises:
        ValueError: If the field is missing, empty, or not a string.
    """
    value = data.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Expected {field_name} for {context} to be a non-empty string.")
    return value


def _required_int(data: dict[str, Any], field_name: str, model_id: str) -> int:
    """Read and validate a required integer model field.

    Args:
        data (dict[str, Any]): Model JSON object containing the field.
        field_name (str): Field name to validate.
        model_id (str): Model identifier for error messages.

    Returns:
        int: The validated integer value.

    Raises:
        ValueError: If the field is missing, boolean, or not an integer.
    """
    value = data.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Expected {field_name} for model {model_id} to be an integer.")
    return value


def _price_per_token(pricing: dict[str, Any], field_name: str, model_id: str) -> float:
    """Convert a USD-per-million-token price field to USD per token.

    Args:
        pricing (dict[str, Any]): Model pricing object from crof.ai.
        field_name (str): Pricing field name to convert.
        model_id (str): Model identifier for error messages.

    Returns:
        float: USD per token for the requested pricing field.

    Raises:
        ValueError: If the field is missing, non-numeric, non-finite, or negative.
    """
    value = pricing.get(field_name)
    if value is None or isinstance(value, bool):
        raise ValueError(f"Expected pricing.{field_name} for model {model_id} to be numeric.")

    try:
        price_per_million = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Expected pricing.{field_name} for model {model_id} to be numeric.") from exc

    if not price_per_million.is_finite() or price_per_million < 0:
        raise ValueError(f"Expected pricing.{field_name} for model {model_id} to be a finite non-negative number.")

    return float(price_per_million / USD_PER_MILLION_TOKENS)


def _format_request_multiplier(value: Any, model_id: str) -> str:
    """Format crof.ai ``sub_req_cost`` as the existing request multiplier field.

    Args:
        value (Any): Raw ``sub_req_cost`` value from crof.ai.
        model_id (str): Model identifier for error messages.

    Returns:
        str: Request multiplier formatted as ``{number}x``.

    Raises:
        ValueError: If the value is non-numeric, non-finite, or negative.
    """
    if isinstance(value, bool):
        raise ValueError(f"Expected sub_req_cost for model {model_id} to be numeric.")

    try:
        multiplier = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Expected sub_req_cost for model {model_id} to be numeric.") from exc

    if not multiplier.is_finite() or multiplier < 0:
        raise ValueError(f"Expected sub_req_cost for model {model_id} to be a finite non-negative number.")

    multiplier_text = f"{multiplier.normalize():f}"
    if "." in multiplier_text:
        multiplier_text = multiplier_text.rstrip("0").rstrip(".")
    return f"{multiplier_text}x"


def main(
    url: str = typer.Option(DEFAULT_CROF_MODELS_URL, "--url", help="crof.ai models endpoint URL."),
    output_path: Path = typer.Option(DEFAULT_OUTPUT_PATH, "--output-path", "-o", help="Pricing JSON path to refresh."),
) -> None:
    """Refresh the bundled crof.ai pricing JSON file.

    Args:
        url (str): URL of the crof.ai models endpoint.
        output_path (Path): Pricing JSON path to refresh.

    Returns:
        None: This function writes the refreshed JSON file and prints a summary.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    resolved_output_path = output_path.expanduser()
    pricing_data = refresh_crof_pricing(url=url, output_path=resolved_output_path)
    typer.echo(f"Wrote {len(pricing_data)} crof.ai model pricing entries to {resolved_output_path}")


if __name__ == "__main__":
    typer.run(main)
