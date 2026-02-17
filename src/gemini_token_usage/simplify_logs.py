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

"""Module for simplifying log records.

## Simplification Levels:

-   **Level 0**: No simplification. All log records and all their fields are retained.
-   **Level 1**: Filters records to include only `gemini_cli.api_response` and `gemini_cli.api_request` events. All fields within these selected records are retained.
-   **Level 2**: Applies Level 1 filtering and further simplifies the structure of the retained records. Only the "attributes" and "_body" fields of the selected records are kept.
-   **Level 3**: The most aggressive simplification. Only `gemini_cli.api_response` events are retained. For these events, only specific token-related attributes (`duration_ms`, `input_token_count`, `output_token_count`, `cached_content_token_count`, `thoughts_token_count`, `total_token_count`, `tool_token_count`, `model`, and `session.id`) are preserved within the "attributes" field. The "_body" field is also retained.
"""

import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Any

import typer
import orjson
import orjsonl

LOGGER = logging.getLogger(__name__)
TYPER_APP = typer.Typer()


def simplify_record(record: dict[str, Any], level: int) -> dict[str, Any] | None:
    """Processes a log record based on the simplification level.

    Args:
        record (Dict[str, Any]): The log record to process.
        level (int): The simplification level to apply.

    Returns:
        Optional[Dict[str, Any]]: The processed record if it should be kept (potentially modified),
            or None if it should be discarded.
    """
    if level == 0:
        # No-op
        return record

    if level < 0 or level > 3:
        raise ValueError("Level must be between 0 and 3")

    if "attributes" not in record or "event.name" not in record["attributes"]:
        LOGGER.warning("Skipping record with no `attributes` field: %s", orjson.dumps(record).decode())
        return None

    try:
        attributes = record["attributes"]
        event_name = attributes["event.name"]

        # Level 3: Keep only gemini_cli.api_response and keep only the token count and duration attributes
        if level >= 3:
            if event_name != "gemini_cli.api_response":
                return None
            record["attributes"] = {
                "event.timestamp": attributes["event.timestamp"],
                "duration_ms": attributes["duration_ms"],
                "input_token_count": attributes["input_token_count"],
                "output_token_count": attributes["output_token_count"],
                "cached_content_token_count": attributes["cached_content_token_count"],
                "thoughts_token_count": attributes["thoughts_token_count"],
                "total_token_count": attributes["total_token_count"],
                "tool_token_count": attributes["tool_token_count"],
                "model": attributes["model"],
                "session.id": attributes["session.id"],
                # The event name is always "gemini_cli.api_response" for now
                "event.name": attributes["event.name"],
            }

        # Level 1: Filter for specific events
        if level >= 1 and level < 3:
            if event_name not in ("gemini_cli.api_response", "gemini_cli.api_request"):
                return None

        # Level 2: Only keep essential fields
        if level >= 2:
            record = {"attributes": record["attributes"], "_body": record["_body"]}
    except KeyError:
        # Skip records that don't have the expected structure
        LOGGER.warning("Skipping record with unexpected structure: %s", record)
        return None

    return record


def run_log_simplification(
    input_file_path: Path,
    level: int,
    archive_folder: Path = Path("/tmp"),
    disable_archiving: bool = False,
) -> int:
    """Simplifies an existing JSONL file by filtering and transforming its records.

    Args:
        input_file_path (Path): The path to the input JSONL file.
        level (int): The simplification level to apply to the records.
        archive_folder (Path): The folder where the original file will be archived
            before simplification, if archiving is not disabled. Defaults to /tmp.
        disable_archiving (bool): If True, the original file will be removed instead
            of archived. Defaults to False.

    Returns:
        int: An exit code (0 for success, 1 for failure).

    Raises:
        FileNotFoundError: If the input file does not exist.
        ValueError: If the input file is not a .jsonl file, or if a temporary/archive
            file already exists from a previous failed run.
        RuntimeError: For other unexpected errors during the simplification process.
    """
    if input_file_path.is_dir():
        if (input_file_path / "telemetry.jsonl").exists():
            input_file_path = input_file_path / "telemetry.jsonl"
        elif (input_file_path / ".gemini" / "telemetry.jsonl").exists():
            input_file_path = input_file_path / ".gemini" / "telemetry.jsonl"
        else:
            raise FileNotFoundError(
                f"Could not find telemetry.jsonl in {input_file_path} nor in its '.gemini' or 'gemini' subdirectories."
            )
    elif input_file_path.suffix != ".jsonl":
        raise ValueError("Input file must be a .jsonl file")

    if not input_file_path.exists():
        raise FileNotFoundError("Input file does not exist")

    if level == 0:
        LOGGER.warning("Level 0 is a no-op. Exiting.")
        return 0
    if level < 0 or level > 3:
        raise ValueError("Level must be between 0 and 3")

    temp_file = input_file_path.with_suffix(".jsonl.tmp")
    if temp_file.exists():
        raise ValueError(
            f"Temp file {temp_file} already exists (possibly from a previous failed run). Please clean up manually and try again."
        )

    archive_file_path: Path | None = None
    if disable_archiving is False:
        # Move the original file to the archive folder
        archive_target_name = f"{input_file_path.stem}.{int(datetime.now().timestamp())}{input_file_path.suffix}"
        archive_file_path = archive_folder / archive_target_name
        if archive_file_path.exists():
            raise ValueError(
                f"Archive file {archive_file_path} already exists. Please clean up manually and try again."
            )

    try:
        with open(temp_file, "wb") as fout:
            for obj in orjsonl.stream(input_file_path):
                if not isinstance(obj, dict):
                    LOGGER.warning("Found malformed record in %s. Skipping...", input_file_path)
                    continue
                if (simplified_obj := simplify_record(obj, level=level)) is not None:
                    _ = fout.write(orjson.dumps(simplified_obj) + b"\n")

        if disable_archiving is False:
            assert archive_file_path is not None, "Archive file path is not set"
            _ = shutil.move(input_file_path, archive_file_path)
            LOGGER.info("Archived %s to %s", input_file_path, archive_file_path)
        else:
            # Remove the original file
            input_file_path.unlink()  # Use unlink for files
            LOGGER.info("Removed %s", input_file_path)

        # Move the temp file to the original file path
        _ = temp_file.rename(input_file_path)

        LOGGER.info("%s has been simplified at level %d", input_file_path, level)
        return 0
    except Exception as e:
        LOGGER.error("An unexpected error occurred during log simplification: %s", e)
        return 1


@TYPER_APP.command()
def main(
    input_file_path: Path = typer.Argument(
        ...,
        help="The path to the input JSONL file.",
        exists=True,
        dir_okay=True,
        readable=True,
    ),
    level: int = typer.Option(
        1,
        "--level",
        "-l",
        help="The simplification level to apply to the records (0, 1, 2, or 3).",
        min=0,
        max=3,
    ),
    archive_folder: Path = typer.Option(
        Path("/tmp"),
        "--archive-folder",
        "-a",
        help="The folder where the original file will be archived before simplification.",
    ),
    disable_archiving: bool = typer.Option(
        False,
        "--disable-archiving",
        "-d",
        help="If set, the original file will be removed instead of archived.",
    ),
):
    """Simplifies an existing JSONL file by filtering and transforming its records.

    This command processes an input JSONL file, applies a specified simplification level,
    and optionally archives or removes the original file.

    Args:
        input_file_path (Path): Path to the input JSONL file. If a directory is provided,
            the command will search for 'telemetry.jsonl' within the directory or its '.gemini' subdirectory.
        level (int): The simplification level to apply.
        archive_folder (Path): Folder to archive the original file to.
        disable_archiving (bool): If True, removes the original file instead of archiving.
    """
    try:
        exit_code = run_log_simplification(input_file_path, level, archive_folder, disable_archiving)
        if exit_code != 0:
            raise typer.Exit(exit_code)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        raise typer.BadParameter(str(e))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    TYPER_APP()
