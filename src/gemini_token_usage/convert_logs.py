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

"A script to convert a loose OpenTelemetry log file into an optimized JSONL file."

import os
import shutil
import logging
from datetime import datetime
from pathlib import Path

import orjson
import typer

from .simplify_logs import simplify_record


LOGGER = logging.getLogger(__name__)


def get_last_timestamp(file_path: Path) -> str | None:
    """Read the last timestamp from an existing JSONL file.

    This function efficiently reads the JSONL file from the end to find the timestamp
    of the last complete JSON object. It is optimized for large files by reading in
    chunks and reconstructing lines as it goes backward.

    Args:
        file_path (Path): The path to the JSONL file to read.

    Returns:
        The timestamp of the last event in the file, or None if the file is empty
        or does not exist.

    Raises:
        ValueError: If reading the file fails unexpectedly.
    """
    if not file_path.exists():
        return None

    try:
        with open(file_path, "rb") as f:
            _ = f.seek(0, os.SEEK_END)  # Move to the end of the file
            pos = f.tell()  # Get the current position (end of file)
            remainder = b""  # Buffer to store incomplete lines from previous chunks

            chunk_size = 64 * 1024  # Size of each chunk to read (64 KB)

            while pos > 0:
                read_len = min(chunk_size, pos)  # Determine how much to read in this chunk
                pos -= read_len  # Move the position backward
                _ = f.seek(pos)  # Seek to the new position
                chunk = f.read(read_len)  # Read the chunk

                # Check if a newline character is present in the chunk
                if b"\n" in chunk:
                    parts = chunk.split(b"\n")

                    # The last part of the chunk combined with the remainder
                    # from the previous chunk forms a potentially complete line.
                    last_line = parts[-1] + remainder
                    if last_line.strip():  # If the line is not empty
                        try:
                            # Attempt to parse the JSON and return the timestamp
                            return orjson.loads(last_line)["attributes"]["event.timestamp"]
                        except orjson.JSONDecodeError:
                            LOGGER.warning("Invalid JSON string found: %s", last_line)
                            # If the last line is incomplete/corrupt, ignore it and continue
                            pass

                    # If the last line was empty or invalid, check other full lines
                    # within this chunk, going backward.
                    for line in reversed(parts[1:-1]):
                        if line.strip():
                            try:
                                return orjson.loads(line)["attributes"]["event.timestamp"]
                            except orjson.JSONDecodeError:
                                LOGGER.warning("Invalid JSON string found: %s", line)
                                continue

                    # The first part of the chunk might be the end of a line
                    # that started in an even earlier chunk. Store it as remainder.
                    remainder = parts[0]
                else:
                    # If no newline in the chunk, it means the current chunk is part
                    # of a larger line. Prepend it to the remainder.
                    remainder = chunk + remainder

            # If the loop finishes, it means we reached the start of the file.
            # The 'remainder' will contain the first line of the file, if any.
            if remainder.strip():
                return orjson.loads(remainder)["attributes"]["event.timestamp"]

    except Exception as e:
        raise ValueError(f"Failed to read last timestamp from {file_path}: {e}")

    return None


def convert_log_file(
    input_file_path: Path,
    output_file_path: Path,
    last_timestamp: str | None = None,
    simplify_level: int = 0,
) -> tuple[int, int]:
    """Converts a raw log file to JSONL format.

    Reads from the input file, parses JSON objects (handling potentially multi-line
    formatting), and writes them to the output file. Supports incremental updates
    by skipping entries before a given timestamp.

    Args:
        input_file_path (Path): Path to the source log file.
        output_file_path (Path): Path to the destination JSONL file.
        last_timestamp (str | None): If provided, only entries with a timestamp strictly greater
            than this value will be processed.
        simplify_level (int): The level of simplification to apply to log records.
            Refer to the `simplify_logs` module for more details.

    Returns:
        A tuple containing:
            - The number of records successfully converted and written.
            - The number of records skipped (due to timestamp or simplification).
    """
    count = 0
    skipped_count = 0
    mode = "ab" if last_timestamp else "wb"

    # Open input in text mode (utf-8) and output in binary mode (for orjson)
    with open(input_file_path, "r", encoding="utf-8") as f_in, open(output_file_path, mode) as f_out:
        buffer = ""

        for line in f_in:
            buffer += line

            # heuristic: only attempt parse if line looks like end of object
            if line.strip() == "}":
                try:
                    # Attempt to parse the accumulated buffer
                    obj = orjson.loads(buffer)
                    # Clear buffer
                    buffer = ""
                    attributes = obj.get("attributes", {})

                    # Pre-write validation: Ensure event.timestamp exists
                    current_ts = attributes.get("event.timestamp")
                    if not current_ts:
                        continue

                    # Incremental check
                    if last_timestamp and current_ts <= last_timestamp:
                        skipped_count += 1
                        continue

                    if (obj := simplify_record(obj, simplify_level)) is None:
                        continue

                    # If successful, write to output and reset buffer
                    _ = f_out.write(orjson.dumps(obj) + b"\n")
                    count += 1
                except orjson.JSONDecodeError:
                    # Buffer might contain a nested object ending in '}', or incomplete data
                    # Continue accumulating
                    pass

        if buffer.strip():
            LOGGER.warning("End of file reached with incomplete JSON data in buffer.")

    return count, skipped_count


def run_log_conversion(
    input_file_path: Path,
    output_file_path: Path | None = None,
    simplify_level: int = 0,
    archiving_enabled: bool = False,
    archive_folder_path: Path = Path("/tmp"),
) -> Path:
    """Converts a raw log file to JSONL format and optionally archives the original.

    This function processes the input log file, converting it into a JSON Lines (JSONL) format.
    It supports incremental updates by checking the last timestamp in the existing output file
    and appending new records.

    Args:
        input_file_path (Path): Path to the source log file.
        output_file_path (Path | None): Path to the destination JSONL file. Defaults to
            `input_file_path` with a `.jsonl` extension if not provided.
        simplify_level (int): The level of simplification to apply to log records.
            Refer to the `simplify_logs` module for more details.
        archiving_enabled (bool): If True, moves the input file to an archive folder after processing.
        archive_folder_path (Path): Destination folder for archived log files. Defaults to `/tmp`.

    Returns:
        Path: The path to the generated or updated JSONL file.

    Raises:
        FileNotFoundError: If the `input_file_path` does not exist.
        ValueError: If arguments are invalid (e.g., unexpected data format).
        Exception: For other errors during conversion or archiving.
    """
    if not input_file_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file_path}")

    if output_file_path is None:
        output_file_path = input_file_path.with_suffix(".jsonl")

    # Determine the last timestamp to support incremental updates
    last_timestamp = get_last_timestamp(output_file_path)

    if last_timestamp:
        LOGGER.info("Found existing output. Appending new entries after %s...", last_timestamp)
    else:
        LOGGER.info("Starting fresh conversion...")

    count, skipped_count = convert_log_file(input_file_path, output_file_path, last_timestamp, simplify_level)

    LOGGER.info("Successfully converted %d records to %s (Skipped %d)", count, output_file_path, skipped_count)

    if archiving_enabled:
        archive_folder_path.mkdir(exist_ok=True, parents=True)
        new_file_name = f"{input_file_path.stem}.{int(datetime.now().timestamp())}{input_file_path.suffix}"
        new_file_path = archive_folder_path / new_file_name
        _ = shutil.move(input_file_path, new_file_path)
        LOGGER.info("Archived %s to %s", input_file_path, new_file_path)

    return output_file_path


def main(
    input_file_path: Path = typer.Argument(
        Path(".gemini/telemetry.log"),
        help="Path to the input log file containing concatenated JSON objects.",
        exists=True,
        dir_okay=False,
        readable=True,
    ),
    output_file_path: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to the output JSONL file. Defaults to the same location of the input file path, but with the suffix changed to .jsonl",
        writable=True,
    ),
    simplify_level: int = typer.Option(
        0,
        "--simplify-level",
        help="Simplification level. 0: None, 1: Events Only. Defaults to 0.",
    ),
    archiving_enabled: bool = typer.Option(False, "--enable-archiving", help="Enable archiving of the input log file."),
    archive_folder_path: Path = typer.Option(
        "/tmp",
        "--archive-folder",
        help="Folder to move the input log file to after processing.",
    ),
):
    """Convert a log file with concatenated JSON objects to JSONL format.

    This function processes the input log file, converting it into a JSON Lines (JSONL) format.
    It supports incremental updates by checking the last timestamp in the existing output file
    and appending new records.

    Key features:

    - **Incremental Updates**: Skips records with timestamps earlier than or equal to the
      last recorded timestamp in the output file.
    - **Simplification**: Optionally filters records based on the specified simplification level.
    - **Robust Parsing**: Accumulatively reads lines and parses JSON objects, handling potential
      formatting issues in raw logs.
    - **Archiving**: Optionally moves the processed input file to an archive directory with
      a timestamped filename.

    Args:
        input_file_path (Path): Path to the input log file.
        output_file_path (Path | None): Path to the output JSONL file. Defaults to input path with .jsonl extension.
        simplify_level (int): Integer level for simplification (0=None, 1: Events Only).
        archiving_enabled (bool): If True, moves the input file to an archive folder after processing.
        archive_folder_path (Path): Destination folder for archived log files.
    """
    try:
        _ = run_log_conversion(
            input_file_path=input_file_path,
            output_file_path=output_file_path,
            simplify_level=simplify_level,
            archiving_enabled=archiving_enabled,
            archive_folder_path=archive_folder_path,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    typer.run(main)
