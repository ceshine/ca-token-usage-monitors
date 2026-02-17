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

import logging
from pathlib import Path

import typer

from .calculate_token_usage import analyze_token_usage
from .simplify_logs import run_log_simplification

TYPER_APP = typer.Typer()


@TYPER_APP.command()
def stats(
    log_file_path: Path,
    enable_archiving: bool = False,
    log_simplify_level: int = 1,
    timezone: str = typer.Option(
        None,
        "--timezone",
        "-tz",
        help="Timezone to use for daily stats (e.g., 'UTC', 'America/New_York'). Defaults to local system time.",
    ),
):
    """A replica of the `main` function of the `calculate_token_usage` module"""
    try:
        exit_code = analyze_token_usage(log_file_path, enable_archiving, log_simplify_level, timezone)
        if exit_code != 0:
            raise typer.Exit(exit_code)
    except (FileNotFoundError, ValueError) as e:
        raise typer.BadParameter(str(e))


@TYPER_APP.command()
def simplify(
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
    """A replica of the `main` function of the `simplify_logs` module"""
    try:
        exit_code = run_log_simplification(input_file_path, level, archive_folder, disable_archiving)
        if exit_code != 0:
            raise typer.Exit(exit_code)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        raise typer.BadParameter(str(e))


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    TYPER_APP()


if __name__ == "__main__":
    main()
