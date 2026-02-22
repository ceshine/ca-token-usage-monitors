"""Module entrypoint for `python -m opencode_token_usage`."""

from .cli import TYPER_APP


if __name__ == "__main__":
    TYPER_APP()
