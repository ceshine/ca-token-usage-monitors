"""Utilities for monitoring coding-agent token usage."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("coding-agent-token-monitors")
except PackageNotFoundError:
    __version__ = "unknown"
