# Development Guidelines

## Philosophy

### Core Beliefs

- **Incremental progress over big bangs** - Small changes that compile and pass tests
- **Learning from existing code** - Study and plan before implementing
- **Pragmatic over dogmatic** - Adapt to project reality
- **Clear intent over clever code** - Be boring and obvious

### Simplicity Means

- Single responsibility per function/class
- Avoid premature abstractions
- No clever tricks - choose the boring solution
- If you need to explain it, it's too complex

## Decision Framework

When multiple valid approaches exist, choose based on:

1. **Testability** - Can I easily test this?
2. **Readability** - Will someone understand this in 6 months?
3. **Consistency** - Does this match project patterns?
4. **Simplicity** - Is this the simplest solution that works?
5. **Reversibility** - How hard to change later?

## Technical Standards

### Architecture Principles

- **Composition over inheritance** - Use dependency injection
- **Interfaces over singletons** - Enable testing and flexibility
- **Explicit over implicit** - Clear data flow and dependencies
- **Test-driven when possible** - Never disable tests, fix them

### Error Handling

- Fail fast with descriptive messages
- Include context for debugging
- Handle errors at appropriate level
- Never silently swallow exceptions

### Type Hinting & Validation

- **Comprehensive Typing:** Use type hints for all function signatures and class members. Use modern Python 3.10+ typing syntax (e.g., `list[str]` instead of `List[str]`, `str | None` instead of `Optional[str]`).
- **Data Validation:** For complex data structures or API models, prefer Pydantic for validation and serialization.
- **Strictness:** Aim for code that passes `pyright` with minimal ignores.

### Logging Standards

- **Standard Logger:** Initialize loggers using `LOGGER = logging.getLogger(__name__)` at the module level.
- **Contextual Info:** Use the logger to provide context during long-running operations or errors, but avoid excessive debug logging in production paths.

### CLI & User Interface

- **Typer:** Use `typer` for creating CLI applications. Organize commands logically using sub-commands if necessary.
- **Rich:** Use `rich` for formatting terminal output (tables, progress bars, colors) to provide a better user experience.
- **Progress Bars:** Use `tqdm` or `rich.progress` for long-running loops.

### Testing Strategy

- **Pytest:** Use `pytest` for all testing. Organize tests in a `tests/` directory mirroring the `search_engine/` structure.
- **Fixtures:** Use pytest fixtures for setup/teardown of data, models, or temporary directories.
- **Mocking:** Mock external services, databases (like Qdrant), or heavy models (Torch) when writing unit tests to keep them fast and reliable.

### Code Style Guidelines

When writing or modifying Python code, you **MUST** adhere to the PEP-8 style guide. Pay particular attention to:

- **Pathlib:** Always use `pathlib.Path` for file and directory operations instead of `os.path` or string manipulations.
- **Import Grouping:** Imports should be grouped in the following order, with a blank line separating each group:
    1.  Standard library imports (e.g., `os`, `sys`, `json`)
    2.  Third-party imports (e.g., `fastapi`, `pydantic`, `uvicorn`)
    3.  Local application/monorepo-specific imports
- **Import Sorting:** Within each group, the imports should be ordered using the following rules:
    - `import ...` statements come first, then `from ... import ...` statements.
    - Sort module paths level-by-level: compare the first segment, then the second only if the first segments are equal, and so on.
    - **Segment comparison:** a shorter segment ranks before a longer one; if two segments have the same length but different text, use alphabetical order as the tiebreaker.
    - **Parent before child:** when one path is the parent of another (e.g. `a.b` vs `a.b.c`), the parent always comes first.
    - Example: `import abc.abc.def` comes before `import abc.abcd.d` because `abc` == `abc` at level 1, then `abc` (length 3) < `abcd` (length 4) at level 2.
    - Exception: Always put `from typing import ...` at the bottom of the standard library group.
- Naming conventions, and whitespace.
- **Docstrings:** Follow Google's Python style guide for docstrings strictly. Even when the function signature contains type annotations, include the parameter and return type information in the docstring.

Example imports:
```python
import os
import abc
import csv
import datetime
from xml.sax.handler import ContentHandler
from xml.parsers.expat import ExpatError
from a.b import c
from a.b.c import d
from a.b.add import f
from a.bc.a import k
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

import typer

from . import runner
```

## Python Environment (UV)

This project uses `uv` for Python setup and dependency management.

- **Sync Environment:** `uv sync --frozen` (creates/updates virtualenv from lockfile).
- **Run Scripts/Tools:** Always prefix with `uv run` (e.g., `uv run python main.py`, `uv run pytest`).
- **Add Dependencies:** Use `uv add <package>` or `uv add --dev <package>`.
- **Linting & Formatting:**
  - `uvx ruff check .` (Linting)
  - `uvx ruff format --line-length 120` (Formatting)

Pre-Submission Check:

- Before submitting for review, always run `uvx ruff check .` and `uvx ruff format --line-length 120`.
- Ensure all tests pass with `uv run pytest`.

Guidelines for commands and CI:

- Always prefix runtime commands with `uv run`.
- Do not use `pip`, `poetry`, or `venv` directly.
