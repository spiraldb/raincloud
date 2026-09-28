# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Named transform handlers. Each handler has signature:
    (spec: dict, parsed: list[(Path, pa.Table | BatchStream | None)], **transform.params)
        -> list[(output_slug, pa.Table | BatchStream)]

`parsed` carries a BatchStream for readers the handler declares with
`batches.batch_input`, and None for inputs parse leaves to the handler. A
streaming handler that writes the canonical Arrow itself (through
`canonical.open_canonical_writer`) returns `[]`.

To add a handler, add a `"name": "module:attr"` entry to
`raincloud._registry.HANDLERS` — the one place the names are declared. Each module is imported only when its handler is actually asked
for. That laziness is what lets the loader answer "can I build this recipe?"
without dragging in pandas, openpyxl, osmium and duckdb, and is why there is no
longer a second copy of these names in a JSON file beside the code.
"""
from __future__ import annotations

from importlib import import_module

from raincloud._registry import HANDLERS

# name -> "module:attr", or a resolved callable once loaded (and for tests that
# install a handler directly). `get` accepts either.
_REGISTRY: dict[str, object] = dict(HANDLERS)


def get(name: str):
    """Resolve `name` to its handler callable, importing its module on demand.

    Returns None for an unknown name, matching the previous `dict.get`.
    """
    target = _REGISTRY.get(name)
    if target is None or callable(target):
        return target
    module_name, _, attr = str(target).partition(":")
    try:
        module = import_module(f".{module_name}", __name__)
    except ModuleNotFoundError as error:
        if error.name and error.name.split(".")[0] != "raincloud":
            from raincloud._extras import missing
            raise missing(error, f"the {name} handler", needs=_format_deps(module_name)) from error
        raise
    handler = getattr(module, attr)
    _REGISTRY[name] = handler  # resolved once
    return handler


def _format_deps(module_name: str) -> tuple[str, ...]:
    """The format-specific modules a handler module imports, read from its
    source the way docs/v{n}/handlers.md derives them
    (`docs._handler_extra_deps`), so the extra named installs every one of
    them rather than only the first one missing."""
    from importlib.util import find_spec
    from pathlib import Path

    from ..docs import _handler_extra_deps
    found = find_spec(f"{__name__}.{module_name}")
    if found is None or found.origin is None:
        return ()
    return tuple(_handler_extra_deps(Path(found.origin).read_text()))


def names() -> list[str]:
    """Registered handler names. Imports nothing."""
    return sorted(_REGISTRY)
