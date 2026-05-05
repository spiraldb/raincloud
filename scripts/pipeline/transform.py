# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 4 — transform parsed tables into the final shape.

The `transform.handler` field of each DatasetSpec names a Python callable
registered in `scripts/pipeline/handlers/`. Each handler takes
    (spec: dict, parsed: list[(Path, pa.Table|None)]) -> list[(output_slug, pa.Table)]

Multi-output handlers return >1 tuple (e.g. `glove_split`).

See `scripts/pipeline/handlers/__init__.py` for the handler registry.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from . import handlers
from .spec import spec_field


def transform(spec: dict, parsed: list[tuple[Path, pa.Table | None]]) -> list[tuple[str, pa.Table]]:
    name = spec_field(spec, "transform.handler", "identity")
    params = spec_field(spec, "transform.params", {}) or {}
    fn = handlers.get(name)
    if fn is None:
        raise ValueError(f"unknown transform.handler: {name}")
    print(f"[transform] {spec['slug']} ({name})")
    return fn(spec, parsed, **params)
