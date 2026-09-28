# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 4 — transform parsed tables into the final shape.

The `transform.handler` field of each DatasetSpec names a Python callable
declared in `raincloud._registry.HANDLERS` (implemented under
`raincloud/pipeline/handlers/`). Each handler is called as
    handler(spec, parsed: list[(Path, Table | BatchStream | None)], **transform.params)
        -> list[(output_slug, Table | BatchStream)]

Batch-capable handlers return a fixed-schema BatchStream instead of a Table.
Multi-output handlers return >1 tuple (e.g. `glove_split`). A streaming handler
that writes the canonical Arrow itself returns `[]`, and the build skips
`write_canonical` for it.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from . import handlers
from .batches import BatchStream
from .spec import spec_field


def transform(spec: dict, parsed: list[tuple[Path, pa.Table | BatchStream | None]]
              ) -> list[tuple[str, pa.Table | BatchStream]]:
    name = spec_field(spec, "transform.handler", "identity")
    params = spec_field(spec, "transform.params", {}) or {}
    fn = handlers.get(name)
    if fn is None:
        raise ValueError(f"unknown transform.handler: {name}")
    print(f"[transform] {spec['slug']} ({name})")
    return fn(spec, parsed, **params)
