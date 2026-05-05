# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Passthrough handler — single parsed table becomes the output."""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa


def identity(spec: dict, parsed: list[tuple[Path, pa.Table | None]], **kwargs) -> list[tuple[str, pa.Table]]:
    if len(parsed) != 1:
        raise ValueError(f"identity handler expects 1 parsed table, got {len(parsed)}")
    _, table = parsed[0]
    if table is None:
        raise ValueError("identity handler cannot run on deferred reader (xml/pbf/etc.)")
    slug = spec["slug"]
    return [(slug, table)]
