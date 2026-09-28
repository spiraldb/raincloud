# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Passthrough handler — preserve a single table or planned batch stream."""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from ..batches import BatchStream, batch_input


@batch_input("parquet")
def identity(spec: dict, parsed: list[tuple[Path, pa.Table | BatchStream | None]],
             **kwargs) -> list[tuple[str, pa.Table | BatchStream]]:
    if len(parsed) != 1:
        raise ValueError(f"identity handler expects 1 parsed table, got {len(parsed)}")
    _, table = parsed[0]
    if table is None:
        raise ValueError("identity handler cannot run on deferred reader (xml/pbf/etc.)")
    slug = spec["slug"]
    return [(slug, table)]
