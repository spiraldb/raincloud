# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""UCI ML Repository default handler.

UCI datasets fetched from the canonical `https://archive.ics.uci.edu/static/public/<id>/data.csv`
endpoint are always a single CSV with a header row. After reading we:

    1. Normalise column names (strip whitespace, collapse internal spaces to `_`, lowercase)
    2. Apply the tighten_types integer-narrowing pass

The uci_id is passed in params but we currently don't use it — it's kept
for future enhancements (e.g. looking up the `variables` metadata from the
UCI API to apply declared types rather than inferred ones).
"""
from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa

from .tighten_types import tighten_types


def uci_default(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *, uci_id: int | None = None
                ) -> list[tuple[str, pa.Table]]:
    if not parsed:
        raise ValueError("uci_default: no parsed tables")
    _, table = parsed[0]
    if table is None:
        raise ValueError("uci_default requires an already-parsed table")
    # Normalise column names
    new_names = []
    for n in table.schema.names:
        nn = re.sub(r"\s+", "_", n.strip()).lower()
        nn = re.sub(r"[^a-z0-9_]+", "_", nn).strip("_") or "col"
        new_names.append(nn)
    table = table.rename_columns(new_names)
    # Delegate to tighten_types
    return tighten_types(spec, [(parsed[0][0], table)])
