# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""UCI ML Repository default handler.

UCI datasets fetched from the canonical `https://archive.ics.uci.edu/static/public/<id>/data.csv`
endpoint are a CSV with a header row; a recipe that extracts several files (a
train/test pair) gets them concatenated. After reading we:

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

from ..parse import parse_csv
from .tighten_types import tighten_types


def uci_default(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *, uci_id: int | None = None
                ) -> list[tuple[str, pa.Table]]:
    if not parsed:
        raise ValueError("uci_default: no parsed tables")
    # Every parsed file is part of the dataset (a recipe that extracts
    # `allbp.data` and `allbp.test` wants both); `tighten_types` unifies and
    # concatenates them. Reading only the first dropped the rest.
    for path, table in parsed:
        if table is None:
            raise ValueError(f"uci_default requires an already-parsed table, not {path.name}")
    conflicts = _conflicting_columns([t for _, t in parsed])
    if conflicts:
        # A column inferred as different, unmergeable types in different files
        # (`?` for missing makes it text in one, an integer in another) is read
        # as text in every file, as it would be from the files concatenated.
        parsed = [(path, parse_csv(spec, path, column_types={n: pa.string() for n in conflicts}))
                  for path, _ in parsed]
    normalised = []
    for path, table in parsed:
        new_names = []
        for n in table.schema.names:
            nn = re.sub(r"\s+", "_", n.strip()).lower()
            nn = re.sub(r"[^a-z0-9_]+", "_", nn).strip("_") or "col"
            new_names.append(nn)
        normalised.append((path, table.rename_columns(new_names)))
    return tighten_types(spec, normalised)


def _conflicting_columns(tables: list[pa.Table]) -> list[str]:
    """Names whose types across `tables` do not unify, even permissively."""
    types: dict[str, list[pa.DataType]] = {}
    for table in tables:
        for field in table.schema:
            types.setdefault(field.name, []).append(field.type)
    conflicts = []
    for name, found in types.items():
        try:
            pa.unify_schemas([pa.schema([(name, t)]) for t in found], promote_options="permissive")
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            conflicts.append(name)
    return conflicts
