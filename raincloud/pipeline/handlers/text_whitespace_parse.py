# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse a text file with irregular whitespace separators into a flat table.

Some UCI datasets (e.g. seeds) use a mix of tabs and multi-tab runs as their
field separator, which pyarrow's CSV reader can't collapse. We read each
non-blank line, split on `\\s+` (every line must have the same number of
fields), and construct a pyarrow Table with auto-generated column names
(`col_0`, `col_1`, ...). Per-column type inference promotes columns where
every non-null token parses as int64 to int64, then to float64; otherwise
the column stays as string. Downstream consumers typically follow up with `uci_default`-style column renaming or
manual schema wiring.
"""
from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa

from ..spec import max_table_cells


def _infer_column(values: list[str | None]) -> pa.Array:
    """Promote a column of string tokens to int64 / float64 when every non-null
    token parses cleanly. Falls back to string for any ambiguity.
    """
    non_null = [v for v in values if v is not None]
    if non_null:
        # int64 first — strict: must parse via int() AND have no decimal point.
        try:
            if all("." not in v and "e" not in v.lower() for v in non_null):
                _ = [int(v) for v in non_null]
                return pa.array(
                    [int(v) if v is not None else None for v in values],
                    type=pa.int64(),
                )
        except (ValueError, TypeError):
            pass
        # float64 fallback for decimals / scientific notation.
        try:
            _ = [float(v) for v in non_null]
            return pa.array(
                [float(v) if v is not None else None for v in values],
                type=pa.float64(),
            )
        except (ValueError, TypeError):
            pass
    return pa.array(values, type=pa.string())


def text_whitespace_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]],
                          **kwargs
                          ) -> list[tuple[str, pa.Table]]:
    if len(parsed) != 1:
        raise ValueError(f"text_whitespace_parse expects exactly 1 input file, got {len(parsed)}")
    path, _ = parsed[0]

    # Two passes over the file rather than one pass plus two full in-memory
    # copies: pass 1 measures, so the size is known before anything is
    # retained. Every non-blank line must have as many fields as the first; a
    # ragged line would shift its values into the wrong columns, so it fails
    # the build, naming the line.
    width = 0
    n_rows = 0
    with open(path, "r", encoding="utf-8") as f:
        for number, line in enumerate(f, 1):
            line = line.strip()
            if not line: continue
            n_cols = len(re.split(r"\s+", line))
            if not width:
                width = n_cols
            elif n_cols != width:
                raise ValueError(f"text_whitespace_parse: {path.name} line {number} has {n_cols} "
                                 f"fields, the first line {width}: {line[:120]!r}")
            n_rows += 1

    ceiling = max_table_cells()
    if ceiling is not None and n_rows * width > ceiling:
        raise ValueError(
            f"text_whitespace_parse: {path.name} is {n_rows:,} rows x {width:,} columns "
            f"= {n_rows * width:,} cells, over the {ceiling:,}-cell ceiling. Raise "
            f"RAINCLOUD_MAX_TABLE_CELLS (0 disables) if the shape is real."
        )

    # Pass 2: fill the columns.
    max_cols = width
    cols: list[list[str | None]] = [[] for _ in range(max_cols)]
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            for i, field in enumerate(re.split(r"\s+", line)):
                cols[i].append(field)

    table = pa.table({f"col_{i}": _infer_column(cols[i]) for i in range(max_cols)})
    print(f"  {path.name}: {table.num_rows:,} rows × {table.num_columns} cols")
    return [(spec["slug"], table)]
