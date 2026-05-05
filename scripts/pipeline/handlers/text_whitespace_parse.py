# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse a text file with irregular whitespace separators into a flat table.

Some UCI datasets (e.g. seeds) use a mix of tabs and multi-tab runs as their
field separator, which pyarrow's CSV reader can't collapse. We read each
non-blank line, split on `\\s+`, and construct a pyarrow Table with
auto-generated column names (`col_0`, `col_1`, ...). Downstream consumers
typically follow up with `uci_default`-style column renaming or manual
schema wiring.
"""
from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa


def text_whitespace_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]],
                          **kwargs
                          ) -> list[tuple[str, pa.Table]]:
    if not parsed:
        raise ValueError("text_whitespace_parse: no input files")
    path, _ = parsed[0]

    rows: list[list[str]] = []
    max_cols = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            fields = re.split(r"\s+", line)
            rows.append(fields)
            if len(fields) > max_cols: max_cols = len(fields)

    # Pad short rows with None so the schema is regular.
    cols: list[list[str | None]] = [[] for _ in range(max_cols)]
    for r in rows:
        for i in range(max_cols):
            cols[i].append(r[i] if i < len(r) else None)

    table = pa.table({f"col_{i}": pa.array(cols[i], type=pa.string())
                      for i in range(max_cols)})
    print(f"  {path.name}: {table.num_rows:,} rows × {table.num_columns} cols")
    return [(spec["slug"], table)]
