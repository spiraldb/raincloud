# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse NYC Finance rolling-sales XLSX files (one per borough) into a
single Arrow table.

File shape (each of the 5 boroughs):
    rows 0-3: preamble text (title, methodology notes)
    row   4 : column header (21 columns)
    rows 5+ : data

Columns are consistent across boroughs. We normalise the SHOUTY-CAPS headers
to snake_case and concat all 5 into a single table. Each row already carries
a `borough` integer column (1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens,
5=Staten Island) so no extra ID column is needed.
"""
from __future__ import annotations

from pathlib import Path

import openpyxl
import pyarrow as pa


def _snake(name: str) -> str:
    s = name.strip().lower().replace("  ", " ")
    out = []
    for c in s:
        if c.isalnum(): out.append(c)
        elif c in (" ", "-", "/"): out.append("_")
    return "_".join(p for p in "".join(out).split("_") if p)


def nyc_rolling_sales(spec: dict, parsed: list[tuple[Path, pa.Table | None]]
                       ) -> list[tuple[str, pa.Table]]:
    xlsx_files = sorted(p for p, _ in parsed if str(p).lower().endswith(".xlsx"))
    if not xlsx_files:
        raise ValueError("nyc_rolling_sales: no .xlsx files in extracted output")
    print(f"  parsing {len(xlsx_files)} xlsx files")

    all_columns: dict[str, list] = {}
    header: list[str] | None = None

    for path in xlsx_files:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        rows = ws.iter_rows(values_only=True)
        # Skip preamble
        for _ in range(4): next(rows, None)
        this_header = [_snake(c) for c in next(rows)]
        if header is None:
            header = this_header
            for col in header: all_columns[col] = []
        elif this_header != header:
            raise ValueError(f"header mismatch in {path.name}: "
                             f"got {this_header}, expected {header}")
        n = 0
        for row in rows:
            # Some trailing empty rows
            if all(v is None or v == "" for v in row): continue
            for col, v in zip(header, row):
                all_columns[col].append(v)
            n += 1
        print(f"    {path.name}: {n:,} rows")
        wb.close()

    # Build Arrow arrays — let pyarrow infer types from the Python values
    arrays = [pa.array(all_columns[col]) for col in header]
    table = pa.table({col: arr for col, arr in zip(header, arrays)})
    print(f"  total: {table.num_rows:,} rows × {len(table.schema)} columns")
    return [(spec["slug"], table)]
