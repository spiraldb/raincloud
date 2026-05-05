# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse an .xlsx file into a single parquet, concatenating all sheets.

UCI's Online Retail II ships as a two-sheet Excel file (Year 2009-2010 /
Year 2010-2011). Both sheets share the same 8-column schema. We read every
sheet via pandas + openpyxl, add a `sheet_name` column so rows remain
traceable to their source, and union the result.

`params.sheet` (optional) restricts to a single named sheet when set.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa


def xlsx_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
               sheet: str | None = None, **kwargs
               ) -> list[tuple[str, pa.Table]]:
    xlsx_paths = [p for p, _ in parsed if p.suffix.lower() == ".xlsx"]
    if not xlsx_paths:
        raise ValueError("xlsx_parse: no .xlsx file in parsed input")
    path = xlsx_paths[0]

    # `dtype=object` keeps every cell as a Python object so pandas doesn't
    # silently infer a common int/float type across mixed-type columns —
    # e.g. the UCI Online Retail II "Invoice" column carries both integer
    # invoices like `489449` and credit-note strings like `'C489449'`, which
    # would otherwise break pa.Table.from_pandas's type inference.
    if sheet is not None:
        sheets = {sheet: pd.read_excel(path, sheet_name=sheet, dtype=object)}
    else:
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)

    tables = []
    for name, df in sheets.items():
        df.insert(0, "sheet_name", name)
        # Force every column to string so heterogeneous cells (mixed int/str,
        # stray NaN, Excel "errors") round-trip into Arrow without inference
        # failures. Downstream consumers can TRY_CAST numeric columns back.
        for col in df.columns:
            if col == "sheet_name": continue
            df[col] = df[col].astype(str).where(df[col].notna(), None)
        table = pa.Table.from_pandas(df, preserve_index=False)
        tables.append(table)
        print(f"  {path.name}[{name!r}]: {table.num_rows:,} rows × {table.num_columns} cols")

    merged = pa.concat_tables(tables, promote_options="default")
    print(f"  merged: {merged.num_rows:,} rows × {merged.num_columns} cols")
    return [(spec["slug"], merged)]
