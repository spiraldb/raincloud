# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Read SAS V5 Transport format (.XPT) files via pyreadstat → Arrow.

Primary use case: CDC BRFSS annual data dumps. Each year's release is a zip
containing a single .XPT file with roughly 330 columns and 400–450k rows.

The BRFSS column names are short SAS identifiers (`_LLCPWT`, `SEX1`, `GENHLTH`,
`ADDEPEV3`, ...). pyreadstat exposes the SAS labels as metadata; we keep the
short names as the Arrow column names (consumers can join against the BRFSS
codebook) and stash the label dictionary in the schema's custom metadata so it
doesn't vanish.

Numeric columns in BRFSS XPT are all doubles; many encode categorical values
(1/2/7/9 sentinels). The `tighten_types` pass will narrow them on a subsequent
handler if desired, but for BRFSS specifically we keep them as-is because the
sentinel codes matter for downstream analysis.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyreadstat


def sas_xpt_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                  year: int | None = None) -> list[tuple[str, pa.Table]]:
    if not parsed:
        raise ValueError("sas_xpt_parse: no input files")
    xpt_files = [p for p, _ in parsed if str(p).upper().endswith(".XPT")]
    if not xpt_files:
        raise ValueError("sas_xpt_parse: no .XPT files in extracted output")
    if len(xpt_files) > 1:
        raise ValueError(f"sas_xpt_parse: multiple .XPT files, unsure which to use: "
                         f"{[p.name for p in xpt_files]}")
    path = xpt_files[0]
    print(f"  reading {path.name} via pyreadstat")

    # BRFSS files sometimes contain non-UTF-8 bytes in free-text columns
    # (e.g. 2022 has byte 0xB4). Try UTF-8 first, fall back to Latin-1.
    try:
        df, meta = pyreadstat.read_xport(str(path))
    except UnicodeDecodeError:
        print("  (non-UTF-8 bytes detected; re-reading with encoding='LATIN1')")
        df, meta = pyreadstat.read_xport(str(path), encoding="LATIN1")
    print(f"  {len(df):,} rows × {len(df.columns)} columns")

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Attach column labels as Arrow schema metadata (key prefix 'sas_label.')
    schema_meta = {}
    for col in df.columns:
        label = meta.column_names_to_labels.get(col)
        if label:
            schema_meta[f"sas_label.{col}".encode()] = label.encode()
    if year is not None:
        schema_meta[b"brfss_year"] = str(year).encode()
    if schema_meta:
        table = table.replace_schema_metadata(schema_meta)

    return [(spec["slug"], table)]
