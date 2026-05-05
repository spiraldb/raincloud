# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse StatLib's `cadata.txt` — California Housing dataset from
Pace & Barry 1997, source for both `sklearn.datasets.fetch_california_housing`
and the Kaggle "California Housing Prices" mirror.

File shape:
    - Multi-paragraph text preamble describing the dataset
    - Blank line
    - Whitespace-separated numeric rows, 9 columns in this order:
        median_house_value, median_income, housing_median_age,
        total_rooms, total_bedrooms, population, households,
        latitude, longitude
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

_COLUMNS = [
    "median_house_value", "median_income", "housing_median_age",
    "total_rooms", "total_bedrooms", "population", "households",
    "latitude", "longitude",
]


def cal_housing_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]]
                     ) -> list[tuple[str, pa.Table]]:
    data_files = [p for p, _ in parsed if p.name in ("cadata.txt", "cal_housing.data")]
    if not data_files:
        raise ValueError("cal_housing_parse: cadata.txt / cal_housing.data not found")
    path = data_files[0]

    cols: list[list[float]] = [[] for _ in _COLUMNS]
    with open(path, "r", encoding="latin-1") as f:
        for line in f:
            # Data rows start with whitespace and contain 9 space-separated floats
            stripped = line.strip()
            if not stripped: continue
            parts = stripped.split()
            if len(parts) != 9: continue
            try:
                vals = [float(p) for p in parts]
            except ValueError:
                continue
            for i, v in enumerate(vals):
                cols[i].append(v)
    table = pa.table({name: pa.array(col, type=pa.float64()) for name, col in zip(_COLUMNS, cols)})
    print(f"  {table.num_rows:,} rows × {len(table.schema)} columns")
    return [(spec["slug"], table)]
