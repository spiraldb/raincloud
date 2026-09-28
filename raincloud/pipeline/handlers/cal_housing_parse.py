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
    if len(data_files) != 1:
        raise ValueError(f"cal_housing_parse: expected one of cadata.txt / cal_housing.data, "
                         f"got {[p.name for p in data_files]}")
    path = data_files[0]

    cols: list[list[float]] = [[] for _ in _COLUMNS]
    # The preamble ends at the first line of nine numbers. Every non-blank line
    # after it is a data row: one that is not nine numbers fails the build
    # rather than being dropped, since a short table looks like a correct one.
    in_data = False
    with open(path, "r", encoding="latin-1") as f:
        for number, line in enumerate(f, 1):
            parts = line.split()
            if not parts:
                continue
            try:
                vals = [float(p) for p in parts] if len(parts) == 9 else None
            except ValueError:
                vals = None
            if vals is None:
                if in_data:
                    raise ValueError(f"cal_housing_parse: {path.name} line {number} is not nine "
                                     f"numbers: {line.strip()[:120]!r}")
                continue
            in_data = True
            for i, v in enumerate(vals):
                cols[i].append(v)
    if not in_data:
        raise ValueError(f"cal_housing_parse: {path.name} has no line of nine numbers")
    table = pa.table({name: pa.array(col, type=pa.float64()) for name, col in zip(_COLUMNS, cols)})
    print(f"  {table.num_rows:,} rows × {len(table.schema)} columns")
    return [(spec["slug"], table)]
