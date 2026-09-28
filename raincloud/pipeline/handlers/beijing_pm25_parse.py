# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse UCI Beijing Multi-Site Air Quality (dataset 501).

Upstream shape: outer zip from archive.ics.uci.edu contains an inner zip
(`PRSA2017_Data_20130301-20170228.zip`) plus a small `data.csv` / `test.csv`
sample pair and a JPG cover. The inner zip carries the full dataset:

    PRSA_Data_20130301-20170228/
        PRSA_Data_<station>_20130301-20170228.csv  (×12 stations)

Each per-station CSV is the same 18-column schema (No, year, month, day,
hour, PM2.5, PM10, SO2, NO2, CO, O3, TEMP, PRES, DEWP, RAIN, wd, WSPM,
station). The 12 stations are: Aotizhongxin, Changping, Dingling, Dongsi,
Guanyuan, Gucheng, Huairou, Nongzhanguan, Shunyi, Tiantan, Wanliu,
Wanshouxigong. ~35K rows per station × 12 = ~420K rows total.

This handler unpacks the inner zip, concats the 12 station CSVs (the
`station` column is already present, so concatenation is non-lossy), and
delegates to `tighten_types` for the integer-narrowing / UTF-8 string
re-annotation pass.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pac

from ..spec import workdir_root
from .tighten_types import tighten_types


def beijing_pm25_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]],
                       **kwargs
                       ) -> list[tuple[str, pa.Table]]:
    inner_zips = [p for p, _ in parsed
                  if p.suffix == ".zip" and p.name.startswith("PRSA2017_Data_")]
    if not inner_zips:
        raise ValueError(
            "beijing_pm25_parse: expected a nested PRSA2017_Data_*.zip in input"
        )
    inner_zip_path = inner_zips[0]

    work = workdir_root() / spec["slug"] / "inner"
    work.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(inner_zip_path) as z:
        z.extractall(work)

    csv_paths = sorted(work.glob("**/PRSA_Data_*.csv"))
    if not csv_paths:
        raise FileNotFoundError(
            f"beijing_pm25_parse: no PRSA_Data_*.csv found under {work}"
        )

    parse_opts = pac.ParseOptions(delimiter=",")
    convert_opts = pac.ConvertOptions(strings_can_be_null=True)
    tables = [pac.read_csv(p, parse_options=parse_opts,
                           convert_options=convert_opts)
              for p in csv_paths]
    merged = pa.concat_tables(tables, promote_options="default")
    print(f"  Beijing PM2.5: stations={len(csv_paths)}  rows={merged.num_rows:,} "
          f"× {merged.num_columns} cols")

    return tighten_types(spec, [(inner_zip_path, merged)])
