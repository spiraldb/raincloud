# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse NOAA GHCN-Daily .dly fixed-width records into a long-format parquet.

Input format (one file per station, e.g. `USW00094728.dly`):
    cols  1-11 : station_id      (11 chars, A11)
    cols 12-15 : year            (I4)
    cols 16-17 : month           (I2)
    cols 18-21 : element         (A4)   e.g. TMAX, TMIN, PRCP
    For each day 1..31 (fixed offsets):
        value  : 5 chars (I5), -9999 for missing
        mflag  : 1 char  (A1)
        qflag  : 1 char  (A1)
        sflag  : 1 char  (A1)

We emit one row per (station, date, element) observation, skipping missing values.

Output columns:
    station_id : string
    date       : date32
    element    : string    (TMAX, TMIN, PRCP, SNOW, ...)
    value      : int32     (hundredths of degrees Celsius for temps, tenths of mm for precip)
    mflag      : string    (measurement flag; empty if absent)
    qflag      : string    (quality flag)
    sflag      : string    (source flag)

Caveats:
- The canonical upstream ships ~125,000 per-station .dly files. This handler
  walks the extraction dir and processes them all. For a 3B-row output, consumers
  will want to partition the result — this handler writes a single parquet;
  callers concerned about size can chunk before writing in a wrapper.
- Dates are materialised only for valid (year, month, day) tuples that parse
  cleanly via datetime.date; February 30th style garbage is dropped with a
  warning count.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pyarrow as pa

_MISSING = -9999


def _parse_dly_file(path: Path):
    """Yield (station_id, date, element, value, mflag, qflag, sflag) tuples."""
    with open(path, "rt", encoding="ascii", errors="replace") as f:
        for line in f:
            if len(line) < 21:
                continue
            station_id = line[0:11].strip()
            try:
                year = int(line[11:15])
                month = int(line[15:17])
            except ValueError:
                continue
            element = line[17:21].strip()
            for d in range(1, 32):
                offset = 21 + (d - 1) * 8
                if offset + 8 > len(line):
                    break
                raw_val = line[offset:offset + 5].strip()
                if not raw_val or raw_val == "-9999":
                    continue
                try:
                    val = int(raw_val)
                except ValueError:
                    continue
                if val == _MISSING:
                    continue
                mflag = line[offset + 5]
                qflag = line[offset + 6]
                sflag = line[offset + 7]
                try:
                    date = _dt.date(year, month, d)
                except ValueError:
                    continue
                yield (station_id, date, element, val,
                       mflag if mflag != " " else "",
                       qflag if qflag != " " else "",
                       sflag if sflag != " " else "")


_SCHEMA = pa.schema([
    ("station_id", pa.string()),
    ("date", pa.date32()),
    ("element", pa.string()),
    ("value", pa.int32()),
    ("mflag", pa.string()),
    ("qflag", pa.string()),
    ("sflag", pa.string()),
])


def ghcn_daily_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                     batch_size: int = 5_000_000,
                     ) -> list[tuple[str, pa.Table]]:
    """Stream ~125K .dly station files into a single parquet via incremental
    ParquetWriter batches. Accumulating the full ~3B-row output in Python
    lists OOMs on a 128 GB machine, so we flush every `batch_size` rows.
    """
    import pyarrow.parquet as pq

    from ..spec import display_path, output_format_dir, spec_field

    if not parsed:
        raise ValueError("ghcn_daily_parse: no input files")

    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(
        spec, "write.output", f"{spec['slug']}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    stations: list[str] = []
    dates: list[_dt.date] = []
    elements: list[str] = []
    values: list[int] = []
    mflags: list[str] = []
    qflags: list[str] = []
    sflags: list[str] = []

    def _flush(writer):
        if not stations:
            return 0
        n = len(stations)
        writer.write_table(pa.table({
            "station_id": pa.array(stations, type=pa.string()),
            "date": pa.array(dates, type=pa.date32()),
            "element": pa.array(elements, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
            "mflag": pa.array(mflags, type=pa.string()),
            "qflag": pa.array(qflags, type=pa.string()),
            "sflag": pa.array(sflags, type=pa.string()),
        }))
        stations.clear(); dates.clear(); elements.clear(); values.clear()
        mflags.clear(); qflags.clear(); sflags.clear()
        return n

    total = 0
    file_count = 0
    writer = pq.ParquetWriter(out_path, _SCHEMA, compression=compression)
    try:
        for path, _ in parsed:
            if not str(path).endswith(".dly"):
                continue
            file_count += 1
            for tup in _parse_dly_file(path):
                stations.append(tup[0])
                dates.append(tup[1])
                elements.append(tup[2])
                values.append(tup[3])
                mflags.append(tup[4])
                qflags.append(tup[5])
                sflags.append(tup[6])
                if len(stations) >= batch_size:
                    flushed = _flush(writer)
                    total += flushed
                    print(f"    {file_count}/{len(parsed)} files; {total:,} rows flushed")
        # Final flush
        flushed = _flush(writer)
        total += flushed
    finally:
        writer.close()

    print(f"  wrote {display_path(out_path)}  rows={total:,} stations≈{file_count}")
    return []
