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

We emit one row per (station, date, element) observation. A `-9999` slot is
no observation and emits nothing; malformed input fails the build.

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
- Day slots past a month's end carry the `-9999` sentinel. A real value on a
  date that does not exist (February 30th) fails the build.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pyarrow as pa

_MISSING = -9999


_LINE = 21 + 31 * 8  # header, then 31 day slots of value + three flags


def _parse_dly_file(path: Path):
    """Yield (station_id, date, element, value, mflag, qflag, sflag) tuples.

    A slot holding the `-9999` sentinel is no observation -- the format pads
    every month to 31 days with it -- and yields nothing. Anything else that is
    not a well-formed observation fails the build, naming the file and line: a
    line that is not 269 ASCII characters, an unparseable year, month or value,
    or a value on a date that does not exist.
    """
    with open(path, "rt", encoding="ascii", newline="\n") as f:
        number = 0
        while True:
            try:
                line = f.readline()
            except UnicodeDecodeError as exc:
                raise ValueError(f"ghcn_daily_parse: {path.name} after line {number:,} is not "
                                 f"ASCII: {exc}") from None
            if not line:
                return
            number += 1
            line = line.rstrip("\n")
            where = f"ghcn_daily_parse: {path.name} line {number:,}"
            if len(line) != _LINE:
                raise ValueError(f"{where} is {len(line)} characters, not {_LINE}: {line[:60]!r}")
            station_id = line[0:11].strip()
            try:
                year = int(line[11:15])
                month = int(line[15:17])
            except ValueError:
                raise ValueError(f"{where} has year/month {line[11:17]!r}") from None
            element = line[17:21].strip()
            for d in range(1, 32):
                offset = 21 + (d - 1) * 8
                raw_val = line[offset:offset + 5]
                try:
                    val = int(raw_val)
                except ValueError:
                    raise ValueError(f"{where} day {d} has value {raw_val!r}") from None
                if val == _MISSING:
                    continue
                mflag = line[offset + 5]
                qflag = line[offset + 6]
                sflag = line[offset + 7]
                try:
                    date = _dt.date(year, month, d)
                except ValueError:
                    raise ValueError(f"{where} has value {val} on {year}-{month:02d}-{d:02d}, "
                                     "a date that does not exist") from None
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
    """Stream ~125K .dly station files into the canonical Arrow spine via
    incremental writer batches. Accumulating the full ~3B-row output in Python
    lists OOMs on a 128 GB machine, so we flush every `batch_size` rows.
    """
    from ..canonical import open_canonical_writer

    if not parsed:
        raise ValueError("ghcn_daily_parse: no input files")

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
    with open_canonical_writer(spec["slug"], _SCHEMA) as writer:
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

    print(f"  wrote {spec['slug']}  rows={total:,} stations≈{file_count}")
    return []
