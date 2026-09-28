# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse UCI dataset 34 (Diabetes, AIM '94) into one flat canonical Arrow table.

Upstream shape (genuinely ancient):
    diabetes.zip
        Index
        README
        diabetes-data.tar.Z          <-- Unix compress / LZW format
            Diabetes-Data/
                Data-Codes           (event code → description)
                data-01 .. data-70   (one patient per file)

Each `data-NN` file is tab-separated:
    MM-DD-YYYY  HH:MM  <code>  <value>

where `code` is a small integer (e.g. 33 = regular insulin dose, 58 = pre-breakfast
blood glucose) and `value` is the reading.

The handler:
  1. reads the outer zip
  2. decompresses the inner `.tar.Z` with `unlzw3`
  3. walks the tar, parses each `data-NN`, prepends `patient_id`
  4. emits one pyarrow Table with columns
     (patient_id: uint8, date: string, time: string, code: uint8,
      value: float64, value_raw: string)

`value` is not always a number: insulin doses can be fractional (`4.5`), meters
flag out-of-range readings (`0Hi`, `0Lo`), and a few records carry `0''`, `3A` or
nothing. `value` holds the number when the text is one (float64 keeps every
fractional dose exactly) and is null otherwise; `value_raw` is the exact upstream
text of every record, zero padding (`009`) and flags included. `date` and `time`
stay strings because the upstream text is not always a valid date or time
(empty dates, `188:00`, `11:0`).
"""
from __future__ import annotations

import io
import re
import tarfile
import zipfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from ..spec import max_decompressed_bytes

_MAX_TARZ_BYTES = 32 << 20   # 32 MiB; the real member is ~187 KB
_DATA_FILE_RE = re.compile(r"data-(\d+)$")
_NUMBER_RE = re.compile(r"[0-9]+(\.[0-9]+)?")


def uci_diabetes_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]],
                       **kwargs) -> list[tuple[str, pa.Table]]:
    zip_paths = [p for p, _ in parsed if p.suffix == ".zip"]
    if len(zip_paths) != 1:
        raise ValueError(f"uci_diabetes_parse: expected one .zip input, got {len(zip_paths)}")
    zip_path = zip_paths[0]

    # Unzip → unlzw → untar → parse
    try:
        import unlzw3
    except ModuleNotFoundError as error:
        from raincloud._extras import missing
        raise missing(error, "decompressing the diabetes .tar.Z member") from error
    with zipfile.ZipFile(zip_path) as z:
        tarz_bytes = None
        for info in z.infolist():
            if not info.filename.endswith(".tar.Z"):
                continue
            # Bound the COMPRESSED input, because that is the only bound
            # available: `unlzw3.unlzw` takes bytes and returns bytes with no
            # streaming API, so peak memory is decided before it is called.
            # LZW reaches roughly 1000:1, and the real member is ~187 KB, so
            # this ceiling is ~180x headroom and still caps a hostile upstream.
            if info.file_size > _MAX_TARZ_BYTES:
                raise ValueError(
                    f"{info.filename} declares {info.file_size:,} bytes, over the "
                    f"{_MAX_TARZ_BYTES:,}-byte ceiling for this member"
                )
            with z.open(info) as f:
                # Read one byte past the ceiling: the zip's declared size is
                # metadata and a crafted archive can understate it.
                tarz_bytes = f.read(_MAX_TARZ_BYTES + 1)
            if len(tarz_bytes) > _MAX_TARZ_BYTES:
                raise ValueError(f"{info.filename} exceeds the {_MAX_TARZ_BYTES:,}-byte ceiling")
            break
        if tarz_bytes is None:
            raise FileNotFoundError("no diabetes-data.tar.Z inside the zip")

    tar_bytes = unlzw3.unlzw(tarz_bytes)
    # Post-hoc: the allocation has already happened, so this catches a merely
    # large expansion rather than preventing an adversarial one. The input bound
    # above is what does that.
    limit = max_decompressed_bytes()
    if limit is not None and len(tar_bytes) > limit:
        raise ValueError(
            f"decompressed tar is {len(tar_bytes):,} bytes, over the {limit:,}-byte "
            f"ceiling (RAINCLOUD_MAX_DECOMPRESSED_BYTES; 0 disables)"
        )

    patient_ids: list[int] = []
    dates: list[str] = []
    times: list[str] = []
    codes: list[int] = []
    values: list[float | None] = []
    raw_values: list[str] = []

    with tarfile.open(fileobj=io.BytesIO(tar_bytes)) as t:
        for member in t.getmembers():
            if not member.isfile(): continue
            m = _DATA_FILE_RE.search(member.name)
            if not m: continue
            pid = int(m.group(1))
            f = t.extractfile(member)
            if f is None: continue
            # Split on tabs without stripping first: a record with an empty
            # first or last field (`\t138\t33\t3A`, `10-12-1989\t7:00\t0\t`,
            # 66 of them in data-27 and data-29) is still four fields, and
            # stripping made it three and dropped it.
            for number, line in enumerate(f.read().decode("utf-8").split("\n"), 1):
                line = line.rstrip("\r")
                if not line.strip(): continue
                fields = line.split("\t")
                if len(fields) != 4:
                    raise ValueError(f"uci_diabetes_parse: {member.name} line {number} has "
                                     f"{len(fields)} tab-separated fields, not 4: {line[:80]!r}")
                d, tm, c, v = fields
                where = f"uci_diabetes_parse: {member.name} line {number}"
                # A code must survive as a uint8 with its text intact; anything
                # else would change what the record says, so it fails the build.
                if not re.fullmatch(r"[0-9]+", c) or str(int(c)) != c or int(c) > 255:
                    raise ValueError(f"{where}: code {c!r} is not a uint8 written plainly")
                number_value = None
                if _NUMBER_RE.fullmatch(v):
                    number_value = float(v)
                    if Decimal(repr(number_value)) != Decimal(v):
                        raise ValueError(f"{where}: value {v!r} does not round-trip through float64")
                patient_ids.append(pid)
                dates.append(d)
                times.append(tm)
                codes.append(int(c))
                values.append(number_value)
                raw_values.append(v)

    table = pa.table({
        "patient_id": pa.array(patient_ids, type=pa.uint8()),
        "date":       pa.array(dates, type=pa.string()),
        "time":       pa.array(times, type=pa.string()),
        "code":       pa.array(codes, type=pa.uint8()),
        "value":      pa.array(values, type=pa.float64()),
        "value_raw":  pa.array(raw_values, type=pa.string()),
    })
    print(f"  diabetes: {table.num_rows:,} rows across "
          f"{len(set(patient_ids))} patients")
    return [(spec["slug"], table)]
