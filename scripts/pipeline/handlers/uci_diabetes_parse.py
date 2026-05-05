# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse UCI dataset 34 (Diabetes, AIM '94) into a single flat parquet.

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
     (patient_id: uint8, date: string, time: string, code: uint8, value: uint16)
"""
from __future__ import annotations

import io
import re
import tarfile
import zipfile
from pathlib import Path

import pyarrow as pa

_DATA_FILE_RE = re.compile(r"data-(\d+)$")


def uci_diabetes_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]],
                       **kwargs) -> list[tuple[str, pa.Table]]:
    zip_paths = [p for p, _ in parsed if p.suffix == ".zip"]
    if not zip_paths:
        raise ValueError("uci_diabetes_parse: no .zip input")
    zip_path = zip_paths[0]

    # Unzip → unlzw → untar → parse
    import unlzw3
    with zipfile.ZipFile(zip_path) as z:
        tarz_bytes = None
        for name in z.namelist():
            if name.endswith(".tar.Z"):
                with z.open(name) as f:
                    tarz_bytes = f.read()
                break
        if tarz_bytes is None:
            raise FileNotFoundError("no diabetes-data.tar.Z inside the zip")

    tar_bytes = unlzw3.unlzw(tarz_bytes)

    patient_ids: list[int] = []
    dates: list[str] = []
    times: list[str] = []
    codes: list[int | None] = []
    values: list[int | None] = []

    with tarfile.open(fileobj=io.BytesIO(tar_bytes)) as t:
        for member in t.getmembers():
            if not member.isfile(): continue
            m = _DATA_FILE_RE.search(member.name)
            if not m: continue
            pid = int(m.group(1))
            f = t.extractfile(member)
            if f is None: continue
            for line in f.read().decode("utf-8", errors="replace").splitlines():
                fields = line.strip().split("\t")
                if len(fields) != 4: continue
                d, tm, c, v = fields
                # values are sometimes zero-padded ("009"); ints normalise that.
                try: c_i = int(c)
                except ValueError: c_i = None
                try: v_i = int(v)
                except ValueError: v_i = None
                patient_ids.append(pid)
                dates.append(d)
                times.append(tm)
                codes.append(c_i)
                values.append(v_i)

    table = pa.table({
        "patient_id": pa.array(patient_ids, type=pa.uint8()),
        "date":       pa.array(dates, type=pa.string()),
        "time":       pa.array(times, type=pa.string()),
        "code":       pa.array(codes, type=pa.uint8()),
        "value":      pa.array(values, type=pa.uint16()),
    })
    print(f"  diabetes: {table.num_rows:,} rows across "
          f"{len(set(patient_ids))} patients")
    return [(spec["slug"], table)]
