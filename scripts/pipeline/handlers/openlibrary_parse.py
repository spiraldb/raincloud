# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse Internet Archive OpenLibrary dump files (`ol_dump_*.txt.gz`).

Each line of an OpenLibrary dump is a tab-separated record:

    <type>\t<key>\t<revision>\t<last_modified>\t<json>

where `<type>` is `/type/author`, `/type/edition`, `/type/work`, `/type/redirect`, etc.
The dump we ingest is already record-type-specific (e.g. `ol_dump_works_latest.txt`),
so the `type` column is constant within a file and we emit it as a plain string.

The JSON blob varies wildly in shape across records (different editions have
different fields), so we store it as a raw string and leave interpretation to
the consumer. This matches what the legacy intake did.

Output columns:
    key              : string   (e.g. '/works/OL1M')
    revision         : int64
    last_modified    : timestamp[us]
    type             : string   (constant per file — kept for joinability)
    record           : string   (raw JSON line)
"""
from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa


def openlibrary_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                      record_type: str = "work") -> list[tuple[str, pa.Table]]:
    if not parsed:
        raise ValueError("openlibrary_parse: no input files")
    path, _ = parsed[0]
    # The file may be .txt, .txt.gz, or .gz — open transparently
    opener = gzip.open if path.suffix == ".gz" or path.name.endswith(".txt.gz") else open

    # Stream into per-column Python lists. 40-55M rows per file means we
    # trade some memory for simplicity here; for a production run this should
    # be chunked into multiple pa.RecordBatches and written with ParquetWriter.
    keys: list[str] = []
    revs: list[int] = []
    times: list[str] = []
    types: list[str] = []
    records: list[str] = []

    with opener(path, "rt", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if not line: continue
            parts = line.split("\t", 4)
            if len(parts) < 5:
                continue  # malformed line — skip
            t, k, rev, ts, rec = parts
            types.append(t)
            keys.append(k)
            try:
                revs.append(int(rev))
            except ValueError:
                revs.append(0)
            times.append(ts)
            records.append(rec)
            if line_no % 1_000_000 == 0:
                print(f"    parsed {line_no:,} lines")

    print(f"    total: {len(keys):,} records")
    # Convert timestamps; OpenLibrary format is YYYY-MM-DDTHH:MM:SS.ffffff
    ts_array = pa.array(times, type=pa.string()).cast(pa.timestamp("us"),
                                                       safe=False)
    table = pa.table({
        "key": pa.array(keys, type=pa.string()),
        "revision": pa.array(revs, type=pa.int64()),
        "last_modified": ts_array,
        "type": pa.array(types, type=pa.string()),
        "record": pa.array(records, type=pa.string()),
    })
    return [(spec["slug"], table)]
